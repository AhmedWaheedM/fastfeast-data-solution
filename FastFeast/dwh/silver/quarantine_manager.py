import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path
from support.logger import pipeline as log
from pipeline.config.config import config_settings

#FIXME constant should be moved to config 
MAX_ORPHAN_RETRIES = 3 

def route_records(table: pa.Table, table_name: str, run_id: str) -> pa.Table:
    """ 
    Splits a validated PyArrow table three ways:
    1. VALID -> Strips metadata and returns for Silver ingestion.
    2. INVALID -> Routes to the Dead Letter Queue (Quarantine).
    3. ORPHAN -> Routes to the Orphan Waiting Room.
    """
    #NOTE also should be derived from metadata/config
    required_cols = ['_record_status', '_error_reasons', '_retry_count']

    if not all(col in table.column_names for col in required_cols): 
        raise ValueError(f"Table {table_name} is missing required validation columns.")
    
    num_rows = table.num_rows
    table = table.append_column('pipeline_run_id', pa.array([run_id] * num_rows))
    table = table.append_column("_processed_at", pa.array([datetime.now()] * num_rows, type=pa.timestamp('us')))

    good_table = table.filter(pc.equal(table['_record_status'], 'VALID'))
    bad_table = table.filter(pc.equal(table['_record_status'], 'INVALID'))
    orphan_table = table.filter(pc.equal(table['_record_status'], 'ORPHAN'))
    expired_orphans = pa.table({})
    if orphan_table.num_rows > 0:
        expired_mask = pc.greater_equal(orphan_table['_retry_count'], MAX_ORPHAN_RETRIES)
        expired_orphans = orphan_table.filter(expired_mask)
        active_orphans = orphan_table.filter(pc.invert(expired_mask))

        if expired_orphans.num_rows > 0:
            log.warning(f"Found {expired_orphans.num_rows} expired orphan records. Routing to DLQ.", table_name=table_name)
            expired_reasons = pc.binary_join_element_wise(
                expired_orphans['_error_reasons'],
                pa.scalar(" | expired_orphan"),
                pa.scalar("")
            )
            expired_orphans = expired_orphans.set_column(
                expired_orphans.schema.get_field_index('_error_reasons'), 
                '_error_reasons',
                expired_reasons
            )
            bad_table = pa.concat_tables([bad_table, expired_orphans])
        
        if active_orphans.num_rows > 0: 
            _write_to_storage(active_orphans, table_name, run_id, folder_type = "orphans")
    if bad_table.num_rows > 0:
        _write_to_storage(bad_table, table_name, run_id, folder_type = "quarantine")
    log.info(
        'Validation routing complete',
        table_name=table_name,
        valid_records=good_table.num_rows,
        invalid_records=bad_table.num_rows,
        orphans_waiting=(orphan_table.num_rows - expired_orphans.num_rows) if orphan_table.num_rows > 0 else 0,
        run_id = run_id
    )
    if good_table.num_rows == 0:
        return None

    clean_table = good_table.drop([col for col in good_table.column_names if col.startswith('_') or col == 'pipeline_run_id'])
    return clean_table

def _write_to_storage(table: pa.Table, table_name: str, run_id: str, folder_type: str):
    """
    Writes a PyArrow table to the appropriate location in storage based on the folder type (quarantine or orphans).
    """
    base_dir = Path(config_settings.paths.output_dir).resolve() / "validation" / folder_type
    today_str = datetime.now().strftime(config_settings.datetime_handling.date_key_format)

    # Path should be something along the lines of:  /validation/{quarantine_or_orphans}/{table_name}/{YYYY-MM-DD}/{run_id}.parquet 
    dest_dir = base_dir / table_name / today_str
    dest_dir.mkdir(parents=True, exist_ok=True)
    file_path = dest_dir / f"{run_id}.parquet"
    try: 
        pq.write_table(table, file_path)
        log.debug(f"Wrote {table.num_rows} records to {folder_type}", path=str(file_path))
    except Exception as e:
        log.error(f"Failed to write {folder_type} file: {e}", path=str(file_path), table_name=table_name, run_id=run_id)
        raise