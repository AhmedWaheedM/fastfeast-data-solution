import time
import duckdb
import json
import re
import json
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.json as pj
import pyarrow.compute as pc
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from support.logger import pipeline as log
from pipeline.config.metadata import metadata_settings
from pipeline.config.config import config_settings
from utilities.file_utils import get_file_hash, wait_for_file
from pipeline.ingestion.batch_file_tracker import get_last_state, update_state

# ----------------------------------------------------------------------
# Clean unexpected values like : Nan
# ----------------------------------------------------------------------

def load_clean_json(file_path):
    with open(file_path, "r", encoding=config_settings.batch.encoding) as f:
        content = f.read()
    content = re.sub(r'\bNaN\b', 'null', content)
    return json.loads(content)


# ----------------------------------------------------------------------
# Read and filter new/updated records
# ----------------------------------------------------------------------

def read_and_filter(file_path, last_checkpoint):
    path = Path(file_path)
    st = config_settings.batch.supported_types

    # Read file
    if path.suffix == st.csv:
        table = pv.read_csv(str(path))
    elif path.suffix == st.json:
        data = load_clean_json(path)
        table = pa.Table.from_pylist(data)
    else:
        log.warning(f"Unsupported format: {path.suffix}")
        return None, None

    # If no updated_at → return full table + file modified time
    if 'updated_at' not in table.column_names:
        return table, datetime.fromtimestamp(path.stat().st_mtime)

    updated_col = table['updated_at']

    # Ensure updated_at is timestamp
    if not pa.types.is_timestamp(updated_col.type):
        updated_col = pc.cast(updated_col, pa.timestamp('us'))

    # Filtering
    if last_checkpoint is None:
        filtered = table
    else:
        checkpoint_str = last_checkpoint.strftime(
            config_settings.datetime_handling.date_time
        )
        checkpoint_scalar = pa.scalar(checkpoint_str, type=pa.timestamp('us'))

        mask = pc.greater(updated_col, checkpoint_scalar)
        filtered = table.filter(mask)

    # Get max(updated_at)
    max_updated = None
    if filtered.num_rows > 0:
        max_scalar = pc.max(
            pc.cast(filtered['updated_at'], pa.timestamp('us'))
            if not pa.types.is_timestamp(filtered['updated_at'].type)
            else filtered['updated_at']
        )
        if max_scalar is not None and max_scalar.as_py() is not None:
            max_updated = max_scalar.as_py()

    return filtered, max_updated

# ----------------------------------------------------------------------
# Upsert bronze table (overwrite by a primary key)
# ----------------------------------------------------------------------
def upsert_bronze(conn, table_name, arrow_table):
    if len(arrow_table) == 0:
        return
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM arrow_table WHERE 1=0")
    conn.register("temp", arrow_table)
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp")
    conn.unregister("temp")


# ----------------------------------------------------------------------
# Process a single file (called in a thread)
# ----------------------------------------------------------------------
def process_file(file_path, db_path):
    path = Path(file_path)
    file_name = path.name

    if not wait_for_file(file_path):
        log.warning(f"File missing after 60s: {file_name}")
        return True 
    conn = duckdb.connect(db_path)
    try:
        new_hash = get_file_hash(file_path) 
        last_hash, last_checkpoint = get_last_state(file_name)

        if last_hash == new_hash:
            #log.info(f"SKIPPED (no change): {file_name}")
            return True

        new_records, max_updated = read_and_filter(file_path, last_checkpoint)

        if len(new_records) == 0:
            update_state(file_name, new_hash, last_checkpoint)
            return True

        bronze_table = f"bronze_{path.stem}"
        upsert_bronze(conn, bronze_table, new_records)

        new_checkpoint = max_updated if max_updated is not None else last_checkpoint
        update_state(file_name, new_hash, new_checkpoint)
        #log.info(f"PROCESSED: {file_name} | rows={new_records.num_rows} | checkpoint={new_checkpoint}")
        return True

    except Exception as e:
        log.error(f"Error processing {file_name}: {e}", exc_info=True)
        return False
    finally:
        conn.close()


# ----------------------------------------------------------------------
# Process all files in parallel
# ----------------------------------------------------------------------
def process_all_batch_files(batch_dir, file_list, db_path):
    today = datetime.now().date()
    batch_dir = Path(config_settings.paths.batch_dir)
    today_folder = Path(batch_dir) / today.strftime(config_settings.datetime_handling.date_key_format)

    if not today_folder.exists():
        log.error(f"Batch folder missing: {today_folder}")
        return False

    any_error = False
    with ThreadPoolExecutor(max_workers=config_settings.pipeline.max_workrs) as executor:
        futures = []
        for file_name in file_list:
            file_path = today_folder / file_name
            #print(file_name)
            futures.append(executor.submit(process_file, str(file_path), db_path))

        for future in as_completed(futures):
            try:
                if not future.result():
                    any_error = True
            except Exception as e:
                log.error(f"Unexpected thread error: {e}", exc_info=True)
                any_error = True
        #print(futures)

    return not any_error



# ----------------------------------------------------------------------
# Test
# ----------------------------------------------------------------------
if __name__ == "__main__":

    batch_dir = Path(config_settings.paths.batch_dir)
    EXPECTED_FILES = [f.file_name for f in metadata_settings.batch]

    success = process_all_batch_files(batch_dir, EXPECTED_FILES, config_settings.database.db_name)
    if success:
        log.info("Succeded: Batch stage completed WITHOUT errors.")
    else:
        log.error("Failed: Batch stage completed WITH errors.")
