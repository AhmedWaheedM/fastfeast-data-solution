import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
import shutil 
from pathlib import Path
from support.logger import pipeline as log
from pipeline.config.config import config_settings

def load_and_clear_orphans(table_name:str) -> pa.Table: 
    """
    Loads all current orphans for a specific table so they can be retried.
    Increments their retry count, and DELETES the old orphan files so they 
    don't get processed twice.    
    """
    base_dir = Path(config_settings.paths.output_dir).resolve() / "validation" /  "orphans" / table_name
    if not base_dir.exists() or not any(base_dir.iterdir()):
        log.info("No orphan records found", table_name=table_name)
        return None
    try: 
        dataset = ds.dataset(str(base_dir), format="parquet")
        orphan_table = dataset.to_table()

        new_retry_count = pc.add(orphan_table['_retry_count'], 1)
        orphan_table = orphan_table.set_column(
            orphan_table.schema.get_field_index('_retry_count'), 
            '_retry_count', 
            new_retry_count
        )
        shutil.rmtree(base_dir)  
        log.info(f"Loaded {orphan_table.num_rows} orphan records for retry", table_name=table_name)
        return orphan_table.drop(['_pipeline_run_id', "_processed_at"])  
    except Exception as e:
        log.error(f"Failed to load orphan records: {e}", table_name=table_name)
        return None
