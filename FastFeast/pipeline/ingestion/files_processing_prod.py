import time
import duckdb
import json
import re
import json
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.json as pj
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from pipeline.logger import pipeline as log
from FastFeast.pipeline.config.metadata import metadata_settings
from FastFeast.pipeline.config.config import config_settings


# ----------------------------------------------------------------------
# Tracking table
# ----------------------------------------------------------------------








# ----------------------------------------------------------------------
# Wait for file
# ----------------------------------------------------------------------


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

    if path.suffix == st.csv:
        table = pv.read_csv(str(path))
    elif path.suffix == st.json:
        with open(path, 'r') as f:
            data = load_clean_json(path)
            table = pa.Table.from_pylist(data)
    else:
        log.warning(f"Unsupported format: {path.suffix}")
        return None, None

    if 'updated_at' not in table.column_names:
        return table, datetime.fromtimestamp(path.stat().st_mtime)

    conn = duckdb.connect()
    conn.register('temp', table)
    if last_checkpoint is None:
        result = conn.execute("SELECT * FROM temp")
        filtered = result.to_arrow_table()
    else:
        checkpoint_str = last_checkpoint.strftime(config_settings.datetime_handling.date_time)
        result = conn.execute(
            "SELECT * FROM temp WHERE CAST(updated_at AS TIMESTAMP) > CAST(? AS TIMESTAMP)",
            (checkpoint_str,)
        )
        filtered = result.to_arrow_table()
    conn.close()

    max_updated = None
    if filtered.num_rows > 0:
        import pyarrow.compute as pc
        dates = filtered.column('updated_at')
        max_scalar = pc.max(dates)
        if max_scalar is not None:
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
        last_hash, last_checkpoint = get_last_state(conn, file_name)

        if last_hash == new_hash:
            #log.info(f"SKIPPED (no change): {file_name}")
            return True

        new_records, max_updated = read_and_filter(file_path, last_checkpoint)

        if len(new_records) == 0:
            update_state(conn, file_name, new_hash, last_checkpoint)
            return True

        bronze_table = f"bronze_{path.stem}"
        upsert_bronze(conn, bronze_table, new_records)

        new_checkpoint = max_updated if max_updated is not None else last_checkpoint
        update_state(conn, file_name, new_hash, new_checkpoint)
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
