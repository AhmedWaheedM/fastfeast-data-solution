import hashlib
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
def init_tracker(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS batch_file_tracker (
            file_name VARCHAR PRIMARY KEY,
            last_hash VARCHAR,
            last_checkpoint TIMESTAMP
        )
    """)
    


def get_file_hash(file_path):
    sha = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha.update(chunk)
    return sha.hexdigest()


def get_last_state(conn, file_name):
    row = conn.execute(
        "SELECT last_hash, last_checkpoint FROM batch_file_tracker WHERE file_name = ?",
        (file_name,)
    ).fetchone()
    return row if row else (None, None)


def update_state(conn, file_name, file_hash, checkpoint):
    conn.execute("""
        INSERT OR REPLACE INTO batch_file_tracker (file_name, last_hash, last_checkpoint)
        VALUES (?, ?, ?)
    """, (file_name, file_hash, checkpoint))


# ----------------------------------------------------------------------
# Wait for file
# ----------------------------------------------------------------------
def wait_for_file(file_path, timeout_sec=60):
    path = Path(file_path)
    start = time.time()
    while not path.exists():
        if time.time() - start > timeout_sec:
            return False
        time.sleep(1)
    return True

# ----------------------------------------------------------------------
# Clean unexpected values like : Nan
# ----------------------------------------------------------------------

def load_clean_json(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Replace NaN with null
    content = re.sub(r'\bNaN\b', 'null', content)

    return json.loads(content)


# ----------------------------------------------------------------------
# Read and filter new/updated records
# ----------------------------------------------------------------------
def read_and_filter(file_path, last_checkpoint):
    path = Path(file_path)
    print("######File Path:######",path)
    #batch_dir = Path(config_settings.paths.batch_dir)

    st = config_settings.batch.supported_types

    #if path.suffix == '.csv':
    if path.suffix == st.csv:
        table = pv.read_csv(str(path))
    #elif path.suffix == '.json':
    elif path.suffix == st.json:
        #table = pj.read_json(str(path))
        with open(path, 'r') as f:
            data = load_clean_json(path)
            table = pa.Table.from_pylist(data)

    else:
        log.warning(f"Unsupported format: {path.suffix}")

    # If no updated_date column, load everything
    #if 'updated_date' not in metadata_settings.batch:
    if not any(f for f in metadata_settings.batch if any(c.name == 'updated_at' for c in f.columns)):
        ##log.warning(f"No 'updated_date' column in {path.name}. Loading all records.")
        return table, datetime.fromtimestamp(path.stat().st_mtime)

    # Use DuckDB for date filtering
    conn = duckdb.connect()
    conn.register('temp', table)
    if last_checkpoint is None:
        filtered = conn.execute("SELECT * FROM temp").arrow()
    else:
        filtered = conn.execute(
            "SELECT * FROM temp WHERE updated_at > ?",
            (last_checkpoint,)
        ).arrow()
    conn.close()

    # Compute max updated_date
    max_updated = None
    if len(filtered) > 0:
        # Get the column as PyArrow array
        dates = filtered.column('updated_at')
        valid = dates.is_valid()
        if valid.any():
            max_updated = dates.filter(valid).max().as_py()
            print("😍🐱‍🐉", max_updated)

    return filtered, max_updated


# ----------------------------------------------------------------------
# Upsert bronze table (overwrite by a primary key)
# ----------------------------------------------------------------------
def upsert_bronze(conn, table_name, arrow_table, pk_col):
    if len(arrow_table) == 0:
        return
    # Create table if not exists
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM arrow_table WHERE 1=0")
    conn.register("temp", arrow_table)
    # Delete existing rows with matching PK
    conn.execute(f"""
        DELETE FROM {table_name} t
        USING temp u
        WHERE t.{pk_col} = u.{pk_col}
    """)
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp")
    conn.unregister("temp")


# ----------------------------------------------------------------------
# Process a single file (called in a thread)
# ----------------------------------------------------------------------
def process_file(file_path, pk_col, db_path):
    path = Path(file_path)
    file_name = path.name

    # 1. Wait for file (1 minute)
    if not wait_for_file(file_path):
        log.warning(f"File missing after 60s: {file_name}")
        return True   # missing is not an error

    conn = duckdb.connect(db_path)
    init_tracker(conn)
    try:
        # 2. Get hash and last state
        new_hash = get_file_hash(file_path) 
        last_hash, last_checkpoint = get_last_state(conn, file_name)

        # 3. If unchanged, skip
        if last_hash == new_hash:
            log.info(f"File unchanged: {file_name}")
            return True

        # 4. Read new/updated records
        new_records, max_updated = read_and_filter(file_path, last_checkpoint)

        if len(new_records) == 0:
            # No new records, just update hash
            update_state(conn, file_name, new_hash, last_checkpoint)
            log.info(f"No new records in {file_name}, updated hash only")
            return True

        # 5. Upsert into bronze
        bronze_table = f"bronze_{path.stem}"
        upsert_bronze(conn, bronze_table, new_records, pk_col)

        # 6. Update tracker
        new_checkpoint = max_updated if max_updated is not None else last_checkpoint
        update_state(conn, file_name, new_hash, new_checkpoint)

        log.info(f"Processed {len(new_records)} rows from {file_name}, checkpoint={new_checkpoint}")
        return True

    except Exception as e:
        log.error(f"Error processing {file_name}: {e}", exc_info=True)
        return False
    finally:
        conn.close()


# ----------------------------------------------------------------------
# Process all files in parallel
# ----------------------------------------------------------------------
def process_all_batch_files(batch_dir, file_list, pk_mapping, db_path):
    today = datetime.now().date()
    batch_dir = Path(config_settings.paths.batch_dir)

    today_folder = Path(batch_dir) / today.strftime("%Y-%m-%d")
    print("Today Date:",today)
    print("Today Folder:", today_folder)
    if not today_folder.exists():
        log.error(f"Batch folder missing: {today_folder}")
        return False

    any_error = False
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for file_name in file_list:
            pk_col = pk_mapping.get(file_name)
            if not pk_col:
                log.warning(f"No PK column defined for {file_name}, skipping")
                continue
            file_path = today_folder / file_name
            futures.append(executor.submit(process_file, str(file_path), pk_col, db_path))

        for future in as_completed(futures):
            try:
                if not future.result():
                    any_error = True
            except Exception as e:
                log.error(f"Unexpected thread error: {e}", exc_info=True)
                any_error = True

    return not any_error

# ----------------------------------------------------------------------
# Debug
# ----------------------------------------------------------------------

def find_functions(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if callable(v):
                print("Function found in key:", k)
            find_functions(v)
    elif isinstance(obj, list):
        for x in obj:
            find_functions(x)

# ----------------------------------------------------------------------
# Test
# ----------------------------------------------------------------------
if __name__ == "__main__":

    print("👵👵👵👵👵👵👵👵👵👵👵👵👵👵👵👵")
    conn = duckdb.connect("fastfeast.duckdb")
    result=conn.execute("""
       SELECT * FROM batch_file_tracker
    """).fetchall()
    print(result)
    print("👵👵👵👵👵👵👵👵👵👵👵👵👵👵👵👵")

    batch_dir = Path(config_settings.paths.batch_dir)
    EXPECTED_FILES = [f.file_name for f in metadata_settings.batch]
    PK_MAPPING = {
        f.file_name: next((c.name for c in f.columns if getattr(c, "pk", False)), None)
        for f in metadata_settings.batch
    }

    success = process_all_batch_files("data/input/batch", EXPECTED_FILES, PK_MAPPING, "fastfeast.duckdb")
    if success:
        log.info("!!!!!!!!!!!!!!!!!!!Batch stage completed without errors.!!!!!!!!!!!!!!!!!!!!")
    else:
        log.error("Batch stage completed with errors.")

    result = get_last_state(conn, "agents.csv")
    print("##########################################", result) 
