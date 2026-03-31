import shutil
import pyarrow.csv as pv
import time
import pyarrow.json as pj
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from FastFeast.support.logger import pipeline as log
from FastFeast.pipeline.config.config import config_settings
from FastFeast.utilities.db_utils import get_connection
from FastFeast.utilities.file_utils import get_file_hash

from FastFeast.pipeline.ingestion.file_tracker import mark_processing, generate_run_id


# ------------------------------------------------------
# Paths
# ------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[3]

SOURCE_BASE = BASE_DIR / "data" / "input" / "batch"
DEST_BASE = BASE_DIR / "FastFeast" / "input_data"


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


# ------------------------------------------------------
# Process single file (NOW includes run_id)
# ------------------------------------------------------
def process_single_file(file, dest_today, run_id):
    conn = get_connection()

    try:
        target_file = dest_today / file.name

        # 1. Wait for file
        if not wait_for_file(file):
            log.warning(f"File missing after 60s: {target_file}")
            return True

        # 2. Copy file (no parsing)
        shutil.copy2(file, target_file)

        # 3. Hash (optional)
        file_hash = get_file_hash(file)

        log.info(
            "Processing file",
            file=str(target_file),
            hash=file_hash,
            run_id=run_id
        )

        # 4. Mark tracking
        mark_processing(
            file_path=str(target_file),
            file_hash=file_hash,
            run_id=run_id
        )

        log.info("File copied + tracked", file=file.name)

        return True

    except Exception as e:
        log.error("Error processing file", file=file.name, error=str(e), exc_info=True)
        return False


# ------------------------------------------------------
# Get today's folder processing
# ------------------------------------------------------
def get_today_files():
    today = datetime.now().date()
    today_folder_name = today.strftime(config_settings.datetime_handling.date_key_format)

    source_today = SOURCE_BASE / today_folder_name
    dest_today = DEST_BASE / today_folder_name

    if not source_today.exists():
        log.error("Source folder does not exist", path=str(source_today))
        return False

    dest_today.mkdir(parents=True, exist_ok=True)

    files = [f for f in source_today.glob("*") if f.is_file()]

    log.info("Starting copy stage", total_files=len(files))

    #Generate ONE run_id per pipeline execution
    run_id = generate_run_id()

    any_error = False

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(process_single_file, file, dest_today, run_id): file
            for file in files
        }

        for future in as_completed(futures):
            file = futures[future]
            try:
                if not future.result():
                    any_error = True
            except Exception as e:
                log.error("Thread crashed", file=file.name, error=str(e), exc_info=True)
                any_error = True

    if any_error:
        log.error("Copy stage completed WITH errors", run_id=run_id)
    else:
        log.info("Copy stage completed WITHOUT errors", run_id=run_id)

    return not any_error


# ------------------------------------------------------
# Check pending files
# ------------------------------------------------------
def check_pending_files():
    conn = get_connection()
    rows = conn.execute("""
        SELECT FILE_PATH, RECORD_COUNT
        FROM fastfeast.FILE_TRACKING
        WHERE STATUS = 'PROCESSING'
    """).fetchall()

    print("Pending files:")
    for r in rows:
        print(f"File: {r[0]} | Rows: {r[1]}")


# ------------------------------------------------------
# Main
# ------------------------------------------------------
if __name__ == "__main__":

    success = get_today_files()

    if success:
        log.info("File copy stage completed successfully.")
    else:
        log.error("File copy stage failed.")

    print("==================================================")
    check_pending_files()
    print("==================================================")

    conn = get_connection()

    result = conn.execute(
        "SELECT STATUS, pipeline_run_id, FROM fastfeast.FILE_TRACKING"
    ).fetchall()

    print("TOTAL TRACKED FILES:", result)

