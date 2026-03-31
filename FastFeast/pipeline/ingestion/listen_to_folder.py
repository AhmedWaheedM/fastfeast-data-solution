import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from FastFeast.support.logger import pipeline as log
from FastFeast.pipeline.config.config import load, yaml_path
from FastFeast.utilities.db_utils import get_connection
from FastFeast.utilities.file_utils import get_file_hash
from FastFeast.pipeline.ingestion.file_tracker import mark_processing, generate_run_id
from FastFeast.dwh.bronze.file_tracking import init_db


# ------------------------------------------------------
# Lazy Config Loader
# ------------------------------------------------------
_config = None

def get_config():
    global _config
    if _config is None:
        _config = load(yaml_path)
    return _config


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
# Process single file
# ------------------------------------------------------
def process_single_file(file, dest_today, run_id):
    conn = get_connection()

    try:
        target_file = dest_today / file.name

        if not wait_for_file(file):
            log.warning(f"File missing after 60s: {target_file}")
            return True

        shutil.copy2(file, target_file)

        file_hash = get_file_hash(file)
        row_count = None

        log.info(
            "Processing file",
            file=str(target_file),
            hash=file_hash,
            rows=row_count,
            run_id=run_id
        )

        mark_processing(
            file_path=str(target_file),
            file_hash=file_hash,
            record_count=row_count,
            run_id=run_id
        )

        log.info("File copied + tracked", file=file.name)

        return True

    except Exception as e:
        log.error("Error processing file", file=file.name, error=str(e), exc_info=True)
        return False


# ------------------------------------------------------
# Get today's folder
# ------------------------------------------------------
def get_today_files(run_id):
    config = get_config()

    today = datetime.now().date()
    today_folder_name = today.strftime(config.datetime_handling.date_key_format)

    source_today = SOURCE_BASE / today_folder_name
    dest_today = DEST_BASE / today_folder_name

    if not source_today.exists():
        log.error("Source folder does not exist", path=str(source_today))
        return False

    dest_today.mkdir(parents=True, exist_ok=True)

    files = [f for f in source_today.glob("*") if f.is_file()]

    log.info("Starting copy stage", total_files=len(files))

    any_error = False

    with ThreadPoolExecutor(max_workers=4) as executor:
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
# Listener / Scheduler
# ------------------------------------------------------
def run_pipeline_listener(conn):

    config = get_config()

    # UPDATED: parse "3:00:00"
    schedule_str = config.batch.schedule  # e.g. "3:00:00"
    parts = schedule_str.split(":")

    hour = int(parts[0])
    minute = int(parts[1]) if len(parts) > 1 else 0
    second = int(parts[2]) if len(parts) > 2 else 0

    while True:
        now = datetime.now()

        # Build today's scheduled datetime
        start_time = now.replace(hour=hour, minute=minute, second=second, microsecond=0)

        # If already passed → schedule for next day
        if now >= start_time:
            start_time = start_time + timedelta(days=1)

        sleep_seconds = (start_time - now).total_seconds()

        log.info(
            "Pipeline scheduled",
            current_time=str(now),
            next_run_time=str(start_time),
            sleep_seconds=sleep_seconds
        )

        time.sleep(max(0, sleep_seconds))

        run_id = generate_run_id()
        log.info("Pipeline started", run_id=run_id)

        window_start = datetime.now()
        timeout = timedelta(hours=1)

        config = get_config()

        source_folder_name = datetime.now().date().strftime(
            config.datetime_handling.date_key_format
        )
        source_today = SOURCE_BASE / source_folder_name

        processed = False

        while datetime.now() - window_start < timeout:
            if source_today.exists():
                log.info("Source folder detected", path=str(source_today), run_id=run_id)
                success = get_today_files(run_id)
                processed = True
                break
            else:
                log.info("Source folder not found yet, retrying...", run_id=run_id)
                time.sleep(30)

        if not processed:
            log.info("Source folder NOT found within 1 hour. Skipping run.", run_id=run_id)


# ------------------------------------------------------
# Main
# ------------------------------------------------------
if __name__ == "__main__":
    conn = init_db("pipeline.duckdb")
    run_pipeline_listener(conn)
