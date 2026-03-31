import shutil
import time
from datetime import datetime, timedelta
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
def run_pipeline_listener(start_hour=2):

    while True:
        now = datetime.now()

        # generate ONE run_id per pipeline cycle
        run_id = generate_run_id()

        start_time = now.replace(hour=start_hour, minute=0, second=0, microsecond=0)

        if now < start_time:
            sleep_seconds = (start_time - now).total_seconds()
            log.info("Waiting for pipeline start time", sleep_seconds=sleep_seconds)
            time.sleep(sleep_seconds)

        log.info("Pipeline listener started for the day", run_id=run_id)

        window_start = datetime.now()
        timeout = timedelta(hours=1)

        source_folder_name = datetime.now().date().strftime(
            config_settings.datetime_handling.date_key_format
        )
        source_today = SOURCE_BASE / source_folder_name

        processed = False

        while datetime.now() - window_start < timeout:
            if source_today.exists():
                log.info("Source folder detected, starting pipeline", path=str(source_today), run_id=run_id)
                success = get_today_files(run_id)
                processed = True
                break
            else:
                log.info("Source folder not found yet, retrying in 30 seconds", run_id=run_id)
                time.sleep(30)

        if not processed:
            log.info("Source folder NOT found within 1 hour. Skipping to next day.", run_id=run_id)

        next_day = (datetime.now() + timedelta(days=1)).replace(
            hour=start_hour, minute=0, second=0, microsecond=0
        )

        sleep_seconds = (next_day - datetime.now()).total_seconds()
        log.info("Sleeping until next day pipeline start", sleep_seconds=sleep_seconds, run_id=run_id)

        time.sleep(max(0, sleep_seconds))


# ------------------------------------------------------
# Main
# ------------------------------------------------------
if __name__ == "__main__":

    # Run listener instead of one-time execution
    run_pipeline_listener(start_hour=2)
