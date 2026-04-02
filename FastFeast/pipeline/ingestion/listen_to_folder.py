import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from FastFeast.support.logger import pipeline as log
from FastFeast.utilities.db_utils import get_connection
from FastFeast.utilities.file_utils import get_file_hash
from FastFeast.pipeline.config.config import get_config
from FastFeast.pipeline.ingestion.file_tracker import mark_processing, generate_run_id
from FastFeast.pipeline.bridge.pyarrow_table import load_file

from FastFeast.test.datatype_validator import validate_pipeline
from FastFeast.pipeline.config.metadata import metadata_settings


config = get_config()

BASE_DIR = Path(__file__).resolve().parents[3]

SOURCE_BASE = BASE_DIR / config.paths.batch_dir
DEST_BASE = BASE_DIR / config.paths.dest_base


# ======================================================
# Helper timing function
# ======================================================
def now():
    return time.perf_counter()


# ------------------------------------------------------
# Wait for file
# ------------------------------------------------------
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
def process_single_file(file, dest_today, run_id, conn):

    file_start = now()

    try:
        target_file = dest_today / file.name

        # -------------------------
        # Wait + Copy Stage
        # -------------------------
        t0 = now()

        if not wait_for_file(file):
            log.warning("File missing after timeout", file=file.name)
            return True

        shutil.copy2(file, target_file)

        copy_time = now() - t0

        log.debug(
            "Copy stage",
            file=file.name,
            seconds=copy_time
        )

        # -------------------------
        # Ingestion Stage
        # -------------------------
        t1 = now()

        table = load_file(target_file)

        if table is None:
            log.warning("Failed to load file", file=file.name)
            return False

        ingestion_time = now() - t1

        log.debug(
            "Ingestion stage",
            file=file.name,
            seconds=ingestion_time,
            rows=table.num_rows
        )

        # -------------------------
        # Validation Stage
        # -------------------------
        t2 = now()

        try:
            final_valid, final_invalid, null_masks = validate_pipeline(
                table,
                str(target_file),
                run_id,
                metadata_settings
            )

            validation_time = now() - t2

            log.debug(
                "Validation stage",
                file=file.name,
                seconds=validation_time,
                valid_rows=final_valid.num_rows,
                invalid_rows=final_invalid.num_rows,
                null_masks_count=len(null_masks)
            )

        except Exception as e:
            log.error("Validation failed", file=file.name, error=str(e), exc_info=True)
            return False

        # -------------------------
        # Post-processing
        # -------------------------
        file_hash = get_file_hash(file)

        mark_processing(
            file_path=str(target_file),
            file_hash=file_hash,
            record_count=table.num_rows,
            run_id=run_id
        )

        total_time = now() - file_start

        log.info(
            "File completed",
            file=file.name,
            total_seconds=total_time
        )

        return True

    except Exception as e:
        log.error("Processing failed", file=file.name, error=str(e), exc_info=True)
        return False


# ------------------------------------------------------
# Get today's folder
# ------------------------------------------------------
def get_today_files(run_id):

    conn = get_connection()

    batch_start = now()

    today = datetime.now().date()
    today_folder_name = today.strftime(config.datetime_handling.date_key_format)

    source_today = SOURCE_BASE / today_folder_name
    dest_today = DEST_BASE / today_folder_name

    if not source_today.exists():
        log.error("Source folder missing", path=str(source_today))
        return False

    dest_today.mkdir(parents=True, exist_ok=True)

    files = [f for f in source_today.glob("*") if f.is_file()]

    log.info("Starting processing", total_files=len(files))

    any_error = False

    # -------------------------
    # Thread execution timing
    # -------------------------
    thread_start = now()

    with ThreadPoolExecutor(max_workers=4) as executor:

        futures = {
            executor.submit(process_single_file, file, dest_today, run_id, conn): file
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

    thread_time = now() - thread_start
    total_batch_time = now() - batch_start

    log.info(
        "Batch timing summary",
        threads_seconds=thread_time,
        total_seconds=total_batch_time
    )

    if any_error:
        log.error("Batch completed WITH errors", run_id=run_id)
    else:
        log.info("Batch completed successfully", run_id=run_id)

    return not any_error


# ------------------------------------------------------
# Listener / Scheduler
# ------------------------------------------------------
def run_pipeline_listener():

    schedule_str = config.batch.schedule
    parts = schedule_str.split(":")

    hour = int(parts[0])
    minute = int(parts[1]) if len(parts) > 1 else 0
    second = int(parts[2]) if len(parts) > 2 else 0

    while True:
        now_dt = datetime.now()

        start_time = now_dt.replace(hour=hour, minute=minute, second=second, microsecond=0)

        if now_dt >= start_time:
            start_time = start_time + timedelta(days=1)

        sleep_seconds = (start_time - now_dt).total_seconds()

        log.info(
            "Scheduler waiting",
            current_time=now_dt.isoformat(),
            next_run=start_time.isoformat(),
            sleep_seconds=round(sleep_seconds, 2)
        )

        time.sleep(max(0, sleep_seconds))

        run_id = generate_run_id()
        log.info("Pipeline started", run_id=run_id)

        source_folder_name = datetime.now().date().strftime(
            config.datetime_handling.date_key_format
        )

        source_today = SOURCE_BASE / source_folder_name

        if source_today.exists():
            get_today_files(run_id)
        else:
            log.info("No source folder found, skipping run", run_id=run_id)


# ------------------------------------------------------
# Main
# ------------------------------------------------------
if __name__ == "__main__":
    run_pipeline_listener()