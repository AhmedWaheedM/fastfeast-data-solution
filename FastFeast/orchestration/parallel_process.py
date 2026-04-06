from pathlib import Path
from FastFeast.utilities.validation_utils import (expected_types, not_null_column, column_format, get_column_pk,
                                                  get_column_range, map_format_to_pattern,
                                                    map_type_to_pyarrow, map_type_to_pattern, compose_table)

from FastFeast.pipeline.config.config import get_config
from datetime import datetime
from FastFeast.pipeline.bridge.pyarrow_table import load_file
from FastFeast.utilities.metadata_cash import compare_columns
from FastFeast.pipeline.validation.schema_validation import validate_table
from FastFeast.pipeline.ingestion.file_tracker import mark_processing, generate_run_id, mark_processed, update_stage
from FastFeast.utilities.db_utils import get_connection
from FastFeast.support.logger import pipeline as log
from FastFeast.pipeline.ingestion.listen_to_folder import copy_files
from FastFeast.utilities.file_utils import get_file_hash
from FastFeast.pipeline.config.config import config_settings
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime, timedelta

####################################################
# Get configuration
config = get_config()
date = config.datetime_handling.date_key_format

BASE_DIR = Path(__file__).resolve().parents[2]

source = BASE_DIR / config.paths.batch_dir
dest = BASE_DIR / config.paths.dest_base

####################################################

#Process single file
def process_single_file(file, dest_today, run_id, stream_fk_pk_map, pipeline_type = 'batch'):
    """
    - This function takes every process we need to do in one file
    - Then we will pass this to Thread function, to perform all operations on all files
    """
    conn = None
    try:

        get_connection()

        target_file = dest_today / file.name
        file_hash = get_file_hash(file)

        pa_table = load_file(target_file)


        row_count = pa_table.num_rows

        mark_processing(
                file_path=str(target_file),
                file_hash=file_hash,
                record_count=row_count,
                run_id=run_id
            )
        
        log.info(
                "Processing file",
                file=str(file),
                rows=row_count,
                run_id=run_id
            )


        if compare_columns(file, pa_table, 'batch'):

            expected_types_str = expected_types(file, pipeline_type)
            expected_types_pa = map_type_to_pyarrow(expected_types_str)
            not_null = not_null_column(file, pipeline_type)
            expected_formats_str = column_format(file, pipeline_type)
            expected_formats = map_format_to_pattern(expected_formats_str)
            column_range = get_column_range(file, pipeline_type)
            expected_pattern = map_type_to_pattern(expected_types_str)
            expected_pk = get_column_pk(file, pipeline_type)

            status_list, error_lists, pk_col= validate_table(pa_table, expected_types_pa, expected_pattern,not_null, expected_formats, column_range,expected_pk, pipeline_type)
            full_table = compose_table(pa_table, status_list, error_lists) #DLQ
            stream_fk_pk_map[pk_col] = file

            return True

        else:
            log.error("Schema mismatch", file=file.name, run_id=run_id)
            return False

    except Exception as e:
        log.error("File processing crashed", file=file.name, error=str(e), exc_info=True)
        return False

        
        
    ####################################################

def process_all_files(stream_fk_pk_map):
    today = datetime.now().date()
    today_folder_name = today.strftime(config_settings.datetime_handling.date_key_format)

    source_today = source / today_folder_name
    dest_today = dest / today_folder_name

    if not source_today.exists():
        #log.error("Source folder does not exist", path=str(source_today))
        return False

    dest_today.mkdir(parents=True, exist_ok=True)

    files = [f for f in source_today.glob("*") if f.is_file()]

    log.info("Starting copy stage", total_files=len(files))

    run_id = generate_run_id()
    any_error = False

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(process_single_file, file, dest_today, run_id, stream_fk_pk_map): file
            for file in files
        }
        try:
            for future in as_completed(futures):
                file = futures[future]
                #try:
                if not future.result():
                    #print("ERRORRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR")
                    any_error = True
        except Exception as e:
            log.error("Thread crashed", file=file.name, error=str(e), exc_info=True)
            any_error = True

    if any_error:
        log.error("Copy stage completed WITH errors", run_id=run_id)
    else:
        status = 'PROCESSED'
        mark_processed(file, status, '13', run_id)
        stage = 'VALIDATED'
        update_stage(file, stage, run_id)
        log.info("Copy stage completed WITHOUT errors", run_id=run_id)

    return not any_error


####################################################

def run_pipeline_listener(stream_fk_pk_map):

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

        time.sleep(max(0, sleep_seconds)) #to avoide crash is seconds= 0

        #source_today = source / source_folder_name

        today = datetime.now().strftime(config_settings.datetime_handling.date_key_format)

        source_today = Path(source) / today
        dest_today = Path(dest) / today

        run_id = generate_run_id()
        log.info("Pipeline started", run_id=run_id)

        copy_files(source_today, dest_today)

        process_all_files()


        # Start 1-hour listening window
        window_start = datetime.now()
        timeout = timedelta(hours=1)


        processed = False

        while datetime.now() - window_start < timeout:
            if source_today.exists():
                log.info("Source folder detected", path=str(source_today), run_id=run_id)
                processed = True
                break
            else:
                log.info("Source folder not found yet, retrying...", run_id=run_id)
                time.sleep(30)

        if not processed:
            log.info("Source folder NOT found within 1 hour. Skipping run.", run_id=run_id)
####################################################

if __name__ == "__main__":
    stream_fk_pk_map = {}
    run_pipeline_listener(stream_fk_pk_map)
