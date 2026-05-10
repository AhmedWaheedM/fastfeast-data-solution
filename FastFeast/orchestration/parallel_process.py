from __future__ import annotations

import argparse
import pyarrow as pa
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
import sys

if __package__ in (None, ""):
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from FastFeast.pipeline.bridge.pyarrow_table import load_file
from FastFeast.pipeline.config.config import config_settings, get_config
from FastFeast.pipeline.config.metadata import load as load_metadata
from FastFeast.pipeline.ingestion.file_tracker import (
    generate_run_id,
    mark_processed,
    mark_processing,
    update_stage,
)
from FastFeast.pipeline.ingestion.listen_to_folder import copy_files
from FastFeast.pipeline.loading.loader import load_to_gold, load_to_silver
from FastFeast.pipeline.validation.schema_validation import validate_table
from FastFeast.support.logger import pipeline as log
from FastFeast.utilities.file_utils import get_file_hash
from FastFeast.utilities.metadata_cache import compare_columns
from FastFeast.utilities.validation_utils import (
    column_format,
    compose_table,
    expected_types,
    get_column_pk,
    get_column_range,
    map_format_to_pattern,
    map_type_to_pattern,
    map_type_to_pyarrow,
    not_null_column,
)

####################################################
config = get_config()
DATE_FMT = config.datetime_handling.date_key_format
MAX_WORKERS = int(getattr(config.pipeline, "max_workers", 4) or 4)

BASE_DIR = Path(__file__).resolve().parents[2]

BATCH_SOURCE_ROOT = BASE_DIR / config.paths.batch_dir
BATCH_DEST_ROOT = BASE_DIR / config.paths.dest_base
MASTER_SOURCE_DIR = BASE_DIR / config.paths.master_dir
MASTER_DEST_DIR = BASE_DIR / "FastFeast" / "data" / "bronze" / "master"
METADATA_PATH = Path(__file__).resolve().parents[1] / "pipeline" / "config" / "files_metadata.yaml"

try:
    _meta = load_metadata(METADATA_PATH)
    SUPPORTED_DIMENSION_FILES = {f.file_name for f in _meta.batch}
    DIM_META_BY_FILE = {f.file_name: f for f in _meta.batch}
except Exception as exc:
    log.warning("Could not load dimension metadata; processing all files: %s", exc)
    SUPPORTED_DIMENSION_FILES = set()
    DIM_META_BY_FILE = {}

tracker_db_lock = threading.Lock()


def _filter_valid_rows(table: pa.Table, status_list: list[str]) -> pa.Table:
    valid_indices = [i for i, status in enumerate(status_list) if str(status).upper() == "VALID"]
    if not valid_indices:
        return table.slice(0, 0)
    return table.take(pa.array(valid_indices, type=pa.int64()))

####################################################

def process_single_file(file_path: Path, dest_folder: Path, run_id: str, pipeline_type: str = "batch") -> bool:
    """
    - This function takes every process we need to do in one file
    - Then we will pass this to Thread function, to perform all operations on all files
    """
    target_file = dest_folder / file_path.name
    try:
        if not target_file.exists():
            log.error("Copied file not found", file=str(target_file), run_id=run_id)
            return False

        file_hash = get_file_hash(target_file)

        pa_table = load_file(target_file)


        row_count = pa_table.num_rows

        with tracker_db_lock:
            mark_processing(
                file_path=str(target_file.resolve()),
                file_hash=file_hash,
                record_count=row_count,
                run_id=run_id,
            )
        
        log.info(
                "Processing file",
                file=str(file_path),
                rows=row_count,
                run_id=run_id,
            )


        if compare_columns(target_file, pa_table, pipeline_type):

            expected_types_str = expected_types(target_file, pipeline_type)
            expected_types_pa = map_type_to_pyarrow(expected_types_str)
            not_null = not_null_column(target_file, pipeline_type)
            expected_formats_str = column_format(target_file, pipeline_type)
            expected_formats = map_format_to_pattern(expected_formats_str)
            column_range = get_column_range(target_file, pipeline_type)
            expected_pattern = map_type_to_pattern(expected_types_str)
            expected_pk = get_column_pk(target_file, pipeline_type)

            status_list, error_lists, pk_col= validate_table(pa_table, expected_types_pa, expected_pattern,not_null, expected_formats, column_range,expected_pk, pipeline_type)
            compose_table(pa_table, status_list, error_lists)

            valid_table = _filter_valid_rows(pa_table, status_list)
            file_meta = DIM_META_BY_FILE.get(file_path.name)

            silver_result = load_to_silver(file_path.name, valid_table, file_meta, is_batch=True)
            if not silver_result.success:
                with tracker_db_lock:
                    update_stage(str(target_file.resolve()), "FAILED_SILVER_LOAD", run_id)
                log.error(
                    "Silver load failed",
                    file=file_path.name,
                    error=silver_result.error,
                    run_id=run_id,
                )
                return False

            gold_result = load_to_gold(file_path.name, file_meta)
            if not gold_result.success:
                with tracker_db_lock:
                    update_stage(str(target_file.resolve()), "LOADED_SILVER_ONLY", run_id)
                log.warning(
                    "Gold load skipped; Silver load already succeeded",
                    file=file_path.name,
                    error=gold_result.error,
                    run_id=run_id,
                )
                gold_inserted = 0
            else:
                with tracker_db_lock:
                    update_stage(str(target_file.resolve()), "LOADED_GOLD", run_id)
                gold_inserted = gold_result.inserted

            with tracker_db_lock:
                mark_processed(str(target_file.resolve()), "PROCESSED", row_count, run_id)

            log.info(
                "File validated and loaded",
                file=file_path.name,
                run_id=run_id,
                valid_rows=valid_table.num_rows,
                attempted_rows=row_count,
                silver_inserted=silver_result.inserted,
                gold_inserted=gold_inserted,
            )

            return True

        else:
            with tracker_db_lock:
                update_stage(str(target_file.resolve()), "FAILED_SCHEMA", run_id)
            log.error("Schema mismatch", file=file_path.name, run_id=run_id)
            return False

    except Exception as e:
        with tracker_db_lock:
            update_stage(str(target_file.resolve()), "FAILED", run_id)
        log.error("File processing crashed", file=file_path.name, error=str(e), run_id=run_id, exc_info=True)
        return False

        
        
    ####################################################

def _process_folder(source_dir: Path, dest_dir: Path, run_id: str, phase_name: str) -> bool:
    if not source_dir.exists() or not source_dir.is_dir():
        log.error("Phase source folder missing", phase=phase_name, path=str(source_dir))
        return False

    all_files = sorted(f for f in source_dir.iterdir() if f.is_file())
    if SUPPORTED_DIMENSION_FILES:
        files = [f for f in all_files if f.name in SUPPORTED_DIMENSION_FILES]
        skipped = [f.name for f in all_files if f.name not in SUPPORTED_DIMENSION_FILES]
        if skipped:
            log.info("Skipping non-dimension files", phase=phase_name, skipped=", ".join(skipped))
    else:
        files = all_files

    if not files:
        log.warning("Phase has no eligible files", phase=phase_name, path=str(source_dir))
        return True

    dest_dir.mkdir(parents=True, exist_ok=True)
    copy_files(source_dir, dest_dir)

    log.info("Phase started", phase=phase_name, source=str(source_dir), dest=str(dest_dir), total_files=len(files), run_id=run_id)

    any_error = False
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_file, file, dest_dir, run_id, "batch"): file for file in files}

        for future in as_completed(futures):
            file = futures[future]
            try:
                if not future.result():
                    any_error = True
                    log.error("File failed in phase", phase=phase_name, file=file.name, run_id=run_id)
            except Exception as exc:
                any_error = True
                log.error("Thread crashed", phase=phase_name, file=file.name, error=str(exc), exc_info=True)

    if any_error:
        log.error("Phase completed WITH errors", phase=phase_name, run_id=run_id)
        return False

    log.info("Phase completed WITHOUT errors", phase=phase_name, run_id=run_id)
    return True


def process_master_files(run_id: str | None = None) -> bool:
    run_id = run_id or generate_run_id()
    return _process_folder(MASTER_SOURCE_DIR, MASTER_DEST_DIR, run_id, phase_name="DIMENSIONS_MASTER")


def process_batch_files(date_str: str | None = None, run_id: str | None = None) -> bool:
    if not date_str:
        date_str = datetime.now().strftime(config_settings.datetime_handling.date_key_format)

    source_today = BATCH_SOURCE_ROOT / date_str
    dest_today = BATCH_DEST_ROOT / date_str
    run_id = run_id or generate_run_id()

    return _process_folder(source_today, dest_today, run_id, phase_name=f"DIMENSIONS_BATCH:{date_str}")


def process_dimension_phase(date_str: str | None = None) -> bool:
    if not date_str:
        date_str = datetime.now().strftime(config_settings.datetime_handling.date_key_format)

    log.info("PHASE 1 START  dimensions load")
    master_ok = process_master_files(run_id=generate_run_id())
    if not master_ok:
        log.error("PHASE 1 FAILED  master dimensions did not complete")
        return False

    log.info("PHASE BARRIER reached  master dimensions complete")

    batch_ok = process_batch_files(date_str=date_str, run_id=generate_run_id())
    if not batch_ok:
        log.error("PHASE 1 FAILED  batch dimensions did not complete")
        return False

    log.info("PHASE 1 SUCCESS  all dimensions loaded")
    return True


def process_all_files(date_str: str | None = None) -> bool:
    # Backward-compatible alias used by existing tests/scripts.
    return process_dimension_phase(date_str=date_str)


####################################################

def run_pipeline_listener():

    schedule_str = config.batch.schedule  # e.g. "3:00:00"
    parts = schedule_str.split(":")

    hour = int(parts[0])
    minute = int(parts[1]) if len(parts) > 1 else 0
    second = int(parts[2]) if len(parts) > 2 else 0

    while True:
        now = datetime.now()

        # Build today's scheduled datetime
        start_time = now.replace(hour=hour, minute=minute, second=second, microsecond=0)

        # If already passed -> schedule for next day
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

        today = datetime.now().strftime(DATE_FMT)
        run_id = generate_run_id()
        log.info("Pipeline started", run_id=run_id, date=today)

        ok = process_dimension_phase(date_str=today)
        if ok:
            log.info("Scheduled dimension phase completed", run_id=run_id, date=today)
        else:
            log.error("Scheduled dimension phase failed", run_id=run_id, date=today)
####################################################

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dimension phase processor")
    parser.add_argument("--once", action="store_true", help="Run once then exit")
    parser.add_argument("--date", default=None, help="Batch date (YYYY-MM-DD). Defaults to today.")
    args = parser.parse_args()

    if args.once:
        success = process_dimension_phase(date_str=args.date)
        raise SystemExit(0 if success else 1)

    run_pipeline_listener()
