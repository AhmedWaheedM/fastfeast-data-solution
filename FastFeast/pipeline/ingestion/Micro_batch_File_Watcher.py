import os
import logging
import argparse
<<<<<<< HEAD
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from pipeline.config import config
from pipeline.ingestion.bronze_writer import write as write_bronze
from pipeline.ingestion.file_tracker import (
    is_processed,
    acquire_file,
    mark_processed,
    update_stage,
    generate_run_id,
    get_attempts,
    get_stored_hash,
    hash_file,
)

# ── Config ────────────────────────────────────────────────────────────────────
_HERE       = Path(__file__).resolve().parent       # pipeline/ingestion/
_PIPELINE   = _HERE.parent                          # pipeline/
_ROOT       = _PIPELINE.parent                      # FastFeast/
CONFIG_PATH = _PIPELINE / "config" / "config.yaml"

cfg = config.load(str(CONFIG_PATH))
STREAM_DIR   = Path(cfg.paths.stream_dir).resolve()
DB_PATH      = Path(os.path.normpath(_PIPELINE / cfg.database.file)).resolve()
=======
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from FastFeast.pipeline.bridge.pyarrow_table import load_file
from FastFeast.utilities.metadata_cash import compare_columns
from FastFeast.pipeline.validation.schema_validation import validate_table

from FastFeast.utilities.validation_utils import (expected_types, not_null_column, column_format, get_column_pk,
                                                  get_column_range, map_format_to_pattern,
                                                    map_type_to_pyarrow, map_type_to_pattern, compose_table)

from FastFeast.pipeline.config.config import get_config
from FastFeast.pipeline.ingestion.bronze_writer import write as write_bronze
from FastFeast.pipeline.ingestion.file_tracker import (
    try_acquire,
    update_stage,
    generate_run_id,
    hash_file,
)
from FastFeast.utilities.db_utils import DB_PATH
from FastFeast.utilities.db_utils import get_connection
from FastFeast.pipeline.ingestion import daemon

# ── Config ────────────────────────────────────────────────────────────────────
_HERE       = Path(__file__).resolve().parent
_PIPELINE   = _HERE.parent
_ROOT       = _PIPELINE.parent
CONFIG_PATH = _PIPELINE / "config" / "config.yaml"

cfg = get_config()
STREAM_DIR   = Path(cfg.paths.stream_dir).resolve()
>>>>>>> origin/dev
BRONZE_ROOT  = _ROOT / "data"

MAX_WORKERS  = cfg.pipeline.max_workers
SLEEP_HOURS  = cfg.pipeline.sleep_hours
MAX_ATTEMPTS = cfg.pipeline.max_attempts

logging.basicConfig(
    level=getattr(logging, cfg.logging.level, logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
<<<<<<< HEAD
log     = logging.getLogger("stream_monitor")
db_lock = threading.Lock()


try:
    from pipeline.config.metadata import metadata_settings
    KNOWN_FILES = {f.file_name for f in metadata_settings.stream}
=======
log = logging.getLogger("stream_monitor")
yaml_path = Path(__file__).parent.parent.parent / cfg.paths.metadata_yaml

#print("GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG", yaml_path)

try:
    from FastFeast.pipeline.config.metadata import load
    meta = load(yaml_path)
    KNOWN_FILES = {f.file_name for f in meta.stream}
>>>>>>> origin/dev
except Exception as _e:
    log.warning("Could not load KNOWN_FILES from metadata, using defaults: %s", _e)
    KNOWN_FILES = {"orders.json", "tickets.csv", "ticket_events.json"}


# ── File processing ───────────────────────────────────────────────────────────

<<<<<<< HEAD
def try_acquire(file_key: str, cycle_id: str, current_hash: str) -> bool:
    attempts = get_attempts(file_key)
    if attempts >= MAX_ATTEMPTS:
        log.warning("SKIP  max attempts reached (%d)  path=%s", MAX_ATTEMPTS, file_key)
        return False

    if get_stored_hash(file_key) == current_hash and is_processed(file_key):
        log.debug("SKIP  unchanged successful file  path=%s", file_key)
        return False

    acquire_file(file_key, current_hash, cycle_id)
    log.info("ACQUIRED  path=%s  attempt=%d", file_key, attempts + 1)
    return True


def process_file(filepath: Path, cycle_id: str):
=======
def process_file(filepath: Path, cycle_id: str, pipeline_type="stream"):
>>>>>>> origin/dev
    file_key = str(filepath.resolve())

    if filepath.name not in KNOWN_FILES:
        log.warning("SKIP  unknown file  path=%s", filepath)
        return filepath.stem, None

    if filepath.stat().st_size == 0:
        log.warning("SKIP  empty file  path=%s", filepath)
        return filepath.stem, None

    current_hash = hash_file(filepath)

<<<<<<< HEAD
    with db_lock:
        acquired = try_acquire(file_key, cycle_id, current_hash)

=======
    # ── Stage 1: PENDING — file detected, waiting to be processed
    update_stage(file_key, "PENDING", cycle_id)
    log.info("PENDING  file detected  path=%s", filepath)

    acquired = try_acquire(file_key, cycle_id, current_hash, MAX_ATTEMPTS)
>>>>>>> origin/dev
    if not acquired:
        return filepath.stem, None

    byte_count = filepath.stat().st_size
    log.info("READ  ok  path=%s  size=%d bytes", filepath, byte_count)
<<<<<<< HEAD
    update_stage(file_key, "WRITING", cycle_id)
=======

    # ── Stage 2: PROCESSING — ingestion has started
    update_stage(file_key, "PROCESSING", cycle_id)
    log.info("PROCESSING  ingestion started  path=%s", filepath)
    ################################################################
    # We need from here copied file to convert into PyArrow table then pass it to validation
    ################################################################
    
    # pa_table = load_file(target_file)

    # if compare_columns(filepath, pa_table, 'batch'):

    #         expected_types_str = expected_types(filepath, pipeline_type)
    #         expected_types_pa = map_type_to_pyarrow(expected_types_str)
    #         not_null = not_null_column(filepath, pipeline_type)
    #         expected_formats_str = column_format(filepath, pipeline_type)
    #         expected_formats = map_format_to_pattern(expected_formats_str)
    #         column_range = get_column_range(filepath, pipeline_type)
    #         expected_pattern = map_type_to_pattern(expected_types_str)
    #         expected_pk = get_column_pk(filepath, pipeline_type)

    #         status_list, error_lists= validate_table(pa_table, expected_types_pa, expected_pattern,not_null, expected_formats, column_range,expected_pk, pipeline_type)
    #         full_table = compose_table(pa_table, status_list, error_lists) #DLQ
    #         #stream_fk_pk_map[pk_col] = file

    #         return True

>>>>>>> origin/dev
    return filepath.stem, (filepath, byte_count)


def process_date_folder(date_folder: Path, cycle_id: str) -> dict:
<<<<<<< HEAD
    """
    Process all known files inside numbered sub-folders of date_folder.
    Path: stream_dir / 2026-03-31 / 01 / orders.json
    """
=======
>>>>>>> origin/dev
    futures = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for sub_folder in sorted(date_folder.iterdir()):
            if not sub_folder.is_dir():
                continue
            for filename in KNOWN_FILES:
                filepath = sub_folder / filename
                if not filepath.exists():
                    continue
                log.info("DETECTED  queuing  path=%s", filepath)
                futures[executor.submit(process_file, filepath, cycle_id)] = str(filepath.resolve())

    batch = {}
    for future in as_completed(futures):
        file_key = futures[future]
        try:
            table_name, result = future.result()
            if result is not None:
                batch[table_name] = (result, file_key)
        except Exception as exc:
            log.error("THREAD  unhandled exception  file=%s  reason=%s", file_key, exc)

    return batch


def write_results(date_str: str, batch: dict, cycle_id: str):
    for table_name, ((filepath, byte_count), file_key) in batch.items():
        log.info("WRITE  file=%s  bytes=%d", filepath.name, byte_count)
<<<<<<< HEAD
        ok     = write_bronze(filepath, date_str)
        status = "SUCCESS" if ok else "FAILED"
        if not ok:
            log.error("WRITE  FAIL  file=%s  — marking FAILED", filepath.name)
        mark_processed(file_key, status, byte_count, cycle_id)


def run_cycle(date_str: str, cycle_id: str):
    """Process all files for a single date. Returns after one pass."""
=======

        # ── Ingestion
        ok = write_bronze(filepath, date_str)
        
        if not ok:
            log.error("WRITE  FAIL  file=%s", filepath.name)


def run_cycle(date_str: str, cycle_id: str):
>>>>>>> origin/dev
    date_folder = STREAM_DIR / date_str
    if not date_folder.exists():
        log.info("CYCLE  folder not found yet  path=%s", date_folder)
        return

    batch = process_date_folder(date_folder, cycle_id)
    if batch:
        log.info("CYCLE  processed  tables=%d  (%s)", len(batch), ", ".join(batch))
        write_results(date_str, batch, cycle_id)
    else:
        log.info("CYCLE  nothing new  date=%s", date_str)


def main():
    parser = argparse.ArgumentParser(description="FastFeast micro-batch stream monitor")
<<<<<<< HEAD
    parser.add_argument(
        "--date", default=None,
        help="Date to monitor (YYYY-MM-DD). Omit to track today.",
    )
=======
    parser.add_argument("--date", default=None, help="Date to monitor (YYYY-MM-DD).")
>>>>>>> origin/dev
    parser.add_argument("--once", action="store_true", help="Single pass then exit")
    args = parser.parse_args()

    if args.date:
        try:
            from datetime import datetime
            datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print("ERROR  invalid date: {}  (expected YYYY-MM-DD)".format(args.date))
            import sys; sys.exit(1)

<<<<<<< HEAD
=======
    get_connection()

>>>>>>> origin/dev
    if args.once:
        from datetime import date
        current_date = args.date or date.today().isoformat()
        cycle_id     = generate_run_id()
        log.info("ONCE  cycle_id=%s  date=%s", cycle_id, current_date)
        run_cycle(current_date, cycle_id)
    else:
<<<<<<< HEAD
        from pipeline.ingestion import daemon
=======
>>>>>>> origin/dev
        daemon.run(explicit_date=args.date)


if __name__ == "__main__":
<<<<<<< HEAD
    main()
=======
    main()
>>>>>>> origin/dev
