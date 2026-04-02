import os
import logging
import argparse
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
BRONZE_ROOT  = _ROOT / "data"

MAX_WORKERS  = cfg.pipeline.max_workers
SLEEP_HOURS  = cfg.pipeline.sleep_hours
MAX_ATTEMPTS = cfg.pipeline.max_attempts

logging.basicConfig(
    level=getattr(logging, cfg.logging.level, logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log     = logging.getLogger("stream_monitor")
db_lock = threading.Lock()


try:
    from pipeline.config.metadata import metadata_settings
    KNOWN_FILES = {f.file_name for f in metadata_settings.stream}
except Exception as _e:
    log.warning("Could not load KNOWN_FILES from metadata, using defaults: %s", _e)
    KNOWN_FILES = {"orders.json", "tickets.csv", "ticket_events.json"}


# ── File processing ───────────────────────────────────────────────────────────

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
    file_key = str(filepath.resolve())

    if filepath.name not in KNOWN_FILES:
        log.warning("SKIP  unknown file  path=%s", filepath)
        return filepath.stem, None

    if filepath.stat().st_size == 0:
        log.warning("SKIP  empty file  path=%s", filepath)
        return filepath.stem, None

    current_hash = hash_file(filepath)

    with db_lock:
        acquired = try_acquire(file_key, cycle_id, current_hash)

    if not acquired:
        return filepath.stem, None

    byte_count = filepath.stat().st_size
    log.info("READ  ok  path=%s  size=%d bytes", filepath, byte_count)
    update_stage(file_key, "WRITING", cycle_id)
    return filepath.stem, (filepath, byte_count)


def process_date_folder(date_folder: Path, cycle_id: str) -> dict:
    """
    Process all known files inside numbered sub-folders of date_folder.
    Path: stream_dir / 2026-03-31 / 01 / orders.json
    """
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
        ok     = write_bronze(filepath, date_str)
        status = "SUCCESS" if ok else "FAILED"
        if not ok:
            log.error("WRITE  FAIL  file=%s  — marking FAILED", filepath.name)
        mark_processed(file_key, status, byte_count, cycle_id)


def run_cycle(date_str: str, cycle_id: str):
    """Process all files for a single date. Returns after one pass."""
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
    parser.add_argument(
        "--date", default=None,
        help="Date to monitor (YYYY-MM-DD). Omit to track today.",
    )
    parser.add_argument("--once", action="store_true", help="Single pass then exit")
    args = parser.parse_args()

    if args.date:
        try:
            from datetime import datetime
            datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print("ERROR  invalid date: {}  (expected YYYY-MM-DD)".format(args.date))
            import sys; sys.exit(1)

    if args.once:
        from datetime import date
        current_date = args.date or date.today().isoformat()
        cycle_id     = generate_run_id()
        log.info("ONCE  cycle_id=%s  date=%s", cycle_id, current_date)
        run_cycle(current_date, cycle_id)
    else:
        from pipeline.ingestion import daemon
        daemon.run(explicit_date=args.date)


if __name__ == "__main__":
    main()
