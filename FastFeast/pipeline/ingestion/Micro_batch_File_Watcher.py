import os
import logging
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from FastFeast.pipeline.config import config
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

cfg = config.load(str(CONFIG_PATH))
STREAM_DIR   = Path(cfg.paths.stream_dir).resolve()
BRONZE_ROOT  = _ROOT / "data"

MAX_WORKERS  = cfg.pipeline.max_workers
SLEEP_HOURS  = cfg.pipeline.sleep_hours
MAX_ATTEMPTS = cfg.pipeline.max_attempts

logging.basicConfig(
    level=getattr(logging, cfg.logging.level, logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("stream_monitor")

try:
    from FastFeast.pipeline.config.meta_data2 import metadata_settings
    KNOWN_FILES = {f.file_name for f in metadata_settings.stream}
except Exception as _e:
    log.warning("Could not load KNOWN_FILES from metadata, using defaults: %s", _e)
    KNOWN_FILES = {"orders.json", "tickets.csv", "ticket_events.json"}


# ── File processing ───────────────────────────────────────────────────────────

def process_file(filepath: Path, cycle_id: str):
    file_key = str(filepath.resolve())

    if filepath.name not in KNOWN_FILES:
        log.warning("SKIP  unknown file  path=%s", filepath)
        return filepath.stem, None

    if filepath.stat().st_size == 0:
        log.warning("SKIP  empty file  path=%s", filepath)
        return filepath.stem, None

    current_hash = hash_file(filepath)

    # ── Stage 1: PENDING — file detected, waiting to be processed
    update_stage(file_key, "PENDING", cycle_id)
    log.info("PENDING  file detected  path=%s", filepath)

    acquired = try_acquire(file_key, cycle_id, current_hash, MAX_ATTEMPTS)
    if not acquired:
        return filepath.stem, None

    byte_count = filepath.stat().st_size
    log.info("READ  ok  path=%s  size=%d bytes", filepath, byte_count)

    # ── Stage 2: PROCESSING — ingestion has started
    update_stage(file_key, "PROCESSING", cycle_id)
    log.info("PROCESSING  ingestion started  path=%s", filepath)

    return filepath.stem, (filepath, byte_count)


def process_date_folder(date_folder: Path, cycle_id: str) -> dict:
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

        # ── Ingestion
        ok = write_bronze(filepath, date_str)
        if not ok:
            log.error("WRITE  FAIL  file=%s", filepath.name)


def run_cycle(date_str: str, cycle_id: str):
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
    parser.add_argument("--date", default=None, help="Date to monitor (YYYY-MM-DD).")
    parser.add_argument("--once", action="store_true", help="Single pass then exit")
    args = parser.parse_args()

    if args.date:
        try:
            from datetime import datetime
            datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print("ERROR  invalid date: {}  (expected YYYY-MM-DD)".format(args.date))
            import sys; sys.exit(1)

    get_connection()

    if args.once:
        from datetime import date
        current_date = args.date or date.today().isoformat()
        cycle_id     = generate_run_id()
        log.info("ONCE  cycle_id=%s  date=%s", cycle_id, current_date)
        run_cycle(current_date, cycle_id)
    else:
        daemon.run(explicit_date=args.date)


# if __name__ == "__main__":
#     main()


import threading
from datetime import datetime

from FastFeast.pipeline.ingestion import daemon
from FastFeast.pipeline.ingestion.file_tracker import generate_run_id
from FastFeast.support.logger import pipeline as log


def run_batch_pipeline():
    """
    Wrapper to run batch pipeline
    """
    from FastFeast.orchestration.parallel_process import run_pipeline_listener

    log.info("Starting BATCH pipeline")
    run_pipeline_listener()
    log.info("BATCH pipeline finished")


def run_stream_pipeline():
    """
    Wrapper to run stream pipeline (daemon)
    """
    log.info("Starting STREAM pipeline")
    main()
    log.info("STREAM pipeline finished")


def run_integrated_pipeline(mode="both"):
    """
    Integrated orchestrator for batch + stream pipelines.

    mode:
        - "batch"   → run only batch
        - "stream"  → run only stream
        - "both"    → run both together
    """

    run_id = generate_run_id()

    log.info("Integrated pipeline started", run_id=run_id, mode=mode)

    if mode == "batch":
        run_batch_pipeline()

    elif mode == "stream":
        run_stream_pipeline()

    elif mode == "both":
        # Run stream in background thread
        stream_thread = threading.Thread(target=run_stream_pipeline, daemon=True)
        stream_thread.start()

        # Run batch in main thread
        run_batch_pipeline()

        # Optionally wait for stream (if you want blocking behavior)
        stream_thread.join()

    else:
        raise ValueError(f"Unknown mode: {mode}")

    log.info("Integrated pipeline completed", run_id=run_id)

if __name__ == "__main__":
    run_integrated_pipeline('both')