import hashlib
import os
import sys
import time
import logging
import argparse
import threading
from datetime import date, datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

SCRIPT_DIR   = Path(os.path.abspath(__file__)).parent
PIPELINE_DIR = SCRIPT_DIR.parent
ROOT_DIR     = PIPELINE_DIR.parent
CONFIG_DIR   = PIPELINE_DIR / "config"

for _p in (str(ROOT_DIR), str(PIPELINE_DIR), str(CONFIG_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import config
from utilities.db_utils import get_connection
from ingestion.file_tracker import (
    is_processed,
    mark_processing,
    mark_processed,
    update_stage,
    generate_run_id,
)

#  Config 
CONFIG_PATH  = CONFIG_DIR / "config.yaml"
cfg          = config.load(str(CONFIG_PATH))

stream_rel   = cfg.paths.stream_dir.lstrip("./").lstrip("../").replace("../", "")
STREAM_DIR   = Path("D:\\Python\\Fast Feast\\data\\stream").resolve()
DB_PATH      = Path(os.path.normpath(PIPELINE_DIR / cfg.database.file)).resolve()
MAX_WORKERS  = 4
SLEEP_HOURS  = 1
MAX_ATTEMPTS = 3

BRONZE_ROOT  = ROOT_DIR / "data" / "bronze"

logging.basicConfig(
    level=getattr(logging, cfg.logging.level, logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("stream_monitor")

con     = None
db_lock = threading.Lock()

KNOWN_FILES = {"orders.json", "tickets.csv", "ticket_events.json"}


#  Database
def setup_schema(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS FILE_TRACKING (
            FILE_PATH        TEXT PRIMARY KEY,
            PROCESSED_AT     TIMESTAMP,
            STATUS           TEXT,
            RECORD_COUNT     INTEGER DEFAULT 0,
            PIPELINE_RUN_ID  TEXT,
            CURRENT_STAGE    TEXT    DEFAULT 'PENDING',
            FILE_HASH        TEXT,
            ATTEMPT_COUNT    INTEGER DEFAULT 0
        )
    """)
    con.execute("ALTER TABLE FILE_TRACKING ADD COLUMN IF NOT EXISTS FILE_HASH TEXT")
    con.execute("ALTER TABLE FILE_TRACKING ADD COLUMN IF NOT EXISTS ATTEMPT_COUNT INTEGER DEFAULT 0")
    con.commit()
    log.debug("SCHEMA  FILE_TRACKING ensured")


def setup_db():
    global con
    os.chdir(ROOT_DIR)
    con = get_connection()
    with db_lock:
        setup_schema(con)


# Hash
def hash_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def get_attempts(con, file_key):
    row = con.execute(
        "SELECT ATTEMPT_COUNT FROM FILE_TRACKING WHERE FILE_PATH = ?", [file_key]
    ).fetchone()
    return row[0] if row else 0


def get_stored_hash(con, file_key):
    row = con.execute(
        "SELECT FILE_HASH FROM FILE_TRACKING WHERE FILE_PATH = ?", [file_key]
    ).fetchone()
    return row[0] if row else None



# save hash and increment attempt count
def save_hash(con, file_key, new_hash):
    con.execute("""
        INSERT INTO FILE_TRACKING (FILE_PATH, FILE_HASH, ATTEMPT_COUNT, STATUS,
                                   CURRENT_STAGE, PROCESSED_AT, RECORD_COUNT)
        VALUES (?, ?, 1, 'PROCESSING', 'PENDING', CURRENT_TIMESTAMP, 0)
        ON CONFLICT (FILE_PATH) DO UPDATE SET
            FILE_HASH     = excluded.FILE_HASH,
            ATTEMPT_COUNT = FILE_TRACKING.ATTEMPT_COUNT + 1
    """, [file_key, new_hash])
    con.commit()

# try to acquire the file for processing, returns True if acquired, False if should skip
def try_acquire(file_key, run_id, current_hash):
    attempts = get_attempts(con, file_key)
    if attempts >= MAX_ATTEMPTS:
        log.warning("SKIP  max attempts reached (%d)  path=%s", MAX_ATTEMPTS, file_key)
        return False

    if get_stored_hash(con, file_key) == current_hash and is_processed(file_key):
        log.debug("SKIP  unchanged successful file  path=%s", file_key)
        return False

    save_hash(con, file_key, current_hash)
    mark_processing(file_key, run_id)
    log.info("ACQUIRED  path=%s  attempt=%d", file_key, attempts + 1)
    return True


# Read raw 
def read_raw(path: Path) -> bytes:
    with open(path, "rb") as fh:
        return fh.read()


# ─ Write raw to bronze 
def write_bronze(src: Path, date_str: str, hour: str) -> bool:
    dest = BRONZE_ROOT / date_str / hour / src.name
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        import shutil
        shutil.copy2(src, dest)
        log.info("BRONZE  ok  file=%s  size=%d bytes", src.name, dest.stat().st_size)
        return True
    except Exception as exc:
        log.error("BRONZE  FAIL  file=%s  reason=%s", src.name, exc)
        return False


# get hour directories
def get_hour_dirs(date_str):
    base = STREAM_DIR / date_str
    if not base.exists():
        return []
    return sorted(p for p in base.iterdir() if p.is_dir() and p.name.isdigit())


def process_file(filepath, run_id):
    file_key = str(filepath.resolve())

    if filepath.name not in KNOWN_FILES:
        log.warning("SKIP  unknown file  path=%s", filepath)
        return filepath.stem, None

    if filepath.stat().st_size == 0:
        log.warning("SKIP  empty file  path=%s", filepath)
        return filepath.stem, None

    current_hash = hash_file(filepath)

    with db_lock:
        acquired = try_acquire(file_key, run_id, current_hash)

    if not acquired:
        return filepath.stem, None

    log.info("READ  starting  path=%s", filepath)
    try:
        data = read_raw(filepath)
    except Exception as exc:
        log.error("FAIL  read error  path=%s  reason=%s", filepath, exc)
        mark_processed(file_key, "FAILED", 0, run_id)
        return filepath.stem, None

    log.info("READ  ok  path=%s  size=%d bytes", filepath, len(data))
    update_stage(file_key, "WRITING", run_id)
    return filepath.stem, (filepath, len(data))


def process_hour(hour_dir, run_id):
    futures = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for filename in KNOWN_FILES:
            filepath = hour_dir / filename
            if not filepath.exists():
                continue
            log.info("DETECTED  queuing  path=%s", filepath)
            futures[executor.submit(process_file, filepath, run_id)] = str(filepath.resolve())

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


def write_batch(date_str, hour, batch, run_id):
    for table_name, ((filepath, byte_count), file_key) in batch.items():
        log.info("WRITE  hour=%s  file=%s  bytes=%d", hour, filepath.name, byte_count)
        ok     = write_bronze(filepath, date_str, hour)
        status = "SUCCESS" if ok else "FAILED"
        if not ok:
            log.error("WRITE  FAIL  hour=%s  file=%s  — marking FAILED", hour, filepath.name)
        mark_processed(file_key, status, 0, run_id)


def run_cycle(date_str, run_id):
    dirs = get_hour_dirs(date_str)
    if not dirs:
        log.info("CYCLE  no hour directories found  date=%s", date_str)
        return

    for hour_dir in dirs:
        hour  = hour_dir.name
        batch = process_hour(hour_dir, run_id)
        if batch:
            log.info("BATCH  hour=%s  tables=%d  (%s)", hour, len(batch), ", ".join(batch))
            write_batch(date_str, hour, batch, run_id)
        else:
            log.info("BATCH  hour=%s  nothing new", hour)


def run_pipeline(date_str=None, once=False):
    setup_db()
    date_str = date_str or date.today().isoformat()

    log.info("=" * 60)
    log.info("PIPELINE  starting  date=%s  once=%s", date_str, once)
    log.info("PIPELINE  stream_dir=%s", STREAM_DIR)
    log.info("PIPELINE  db_path=%s",    DB_PATH)
    log.info("=" * 60)

    cycle = 0
    try:
        while True:
            cycle  += 1
            run_id  = generate_run_id()
            log.info("CYCLE  #%d  run_id=%s", cycle, run_id)

            run_cycle(date_str, run_id)

            if once:
                break

            wake_at = datetime.fromtimestamp(time.time() + SLEEP_HOURS * 3600)
            log.info("SLEEP  %dh  wake~%s", SLEEP_HOURS, wake_at.strftime("%H:%M:%S"))
            time.sleep(SLEEP_HOURS * 30)

    except KeyboardInterrupt:
        log.info("PIPELINE  interrupted")

    log.info("PIPELINE  finished  cycles=%d", cycle)


def main():
    parser = argparse.ArgumentParser(description="FastFeast micro-batch stream monitor")
    parser.add_argument("--date", default=date.today().isoformat(),
                        help="Date to monitor (YYYY-MM-DD, default: today)")
    parser.add_argument("--once", action="store_true",
                        help="Single pass then exit")
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print("ERROR  invalid date: {}  (expected YYYY-MM-DD)".format(args.date))
        sys.exit(1)

    run_pipeline(date_str=args.date, once=args.once)


if __name__ == "__main__":
    main()