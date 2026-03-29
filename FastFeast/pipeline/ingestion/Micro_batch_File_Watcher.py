import os
import time
import logging
import argparse
import duckdb
import pyarrow as pa
from datetime import datetime, date
from pathlib import Path

import config
import bronze_writer

# ---------------- Config ----------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CFG_PATH = os.path.join(BASE_DIR, "config.yaml")
cfg      = config.load(CFG_PATH)

STREAM_DIR = os.path.normpath(os.path.join(BASE_DIR, cfg.paths.stream_dir))
DB_PATH    = os.path.normpath(os.path.join(BASE_DIR, cfg.database.file))
POLL_SEC   = cfg.stream.poll_interval_sec

# ---------------- Logging ----------------

logging.basicConfig(
    level=getattr(logging, cfg.logging.level, logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("monitor")

# ---------------- DB ----------------

con = duckdb.connect(DB_PATH)
log.info("INIT DB %s", DB_PATH)

# ---------------- Readers ----------------

def read_j(p: Path) -> pa.Table:
    return con.execute(f"SELECT * FROM read_json_auto('{p}')").to_arrow_table()

def read_c(p: Path) -> pa.Table:
    return con.execute(f"SELECT * FROM read_csv_auto('{p}')").to_arrow_table()

READERS = {
    "orders.json":        read_j,
    "tickets.csv":        read_c,
    "ticket_events.json": read_j,
}

# ---------------- Hours ----------------

def get_hours(d: str) -> list[Path]:
    base = Path(STREAM_DIR) / d
    if not base.exists():
        return []
    return sorted(p for p in base.iterdir() if p.is_dir() and p.name.isdigit())

# ---------------- Read File ----------------

def read_file(fp: Path, seen: set) -> pa.Table | None:
    key = str(fp)

    if fp.stat().st_size == 0:
        log.warning("SKIP empty %s", fp)
        seen.add(key)
        return None

    try:
        tbl = READERS[fp.name](fp)
    except Exception as e:
        log.warning("SKIP bad %s (%s)", fp, e)
        return None

    seen.add(key)
    log.info("OK %s rows=%d cols=%d", fp, tbl.num_rows, tbl.num_columns)
    return tbl

# ---------------- Scan Hour ----------------

def scan_hour(hdir: Path, seen: set) -> dict[str, pa.Table]:
    out = {}

    for fname in READERS:
        fp = hdir / fname
        key = str(fp)

        if key in seen:
            continue

        if not fp.exists():
            continue

        log.info("FOUND %s", fp)

        tbl = read_file(fp, seen)
        if tbl:
            out[fname.split(".")[0]] = tbl

    return out

# ---------------- Handle ----------------

def handle(hour: str, batch: dict[str, pa.Table]):
    for name, tbl in batch.items():
        log.info("WRITE hour=%s table=%s rows=%d", hour, name, tbl.num_rows)
        bronze_writer.write(con, name, tbl, hour)

# ---------------- Main Loop ----------------

def start(d: str, once: bool = False):
    log.info("=" * 50)
    log.info("START date=%s poll=%ss", d, POLL_SEC)
    log.info("DIR %s", STREAM_DIR)
    log.info("=" * 50)

    seen_files = set()
    seen_hours = set()
    cycle = 0

    try:
        while True:
            cycle += 1

            dirs = get_hours(d)

            if not dirs:
                log.info("WAIT no dirs yet")
            else:
                for hdir in dirs:
                    h = hdir.name

                    if h not in seen_hours:
                        seen_hours.add(h)
                        log.info("NEW HOUR %s", h)

                    batch = scan_hour(hdir, seen_files)

                    if batch:
                        log.info("BATCH %s tables=%s", h, list(batch.keys()))
                        handle(h, batch)

            if once:
                break

            time.sleep(POLL_SEC)

    except KeyboardInterrupt:
        log.info("STOP interrupt")

    log.info("=" * 50)
    log.info("END cycles=%d files=%d hours=%d",
             cycle, len(seen_files), len(seen_hours))
    log.info("=" * 50)

# ---------------- Entry ----------------

def main():
    parser = argparse.ArgumentParser(description="Stream monitor")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--once", action="store_true")

    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print("Invalid date format (YYYY-MM-DD)")
        return

    start(args.date, once=args.once)

if __name__ == "__main__":
    main()