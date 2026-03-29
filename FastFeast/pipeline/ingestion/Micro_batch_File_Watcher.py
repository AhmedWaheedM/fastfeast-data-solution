import os
import time
import logging
import argparse
import duckdb
import pyarrow as pa
from datetime import datetime, date
from pathlib import Path
import sys

# ── Path setup ────────────────────────────────────────────────────────────────
SCRIPT_DIR   = Path(os.path.dirname(os.path.abspath(__file__)))   # pipeline/ingestion/
PIPELINE_DIR = SCRIPT_DIR.parent                                   # pipeline/
CONFIG_DIR   = PIPELINE_DIR / "config"                             # pipeline/config/

# Add pipeline/config to sys.path so `import config` resolves correctly
sys.path.insert(0, str(CONFIG_DIR))
# Add pipeline/ingestion to sys.path so `import bronze_writer` resolves correctly
sys.path.insert(0, str(SCRIPT_DIR))

import config
import bronze_writer

# ── Config ────────────────────────────────────────────────────────────────────
CONFIG_PATH = CONFIG_DIR / "config.yaml"
cfg         = config.load(str(CONFIG_PATH))

STREAM_DIR = Path(os.path.normpath(PIPELINE_DIR / cfg.paths.stream_dir))
DB_PATH    = os.path.normpath(PIPELINE_DIR / cfg.database.file)
POLL_SEC   = cfg.stream.poll_interval_sec

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, cfg.logging.level, logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("stream_monitor")

# ── DuckDB Connection ─────────────────────────────────────────────────────────
con = duckdb.connect(DB_PATH)
log.info("INIT  DuckDB connected  path=%s", DB_PATH)

# ── File readers ──────────────────────────────────────────────────────────────
def read_json(path: Path) -> pa.Table:
    log.debug("READ  opening JSON  path=%s", path)
    return con.execute(f"SELECT * FROM read_json_auto('{path}')").to_arrow_table()

def read_csv(path: Path) -> pa.Table:
    log.debug("READ  opening CSV   path=%s", path)
    return con.execute(f"SELECT * FROM read_csv_auto('{path}')").to_arrow_table()

FILE_READERS = {
    "orders.json":        read_json,
    "tickets.csv":        read_csv,
    "ticket_events.json": read_json,
}

# ── Hour-directory helpers ────────────────────────────────────────────────────
def hour_dirs(date_str: str) -> list[Path]:
    """Return all HH/ subdirectories for date_str, sorted ascending."""
    base = STREAM_DIR / date_str
    if not base.exists():
        log.debug("POLL  base directory not found yet  path=%s", base)
        return []
    dirs = sorted(
        p for p in base.iterdir()
        if p.is_dir() and p.name.isdigit()
    )
    log.debug("POLL  found %d hour director%s under %s",
              len(dirs), "y" if len(dirs) == 1 else "ies", base)
    return dirs

# ── Single-file read ──────────────────────────────────────────────────────────
def try_read(filepath: Path, seen_files: set) -> pa.Table | None:
    key  = str(filepath)
    size = filepath.stat().st_size

    if size == 0:
        log.warning("SKIP  empty file — nothing to read  path=%s", filepath)
        seen_files.add(key)
        return None

    log.debug("READ  attempting  path=%s  size=%d bytes", filepath, size)

    try:
        table = FILE_READERS[filepath.name](filepath)
    except Exception as exc:
        log.warning(
            "SKIP  unreadable file — pipeline continues  path=%s  reason=%s",
            filepath, exc,
        )
        return None

    seen_files.add(key)
    log.info(
        "READ  ok  path=%s  rows=%d  cols=%d  schema=%s",
        filepath, table.num_rows, table.num_columns,
        ", ".join(f"{f.name}:{f.type}" for f in table.schema),
    )
    return table

# ── Per-hour scan ─────────────────────────────────────────────────────────────
def process_hour_dir(hour_dir: Path, seen_files: set) -> dict[str, pa.Table]:
    results: dict[str, pa.Table] = {}

    for filename in FILE_READERS:
        filepath = hour_dir / filename
        key      = str(filepath)

        if key in seen_files:
            log.debug("SKIP  already processed  path=%s", filepath)
            continue

        if not filepath.exists():
            log.debug("WAIT  file not yet present  path=%s", filepath)
            continue

        log.info("DETECTED  new file  path=%s", filepath)

        table = try_read(filepath, seen_files)
        if table is not None:
            results[filename.split(".")[0]] = table

    return results

# ── Downstream dispatch ───────────────────────────────────────────────────────
def on_new_records(hour: str, batch: dict[str, pa.Table]):
    for table_name, arrow_table in batch.items():
        log.info(
            "DOWNSTREAM  dispatching  hour=%s  table=%-16s  rows=%d  cols=%d",
            hour, table_name, arrow_table.num_rows, arrow_table.num_columns,
        )
        bronze_writer.write(con, table_name, arrow_table, hour)

# ── Main polling loop ─────────────────────────────────────────────────────────
def run(date_str: str, once: bool = False):
    log.info("=" * 60)
    log.info("INIT  stream monitor starting")
    log.info("INIT  date=%s  poll_interval=%ss", date_str, POLL_SEC)
    log.info("INIT  stream_dir=%s", STREAM_DIR)
    log.info("INIT  watching for files: %s", ", ".join(FILE_READERS))
    log.info("=" * 60)

    seen_files: set[str] = set()
    seen_hours: set[str] = set()
    cycle = 0

    try:
        while True:
            cycle += 1
            log.debug("POLL  cycle=%d  date=%s", cycle, date_str)

            dirs = hour_dirs(date_str)

            if not dirs:
                log.info("POLL  cycle=%d  no hour directories present yet — waiting", cycle)
            else:
                for hour_dir in dirs:
                    hour = hour_dir.name

                    if hour not in seen_hours:
                        seen_hours.add(hour)
                        log.info("HOUR  new directory detected  hour=%s  path=%s", hour, hour_dir)

                    batch = process_hour_dir(hour_dir, seen_files)

                    if batch:
                        log.info(
                            "BATCH  hour=%s  new tables=%d  (%s)",
                            hour, len(batch), ", ".join(batch),
                        )
                        on_new_records(hour, batch)
                    else:
                        log.debug("POLL  hour=%s  no new files this cycle", hour)

            if once:
                log.info("POLL  --once flag set — exiting after single pass")
                break

            log.debug("POLL  sleeping %ss before next cycle", POLL_SEC)
            time.sleep(POLL_SEC)

    except KeyboardInterrupt:
        log.info("STOP  keyboard interrupt received — shutting down cleanly")

    log.info("=" * 60)
    log.info("STOP  stream monitor finished  cycles_run=%d  files_seen=%d  hours_seen=%d",
             cycle, len(seen_files), len(seen_hours))
    log.info("=" * 60)

# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="FastFeast stream directory monitor")
    parser.add_argument(
        "--date", default=date.today().isoformat(),
        help="Date to monitor (YYYY-MM-DD, default: today)",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Single pass then exit",
    )
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"Invalid date: {args.date}  (use YYYY-MM-DD)")
        return

    run(args.date, once=args.once)


if __name__ == "__main__":
    main()