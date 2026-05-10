import os
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from FastFeast.pipeline.ingestion.file_processing import build_validated_table
from FastFeast.pipeline.loading.loader import load_to_silver
from FastFeast.observability.logger import setup_logger, get_logger

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

# Config
_HERE       = Path(__file__).resolve().parent
_PIPELINE   = _HERE.parent
_ROOT       = _PIPELINE.parent
CONFIG_PATH = _PIPELINE / "config" / "config.yaml"
SLA_VIEW_SQL = _ROOT / "dwh" / "gold" / "view_sla_metrics.sql"
REVENUE_VIEW_SQL = _ROOT / "dwh" / "analytics" / "view_revenue_impact.sql"

cfg = get_config()
STREAM_DIR   = Path(cfg.paths.stream_dir).resolve()
BRONZE_ROOT  = _ROOT / "data"

MAX_WORKERS  = cfg.pipeline.max_workers
SLEEP_HOURS  = cfg.pipeline.sleep_hours
MAX_ATTEMPTS = cfg.pipeline.max_attempts

setup_logger(name="stream_monitor", log_dir="logs", level=cfg.logging.level)
log = get_logger("stream_monitor")
yaml_path = Path(__file__).parent.parent.parent / cfg.paths.metadata_yaml

#print("GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG", yaml_path)

try:
    from FastFeast.pipeline.config.metadata import load
    meta = load(yaml_path)
    KNOWN_FILES = {f.file_name for f in meta.stream}
    STREAM_META_BY_FILE = {f.file_name: f for f in meta.stream}
except Exception as _e:
    log.warning("Could not load KNOWN_FILES from metadata, using defaults: %s", _e)
    KNOWN_FILES = {"orders.json", "tickets.csv", "ticket_events.json"}
    STREAM_META_BY_FILE = {}


# File processing

def _fail_file(file_key: str, cycle_id: str, filepath: Path, message: str):
    log.error(message, filepath)
    update_stage(file_key, "FAILED", cycle_id)
    return filepath.stem, None

def process_file(filepath: Path, cycle_id: str, pipeline_type="stream"):
    file_key = str(filepath.resolve())

    if filepath.name not in KNOWN_FILES:
        log.warning("SKIP  unknown file  path=%s", filepath)
        return filepath.stem, None

    if filepath.stat().st_size == 0:
        log.warning("SKIP  empty file  path=%s", filepath)
        return filepath.stem, None

    current_hash = hash_file(filepath)

    # Stage 1: PENDING - file detected, waiting to be processed
    update_stage(file_key, "PENDING", cycle_id)
    log.info("PENDING  file detected  path=%s", filepath)

    acquired = try_acquire(file_key, cycle_id, current_hash, MAX_ATTEMPTS)
    if not acquired:
        return filepath.stem, None

    byte_count = filepath.stat().st_size
    log.info("READ  ok  path=%s  size=%d bytes", filepath, byte_count)

    # Stage 2: PROCESSING - ingestion has started
    update_stage(file_key, "PROCESSING", cycle_id)
    log.info("PROCESSING  ingestion started  path=%s", filepath)

    source_table, full_table, status_list, error_code = build_validated_table(filepath, pipeline_type)
    if error_code == "unsupported_file_type":
        return _fail_file(file_key, cycle_id, filepath, "LOAD  unsupported file type  path=%s")
    if error_code == "column_mismatch":
        return _fail_file(file_key, cycle_id, filepath, "SCHEMA  column mismatch  path=%s")

    if source_table is None or full_table is None:
        return _fail_file(file_key, cycle_id, filepath, "VALIDATION  failed to build validation output  path=%s")

    valid_count = sum(1 for status in status_list if str(status).upper() == "VALID")

    log.info(
        "VALIDATED  path=%s  rows=%d  valid=%d",
        filepath,
        len(status_list),
        valid_count,
    )

    # full_table is now ready for write_bronze or further FK/PK checks
    return filepath.stem, (filepath, byte_count, source_table)











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
    loaded_any_silver = False

    for table_name, ((filepath, byte_count, source_table), file_key) in batch.items():
        log.info("WRITE  file=%s  bytes=%d", filepath.name, byte_count)

        # Ingestion
        ok = write_bronze(filepath, date_str)
        
        if not ok:
            log.error("WRITE  FAIL  file=%s", filepath.name)
            continue

        file_meta = STREAM_META_BY_FILE.get(filepath.name)
        silver_result = load_to_silver(filepath.name, source_table, file_meta, is_batch=False)

        if silver_result.success:
            loaded_any_silver = True
            log.info(
                "SILVER  ok  file=%s  inserted=%d  skipped=%d",
                filepath.name,
                silver_result.inserted,
                silver_result.skipped,
            )
        else:
            log.error("SILVER  FAIL  file=%s  reason=%s", filepath.name, silver_result.error)

    if loaded_any_silver:
        _upsert_fact_ticket_lifecycle(date_str)
        _refresh_analytics_views()


def _upsert_fact_ticket_lifecycle(batch_id: str):
    conn = get_connection()
    try:
        required_tables = (
            "SILVER_TICKETS",
            "SILVER_ORDERS",
            "SILVER_TICKET_EVENTS",
            "FACT_TICKET_LIFECYCLE",
        )
        for table_name in required_tables:
            exists = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()[0]
            if not exists:
                log.warning("GOLD  skipped fact upsert  missing=%s", table_name)
                return

        insert_sql = """
        WITH max_sk AS (
            SELECT COALESCE(MAX(ticket_sk), 0) AS base_sk
            FROM FACT_TICKET_LIFECYCLE
        ),
        event_rollup AS (
            SELECT
                ticket_id,
                COUNT(*) AS total_event_count,
                SUM(
                    CASE
                        WHEN LOWER(COALESCE(new_status, '')) = 'reopened'
                             OR LOWER(COALESCE(old_status, '')) = 'reopened'
                        THEN 1 ELSE 0
                    END
                ) AS reopen_count
            FROM SILVER_TICKET_EVENTS
            GROUP BY ticket_id
        ),
        ticket_source AS (
            SELECT
                t.ticket_id,
                t.order_id,
                t.status AS ticket_status,
                o.order_status,
                o.order_amount,
                o.discount_amount,
                o.delivery_fee,
                o.total_amount,
                t.refund_amount,
                COALESCE(er.total_event_count, 0) AS total_event_count,
                COALESCE(er.reopen_count, 0) AS reopen_count
            FROM SILVER_TICKETS t
            LEFT JOIN SILVER_ORDERS o ON o.order_id = t.order_id
            LEFT JOIN event_rollup er ON er.ticket_id = t.ticket_id
        )
        INSERT INTO FACT_TICKET_LIFECYCLE (
            TICKET_SK,
            TICKET_ID,
            ORDER_ID,
            TICKET_STATUS,
            ORDER_STATUS,
            ORDER_SUBTOTAL,
            ORDER_DISCOUNT_AMOUNT,
            ORDER_DELIVERY_FEE,
            ORDER_TOTAL_AMOUNT,
            REFUND_AMOUNT,
            COMPENSATION_AMOUNT,
            CREDIT_AMOUNT,
            TOTAL_REVENUE_IMPACT,
            REFUND_IMPACT_PCT,
            TOTAL_EVENT_COUNT,
            REOPEN_COUNT,
            IS_REOPENED,
            IS_ESCALATED,
            BATCH_ID
        )
        SELECT
            ms.base_sk + ROW_NUMBER() OVER (ORDER BY ts.ticket_id) AS ticket_sk,
            ts.ticket_id,
            ts.order_id,
            ts.ticket_status,
            ts.order_status,
            CAST(ts.order_amount AS DECIMAL(18,2)) AS order_subtotal,
            CAST(ts.discount_amount AS DECIMAL(18,2)) AS order_discount_amount,
            CAST(ts.delivery_fee AS DECIMAL(18,2)) AS order_delivery_fee,
            CAST(ts.total_amount AS DECIMAL(18,2)) AS order_total_amount,
            CAST(ts.refund_amount AS DECIMAL(18,2)) AS refund_amount,
            CAST(0 AS DECIMAL(18,2)) AS compensation_amount,
            CAST(0 AS DECIMAL(18,2)) AS credit_amount,
            CAST(COALESCE(ts.refund_amount, 0) AS DECIMAL(18,2)) AS total_revenue_impact,
            CASE
                WHEN COALESCE(ts.total_amount, 0) = 0 THEN NULL
                ELSE CAST(ROUND((COALESCE(ts.refund_amount, 0) / ts.total_amount) * 100, 2) AS DECIMAL(5,2))
            END AS refund_impact_pct,
            ts.total_event_count,
            ts.reopen_count,
            ts.reopen_count > 0 AS is_reopened,
            FALSE AS is_escalated,
            ? AS batch_id
        FROM ticket_source ts
        CROSS JOIN max_sk ms
        WHERE ts.ticket_id IS NOT NULL
          AND NOT EXISTS (
                SELECT 1
                FROM FACT_TICKET_LIFECYCLE f
                WHERE f.ticket_id = ts.ticket_id
          )
        """
        conn.execute(insert_sql, [batch_id])

        fact_count = conn.execute("SELECT COUNT(*) FROM FACT_TICKET_LIFECYCLE").fetchone()[0]
        log.info("GOLD  fact upsert complete  table=FACT_TICKET_LIFECYCLE  rows=%d", fact_count)

    except Exception as exc:
        log.error("GOLD  fact upsert failed  reason=%s", exc)
    finally:
        conn.close()


def _refresh_analytics_views():
    conn = get_connection()
    try:
        for sql_path in (SLA_VIEW_SQL, REVENUE_VIEW_SQL):
            if not sql_path.exists():
                log.warning("VIEW  missing SQL file  path=%s", sql_path)
                continue

            try:
                conn.execute(sql_path.read_text(encoding="utf-8"))
                log.info("VIEW  refreshed  name=%s", sql_path.name)
            except Exception as exc:
                log.warning("VIEW  skipped  name=%s  reason=%s", sql_path.name, exc)
    finally:
        conn.close()


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


if __name__ == "__main__":
    main()