
import os
import argparse
import duckdb

from pipeline.logger     import pipeline as log
from pipeline.validation import validate_table

STREAM_DIR = "data/input/stream"
DB_PATH    = "fastfeast.duckdb"


def _ensure_schema(conn: duckdb.DuckDBPyConnection):
    conn.execute("CREATE SCHEMA IF NOT EXISTS stream")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stream.orders (
            order_id         VARCHAR,
            customer_id      INTEGER,
            restaurant_id    INTEGER,
            driver_id        INTEGER,
            region_id        INTEGER,
            order_amount     DOUBLE,
            delivery_fee     DOUBLE,
            discount_amount  DOUBLE,
            total_amount     DOUBLE,
            order_status     VARCHAR,
            payment_method   VARCHAR,
            order_created_at TIMESTAMP,
            delivered_at     TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stream.tickets (
            ticket_id          VARCHAR,
            order_id           VARCHAR,
            customer_id        INTEGER,
            driver_id          INTEGER,
            restaurant_id      INTEGER,
            agent_id           INTEGER,
            reason_id          INTEGER,
            priority_id        INTEGER,
            channel_id         INTEGER,
            status             VARCHAR,
            refund_amount      DOUBLE,
            created_at         TIMESTAMP,
            first_response_at  TIMESTAMP,
            resolved_at        TIMESTAMP,
            sla_first_due_at   TIMESTAMP,
            sla_resolve_due_at TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stream.ticket_events (
            event_id   VARCHAR,
            ticket_id  VARCHAR,
            agent_id   INTEGER,
            event_ts   TIMESTAMP,
            old_status VARCHAR,
            new_status VARCHAR,
            notes      VARCHAR
        )
    """)


def _load(conn, filepath: str,table: str, fmt: str):
    tmp = f"_tmp_{table}"
    conn.execute(f"DROP TABLE IF EXISTS {tmp}")

    if fmt == "json":
        conn.execute(f"""
            CREATE TEMP TABLE {tmp} AS
            SELECT * FROM read_json_auto(
                '{filepath}',
                ignore_errors   = true,
                timestampformat = '%Y-%m-%d %H:%M:%S'
            )
        """)
    else:
        conn.execute(f"""
            CREATE TEMP TABLE {tmp} AS
            SELECT * FROM read_csv_auto(
                '{filepath}', header=true, ignore_errors=true
            )
        """)

    log.info("loaded", table=table, source=filepath)

    # validate 
    validate_table(conn, tmp, table)

    # build safe INSERT
    target_cols = [
        r[0] for r in conn.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'stream' AND table_name = '{table}'
            ORDER BY ordinal_position
        """).fetchall()
    ]
    tmp_cols = {
        r[0] for r in conn.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = '{tmp}'
        """).fetchall()
    }
    use_cols = [c for c in target_cols if c in tmp_cols]

    tmp_types = {
        r[0]: r[1].upper() for r in conn.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns WHERE table_name = '{tmp}'
        """).fetchall()
    }
    tgt_types = {
        r[0]: r[1].upper() for r in conn.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='stream' AND table_name='{table}'
        """).fetchall()
    }
    exprs = []
    for c in use_cols:
        s, t = tmp_types.get(c, ""), tgt_types.get(c, "")
        if "INT" in t and "INT" not in s:
            exprs.append(
                f"TRY_CAST(NULLIF(CAST({c} AS VARCHAR),'NaN') AS INTEGER) AS {c}"
            )
        elif "TIMESTAMP" in t and "TIMESTAMP" not in s:
            exprs.append(f"TRY_CAST(CAST({c} AS VARCHAR) AS TIMESTAMP) AS {c}")
        else:
            exprs.append(c)

    col_list  = ", ".join(use_cols)
    expr_list = ", ".join(exprs)
    
    conn.execute(f"""
        INSERT INTO stream.{table} ({col_list})
        SELECT {expr_list} FROM {tmp}
    """)
    conn.execute(f"DROP TABLE IF EXISTS {tmp}")


def run(date: str, hour: int):
    stream_dir = os.path.join(STREAM_DIR, date, f"{hour:02d}")
    if not os.path.isdir(stream_dir):
        return

    conn = duckdb.connect(DB_PATH)
    _ensure_schema(conn)

    files = [
        (os.path.join(stream_dir, "orders.json"),        "orders",        "json"),
        (os.path.join(stream_dir, "tickets.csv"),         "tickets",       "csv"),
        (os.path.join(stream_dir, "ticket_events.json"),  "ticket_events", "json"),
    ]
    for filepath, table, fmt in files:
        if os.path.exists(filepath):
            _load(conn, filepath, table, fmt)

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", type=int, required=True)
    args = parser.parse_args()
    run(args.date, args.hour)
