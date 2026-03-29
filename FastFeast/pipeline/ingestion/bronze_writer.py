import logging
import pyarrow as pa
import pyarrow.compute as pc
from datetime import datetime, timezone

log = logging.getLogger("bronze_writer")


REQUIRED_COLS: dict[str, list[str]] = {
    "orders": [
        "order_id",
        "customer_id",
        "driver_id",
        "restaurant_id",
        "order_created_at",
        "total_amount",
        "order_status",
    ],
    "tickets": [
        "ticket_id",
        "order_id",
        "customer_id",
        "created_at",
        "status",
    ],
    "ticket_events": [
        "event_id",
        "ticket_id",
        "event_ts",
        "new_status",
    ],
}


def bronze_table_name(table_name):

    return f"bronze_{table_name}"


def ensure_table(con, table_name) :
    bt = bronze_table_name(table_name)

    try:
        con.execute(f"SELECT 1 FROM {bt} LIMIT 0")
        return True
    
    except Exception:
        return False


def validate(table_name, arrow_table):

    failures: list[str] = []

    if arrow_table.num_rows == 0:
        failures.append("table is empty")
        return failures

    actual_cols = set(arrow_table.schema.names)
    required    = set(REQUIRED_COLS.get(table_name, []))
    missing     = required - actual_cols

    if missing:
        failures.append(f"missing required columns: {sorted(missing)}")

    return failures


def add_metadata_cols(arrow_table,hour,ingested_at):

    n = arrow_table.num_rows

    arrow_table = arrow_table.append_column(
        "_ingested_at",
        pa.array([ingested_at.isoformat()] * n, type=pa.string()),
    )

    arrow_table = arrow_table.append_column(
        "_hour",
        pa.array([hour] * n, type=pa.string()),
    )

    arrow_table = arrow_table.append_column(
        "_source",
        pa.array(["stream"] * n, type=pa.string()),
    )

    return arrow_table


def clean_numeric_columns(arrow_table) :
    new_cols = {}

    for i, field in enumerate(arrow_table.schema):
        col = arrow_table.column(i)

        if pa.types.is_floating(field.type):
            try:
                nan_mask = pc.is_nan(col)
                col = pc.if_else(nan_mask, None, col)
            except Exception:
                pass

        elif pa.types.is_integer(field.type):
            try:
                float_col = col.cast(pa.float64())
                nan_mask = pc.is_nan(float_col)
                col = pc.if_else(nan_mask, None, float_col).cast(field.type)
            except Exception:
                pass

        new_cols[field.name] = col

    return pa.table(new_cols, schema=arrow_table.schema)


def write( con, table_name, arrow_table, hour) :

    ingested_at = datetime.now(timezone.utc)

    log.info(
        "WRITE  start  table=%-16s  hour=%s  rows=%d",
        table_name, hour, arrow_table.num_rows,
    )

    # Validate
    failures = validate(table_name, arrow_table)
    if failures:
        for reason in failures:
            log.warning(
                "VALID  FAIL  table=%-16s  hour=%s  reason=%s",
                table_name, hour, reason,
            )
        return False

    log.debug("VALID  ok  table=%s", table_name)

    # Replace NaN with null in numeric columns
    arrow_table = clean_numeric_columns(arrow_table)

    # Append ingestion metadata
    arrow_table = add_metadata_cols(arrow_table, hour, ingested_at)

    # Write to bronze
    bt = bronze_table_name(table_name)
    con.register("_incoming", arrow_table)
    try:
        if ensure_table(con, table_name):
 
            target_schema = con.execute(f"DESCRIBE {bt}").fetchdf()

            casts = ", ".join(
                f"TRY_CAST(\"{row['column_name']}\" AS {row['column_type']}) AS \"{row['column_name']}\""
                for _, row in target_schema.iterrows()
            )

            con.execute(f"INSERT INTO {bt} SELECT {casts} FROM _incoming")
            log.info("WRITE  inserted  table=%-16s  hour=%s  rows=%d", table_name, hour, arrow_table.num_rows)

        else:

            con.execute(f"CREATE TABLE {bt} AS SELECT * FROM _incoming")
            log.info("WRITE  created   table=%-16s  hour=%s  rows=%d", table_name, hour, arrow_table.num_rows)

    except Exception as exc:
        log.error("WRITE  ERROR  table=%-16s  hour=%s  reason=%s", table_name, hour, exc)
        return False
    
    finally:
        con.unregister("_incoming")

    return True