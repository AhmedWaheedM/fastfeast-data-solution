import duckdb
from pipeline.logger import validation as log

# ── required columns ────────────────────────────
REQUIRED_COLS: dict[str, list[str]] = {
    # batch
    "customers":         ["customer_id", "region_id"],
    "drivers":           ["driver_id",   "region_id"],
    "agents":            ["agent_id"],
    "restaurants":       ["restaurant_id", "region_id"],
    "regions":           ["region_id", "city_id"],
    "cities":            ["city_id"],
    "segments":          ["segment_id"],
    "categories":        ["category_id"],
    "teams":             ["team_id"],
    "channels":          ["channel_id"],
    "priorities":        ["priority_id"],
    "reasons":           ["reason_id"],
    "reason_categories": ["reason_category_id"],
    # stream
    "orders":        ["order_id", "customer_id", "restaurant_id",
                      "driver_id", "order_amount", "order_status",
                      "order_created_at"],
    "tickets":       ["ticket_id", "order_id", "customer_id",
                      "agent_id", "reason_id", "priority_id",
                      "channel_id", "status", "created_at"],
    "ticket_events": ["event_id", "ticket_id", "agent_id",
                      "event_ts", "new_status"],
}

PRIMARY_KEYS: dict[str, str] = {
    "customers":         "customer_id",
    "drivers":           "driver_id",
    "agents":            "agent_id",
    "restaurants":       "restaurant_id",
    "regions":           "region_id",
    "cities":            "city_id",
    "segments":          "segment_id",
    "categories":        "category_id",
    "teams":             "team_id",
    "channels":          "channel_id",
    "priorities":        "priority_id",
    "reasons":           "reason_id",
    "reason_categories": "reason_category_id",
    "orders":            "order_id",
    "tickets":           "ticket_id",
    "ticket_events":     "event_id",
}


# ─────────────────────────────────────────────────────────
# 1. DUPLICATE CHECK
# ─────────────────────────────────────────────────────────
def duplicate_check(
    conn: duckdb.DuckDBPyConnection,
    table_ref: str,
    pk: str,
    label: str,
) -> bool:
   
    rows = conn.execute(f"""
        SELECT CAST({pk} AS VARCHAR) AS pk_val, COUNT(*) AS cnt
        FROM {table_ref}
        WHERE {pk} IS NOT NULL
        GROUP BY {pk}
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC
    """).fetchall()

    if not rows:
        return True   

    extra_rows = sum(int(r[1]) - 1 for r in rows)
    sample_ids = [r[0] for r in rows[:5]]

    log.warning(
        "DUPLICATE",
        table      = label,
        pk         = pk,
        dup_keys   = len(rows),
        extra_rows = extra_rows,
        sample_ids = sample_ids,
    )
    return False


# ─────────────────────────────────────────────────────────
# 2. NULL CHECK
# ─────────────────────────────────────────────────────────
def null_check(conn,table_ref: str,cols: list[str], label: str,) -> bool:

    if not cols:
        return True

    actual = {
        r[0] for r in conn.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = '{table_ref.split(".")[-1]}'
        """).fetchall()
    }
    check_cols = [c for c in cols if c in actual]
    if not check_cols:
        return True

    exprs = ", ".join(
        f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS \"{c}\""
        for c in check_cols
    )
    row = conn.execute(f"SELECT {exprs} FROM {table_ref}").fetchone()

    null_per_col = {
        col: int(cnt)
        for col, cnt in zip(check_cols, row)
        if cnt and int(cnt) > 0
    }

    if not null_per_col:
        return True   

    any_null = " OR ".join(f"{c} IS NULL" for c in null_per_col)
    total_rows = conn.execute(
        f"SELECT COUNT(*) FROM {table_ref} WHERE {any_null}"
    ).fetchone()[0]

    log.warning(
        "NULL",
        table        = label,
        null_per_col = null_per_col,
        total_rows   = int(total_rows),
    )
    return False


# ─────────────────────────────────────────────────────────
# 3. VALIDATE TABLE
# ─────────────────────────────────────────────────────────
def validate_table(conn,table_ref: str,table_name: str,):
    pk       = PRIMARY_KEYS.get(table_name)
    req_cols = REQUIRED_COLS.get(table_name, [])

    dup_ok  = duplicate_check(conn, table_ref, pk, table_name)  if pk       else True
    null_ok = null_check(conn, table_ref, req_cols, table_name) if req_cols else True

    if dup_ok and null_ok:
        log.info("OK", table=table_name)
