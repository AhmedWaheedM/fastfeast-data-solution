import re
import os
import pyarrow as pa
import time
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, Optional

from utilities.db_utils import get_connection
from utilities.file_utils import resolve_silver_name
from support.logger import pipeline as log

# #TODO Temporary Metrics Placeholder (Refactor after Merge)
@dataclass
class LoadResult:
    table_name: str
    layer: str
    attempted: int = 0
    inserted: int = 0
    skipped: int = 0
    duration_ms: float = 0.0
    success: bool = True
    error: str = ""

db_write_lock = threading.Lock()

def _build_dim_sk_map() -> Dict[str, Tuple[str, str, Optional[str]]]:

    _HERE    = Path(__file__).resolve().parent       
    _ROOT    = _HERE.parent.parent                      
    ddl_dir  = _ROOT / "dwh" / "gold"

    _col_re  = re.compile(r"^\s{2,}(\w+)\s+\w+", re.MULTILINE)
    _tbl_re  = re.compile(r"CREATE\s+TABLE\s+(\w+)", re.IGNORECASE)

    dim_info: Dict[str, Tuple[str, str]] = {}  
    for sql_file in ddl_dir.glob("dim_*.sql"):
        if sql_file.stem == "fact_ticket_lifecycle":
            continue
        src  = sql_file.read_text(encoding="utf-8", errors="ignore")
        tbl  = _tbl_re.search(src)
        cols = _col_re.findall(src)
        if tbl and len(cols) >= 2:
            table_name = tbl.group(1).upper()
            sk_col     = cols[0].upper()  
            nat_key    = cols[1].lower()  
            dim_info[table_name] = (nat_key, sk_col)
            log.debug("DIM_SK_MAP parsed", table=table_name, sk=sk_col, nat_key=nat_key)

    # ── Step 2: map each dimension to its silver table via metadata ───────────
    try:
        from pipeline.config.config import get_config
        from pipeline.config.metadata import load as load_meta
        _cfg      = get_config()
        _yaml     = _ROOT / _cfg.paths.metadata_yaml
        _meta     = load_meta(str(_yaml))
        # Build: target_dimension_upper → silver_table_name
        _dim_to_silver: Dict[str, str] = {}
        for file_meta in _meta.batch:
            if file_meta.target_dimension:
                silver = resolve_silver_name(file_meta.file_name)
                _dim_to_silver[file_meta.target_dimension.upper()] = silver
    except Exception as exc:
        log.warning("Could not load metadata for DIM_SK_MAP silver resolution", error=str(exc))
        _dim_to_silver = {}

    # ── Step 3: merge ─────────────────────────────────────────────────────────
    result: Dict[str, Tuple[str, str, Optional[str]]] = {}
    for table_name, (nat_key, sk_col) in dim_info.items():
        silver = _dim_to_silver.get(table_name)   # None for DIM_DATE, DIM_TIME etc.
        result[table_name] = (nat_key, sk_col, silver)

    log.info("DIM_SK_MAP built dynamically", dimensions=list(result.keys()))
    return result


# Built once at module load; dimensions that have no silver source get None
_DIM_SK_MAP: Dict[str, Tuple[str, str, Optional[str]]] = _build_dim_sk_map()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _build_pk_match_clause(pk_cols: list, alias1="s", alias2="temp_pa_view") -> str:
    """Handles both single and composite primary keys for SQL JOIN/NOT EXISTS."""
    if not pk_cols:
        return "1=1"
    return " AND ".join([f"{alias1}.{col} = {alias2}.{col}" for col in pk_cols])


def _dispatch_alert(target: str, error_msg: str):
    """Placeholder for the peer's async alerting logic."""
    log.error(f"ALERT: {target} load failed", error=error_msg)


def _dims_ready(conn, required_dim_tables: list) -> bool:
    """
    Returns True only when every required dimension table exists in Gold and
    has at least one row.  Derived from _DIM_SK_MAP keys — never hardcoded.
    Fact loads are deferred until this returns True.
    """
    for dim in required_dim_tables:
        try:
            count = conn.execute(
                f"SELECT COUNT(*) FROM information_schema.tables "
                f"WHERE UPPER(table_name) = '{dim.upper()}'"
            ).fetchone()[0]
            if count == 0:
                log.warning("Dimension table does not exist yet", dimension=dim)
                return False
            row_count = conn.execute(f"SELECT COUNT(*) FROM {dim}").fetchone()[0]
            if row_count == 0:
                log.warning("Dimension table is empty — fact load deferred", dimension=dim)
                return False
        except Exception as exc:
            log.warning("Could not verify dimension", dimension=dim, error=str(exc))
            return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Silver Persistence  (unchanged from original)
# ─────────────────────────────────────────────────────────────────────────────

def load_to_silver(file_name: str, pa_table: pa.Table, meta, is_batch: bool) -> LoadResult:
    silver_table = resolve_silver_name(file_name)
    result = LoadResult(table_name=silver_table, layer="Silver", attempted=pa_table.num_rows)

    if pa_table.num_rows == 0:
        return result

    conn = get_connection()
    t0   = time.perf_counter()

    try:
        conn.register("temp_pa_view", pa_table)

        select_cols = []
        for col_name in pa_table.column_names:
            is_pii = any(
                c.name.lower() == col_name.lower() and getattr(c, "pii", False)
                for c in getattr(meta, "columns", [])
            ) if meta else False
            if is_pii:
                select_cols.append(
                    f"MD5(CAST(temp_pa_view.{col_name} AS VARCHAR)) AS {col_name}"
                )
            else:
                select_cols.append(f"temp_pa_view.{col_name}")

        select_clause = ", ".join(select_cols)
        pk_cols       = meta.primary_keys if meta else []

        with db_write_lock:
            exists = conn.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{silver_table}'"
            ).fetchone()[0] > 0

            if is_batch:
                conn.execute(
                    f"CREATE OR REPLACE TABLE {silver_table} AS SELECT {select_clause} FROM temp_pa_view"
                )
                result.inserted = pa_table.num_rows
            else:
                if not exists:
                    conn.execute(
                        f"CREATE TABLE {silver_table} AS SELECT {select_clause} FROM temp_pa_view"
                    )
                    result.inserted = pa_table.num_rows
                elif pk_cols:
                    match_clause   = _build_pk_match_clause(pk_cols)
                    result.inserted = conn.execute(f"""
                        INSERT INTO {silver_table}
                        SELECT {select_clause} FROM temp_pa_view
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {silver_table} s WHERE {match_clause}
                        )
                    """).fetchone()[0]
                    result.skipped = result.attempted - result.inserted
                else:
                    conn.execute(
                        f"INSERT INTO {silver_table} SELECT {select_clause} FROM temp_pa_view"
                    )
                    result.inserted = pa_table.num_rows

    except Exception as e:
        result.success = False
        result.error   = str(e)
        _dispatch_alert(silver_table, result.error)
    finally:
        result.duration_ms = (time.perf_counter() - t0) * 1000
        log.info("Silver Load Complete", table=silver_table, inserted=result.inserted)
        try:
            conn.unregister("temp_pa_view")
        except Exception:
            pass
        conn.close()

    return result


def load_to_gold(file_name: str, meta) -> LoadResult:
    target_table = getattr(meta, "target_dimension", None) or getattr(meta, "target_fact", None)

    if not meta or not target_table:
        return LoadResult(table_name="N/A", layer="Gold", success=True)

    conn         = get_connection()
    silver_table = resolve_silver_name(file_name)
    pk_cols      = getattr(meta, "primary_keys", [])
    pk_str       = ", ".join(pk_cols)
    result       = LoadResult(table_name=target_table, layer="Gold")
    t0           = time.perf_counter()

    try:
        with db_write_lock:

            silver_exists = conn.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{silver_table}'"
            ).fetchone()[0] > 0
            if not silver_exists:
                log.warning("Silver table not found — skipping gold load", silver=silver_table)
                return result

            result.attempted = conn.execute(f"SELECT COUNT(*) FROM {silver_table}").fetchone()[0]
            if result.attempted == 0:
                return result

            is_fact = bool(getattr(meta, "target_fact", None))

            dim_key  = target_table.upper()
            dim_entry = _DIM_SK_MAP.get(dim_key) 

            if is_fact:
                dims_with_silver = [
                    dim for dim, (_, _, sv) in _DIM_SK_MAP.items() if sv is not None
                ]
                if not _dims_ready(conn, dims_with_silver):
                    result.success = False
                    result.error   = (
                        "Fact load deferred: one or more source dimensions "
                        "are not yet populated in Gold."
                    )
                    log.warning(result.error, fact=target_table)
                    return result
                join_clauses   = []
                sk_select_cols = []
                for dim_name, (nat_key, sk_col, sv) in _DIM_SK_MAP.items():
                    if sv is None:
                        continue
                    alias = dim_name.lower()   
                    # Find the FK column in THIS silver file that points to dim
                    fk_col = None
                    for col in meta.columns:
                        fk = getattr(col, "fk", None) or {}
                        if isinstance(fk, dict) and fk.get("target_dimension", "").upper() == dim_name:
                            fk_col = col.name
                            break
                    if fk_col:
                        join_clauses.append(
                            f"LEFT JOIN {dim_name} {alias} "
                            f"ON s.{fk_col} = {alias}.{nat_key.upper()}"
                        )
                        sk_select_cols.append(f"{alias}.{sk_col}")
                    else:
                        sk_select_cols.append(f"NULL AS {sk_col}")

                silver_cols = [
                    row[0] for row in conn.execute(
                        f"SELECT column_name FROM information_schema.columns "
                        f"WHERE table_name = '{silver_table}'"
                    ).fetchall()
                ]

                gold_cols = [
                    row[0].upper() for row in conn.execute(
                        f"SELECT column_name FROM information_schema.columns "
                        f"WHERE UPPER(table_name) = '{target_table.upper()}'"
                    ).fetchall()
                ]

                fact_sk_col = gold_cols[0] if gold_cols else "TICKET_SK"
                silver_upper = {c.upper(): c for c in silver_cols}
                sk_map_cols  = {sk for _, sk, _ in _DIM_SK_MAP.values()}

                select_parts      = []
                insert_col_names  = []

                for gc in gold_cols:
                    if gc == fact_sk_col:
                        select_parts.append(
                            f"ROW_NUMBER() OVER (ORDER BY s.{pk_cols[0] if pk_cols else 'rowid'}) "
                            f"+ COALESCE((SELECT MAX({fact_sk_col}) FROM {target_table}), 0)"
                        )
                        insert_col_names.append(gc)
                    elif gc in sk_map_cols:
                        # Find the dim this SK belongs to and its alias
                        matched = None
                        for dim_name, (nat_key, sk_col, sv) in _DIM_SK_MAP.items():
                            if sk_col == gc and sv is not None:
                                alias   = dim_name.lower()
                                matched = f"{alias}.{sk_col}"
                                break
                        select_parts.append(matched if matched else "NULL")
                        insert_col_names.append(gc)
                    elif gc in silver_upper:
                        select_parts.append(f"s.{silver_upper[gc]}")
                        insert_col_names.append(gc)
                    else:
                        select_parts.append("NULL")
                        insert_col_names.append(gc)

                joins_str  = "\n                    ".join(join_clauses)
                select_str = ",\n                        ".join(select_parts)
                cols_str   = ", ".join(insert_col_names)
                nat_pk     = pk_cols[0] if pk_cols else gold_cols[1] if len(gold_cols) > 1 else "ticket_id"

                query = f"""
                    INSERT INTO {target_table} ({cols_str})
                    SELECT
                        {select_str}
                    FROM {silver_table} s
                    {joins_str}
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {target_table} f
                        WHERE UPPER(f.{nat_pk}) = UPPER(s.{nat_pk})
                    )
                """

            else:
                # ── DIMENSION LOAD ────────────────────────────────────────────
                if not dim_entry:
                    log.warning(
                        "No DIM_SK_MAP entry found for target — falling back to plain upsert",
                        target=target_table
                    )
                    silver_cols = [
                        row[0] for row in conn.execute(
                            f"SELECT column_name FROM information_schema.columns "
                            f"WHERE table_name = '{silver_table}'"
                        ).fetchall()
                    ]
                    cols_str = ", ".join(silver_cols)
                    update_cols = [c for c in silver_cols if c.upper() not in [p.upper() for p in pk_cols]]
                    updates = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
                    if pk_str:
                        query = (
                            f"INSERT INTO {target_table} ({cols_str}) "
                            f"SELECT {cols_str} FROM {silver_table} s "
                            f"ON CONFLICT ({pk_str}) DO UPDATE SET {updates};"
                        )
                    else:
                        query = (
                            f"INSERT INTO {target_table} ({cols_str}) "
                            f"SELECT {cols_str} FROM {silver_table} s "
                            f"ON CONFLICT DO NOTHING;"
                        )
                else:
                    nat_key, sk_col, _ = dim_entry

                    # Fetch gold column list from the live schema
                    gold_cols = [
                        row[0].upper() for row in conn.execute(
                            f"SELECT column_name FROM information_schema.columns "
                            f"WHERE UPPER(table_name) = '{target_table.upper()}'"
                        ).fetchall()
                    ]

                    silver_cols = [
                        row[0] for row in conn.execute(
                            f"SELECT column_name FROM information_schema.columns "
                            f"WHERE table_name = '{silver_table}'"
                        ).fetchall()
                    ]
                    silver_upper = {c.upper(): c for c in silver_cols}

                    select_parts     = []
                    insert_col_names = []

                    for gc in gold_cols:
                        if gc == sk_col:
                            # Surrogate key: ROW_NUMBER offset by current max
                            select_parts.append(
                                f"ROW_NUMBER() OVER (ORDER BY s.{nat_key}) "
                                f"+ COALESCE((SELECT MAX({sk_col}) FROM {target_table}), 0)"
                            )
                            insert_col_names.append(gc)
                        else:
                            candidate = gc.replace("HASHED_", "")
                            if candidate in silver_upper:
                                select_parts.append(f"s.{silver_upper[candidate]}")
                            elif gc in silver_upper:
                                select_parts.append(f"s.{silver_upper[gc]}")
                            else:

                                select_parts.append("NULL")
                            insert_col_names.append(gc)

                    insert_cols_str = ", ".join(insert_col_names)
                    select_str      = ", ".join(select_parts)
                    nat_col_silver  = silver_upper.get(nat_key.upper(), nat_key)

                    query = f"""
                        INSERT INTO {target_table} ({insert_cols_str})
                        SELECT {select_str}
                        FROM {silver_table} s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {target_table} g
                            WHERE g.{nat_key.upper()} = s.{nat_col_silver}
                        )
                    """

            result.inserted = conn.execute(query).fetchone()[0]
            result.skipped  = result.attempted - result.inserted

    except Exception as e:
        result.success = False
        result.error   = str(e)
        _dispatch_alert(target_table, result.error)
    finally:
        result.duration_ms = (time.perf_counter() - t0) * 1000
        log.info("Gold Load Complete", table=target_table, inserted=result.inserted)
        conn.close()

    return result