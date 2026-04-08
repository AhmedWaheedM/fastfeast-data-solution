import pyarrow as pa
import time
import threading
from dataclasses import dataclass

from FastFeast.utilities.db_utils import get_connection
from FastFeast.utilities.file_utils import resolve_silver_name
from FastFeast.support.logger import pipeline as log

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

# Global lock to serialize writes to DuckDB (since it doesn't handle concurrent writes well)
db_write_lock = threading.Lock()

# Internal Helpers
def _build_pk_match_clause(pk_cols: list, alias1="s", alias2="temp_pa_view") -> str:
    """Handles both single and composite primary keys for SQL JOIN/NOT EXISTS."""
    if not pk_cols:
        return "1=1"
    return " AND ".join([f"{alias1}.{col} = {alias2}.{col}" for col in pk_cols])


#TODO might need to be refactored after merge, but for now this is a simple way to centralize error handling and alerting logic for both Silver and Gold loads.
def _dispatch_alert(target: str, error_msg: str):
    """Placeholder for the peer's async alerting logic."""
    log.error(f"ALERT: {target} load failed", error=error_msg)

# Silver Persistence
def load_to_silver(file_name: str, pa_table: pa.Table, meta, is_batch: bool) -> LoadResult:
    silver_table = resolve_silver_name(file_name)
    result = LoadResult(table_name=silver_table, layer="Silver", attempted=pa_table.num_rows)
    
    if pa_table.num_rows == 0:
        return result

    conn = get_connection()
    t0 = time.perf_counter()
    
    try:
        conn.register("temp_pa_view", pa_table)
        
        # Build PII Hashing Select (MD5 native in DuckDB, so much faster than doing it in PyArrow)
        select_cols = []
        for col_name in pa_table.column_names:
            is_pii = any(c.name.lower() == col_name.lower() and getattr(c, 'pii', False) 
                         for c in getattr(meta, 'columns', [])) if meta else False
            if is_pii:
                select_cols.append(f"MD5(CAST(temp_pa_view.{col_name} AS VARCHAR)) AS {col_name}")
            else:
                select_cols.append(f"temp_pa_view.{col_name}")
        
        select_clause = ", ".join(select_cols)
        pk_cols = meta.primary_keys if meta else []

        with db_write_lock:
            exists = conn.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{silver_table}'").fetchone()[0] > 0

            if is_batch:
                conn.execute(f"CREATE OR REPLACE TABLE {silver_table} AS SELECT {select_clause} FROM temp_pa_view")
                result.inserted = pa_table.num_rows
            else:
                if not exists:
                    conn.execute(f"CREATE TABLE {silver_table} AS SELECT {select_clause} FROM temp_pa_view")
                    result.inserted = pa_table.num_rows
                elif pk_cols:
                    match_clause = _build_pk_match_clause(pk_cols)
                    result.inserted = conn.execute(f"""
                        INSERT INTO {silver_table}
                        SELECT {select_clause} FROM temp_pa_view
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {silver_table} s WHERE {match_clause}
                        )
                    """).fetchone()[0]
                    result.skipped = result.attempted - result.inserted
                else:
                    conn.execute(f"INSERT INTO {silver_table} SELECT {select_clause} FROM temp_pa_view")
                    result.inserted = pa_table.num_rows

    except Exception as e:
        result.success = False
        result.error = str(e)
        _dispatch_alert(silver_table, result.error)
    finally:
        result.duration_ms = (time.perf_counter() - t0) * 1000
        log.info(f"Silver Load Complete", table=silver_table, inserted=result.inserted)
        try: conn.unregister("temp_pa_view")
        except: pass
        conn.close()
        
    return result

# Gold Dimensional Load
def load_to_gold(file_name: str, meta) -> LoadResult:
    target_table = getattr(meta, 'target_dimension', None) or getattr(meta, 'target_fact', None)
    
    if not meta or not target_table:
        return LoadResult(table_name="N/A", layer="Gold", success=True) 

    conn = get_connection()
    silver_table = resolve_silver_name(file_name)
    pk_cols = getattr(meta, 'primary_keys', [])  # Fallback to empty list if not defined TODO this should probably be refactored after merge to raise an error if primary keys aren't defined for a dimension, since we won't be able to do upserts without them.
    pk_str = ", ".join(pk_cols)
    
    result = LoadResult(table_name=target_table, layer="Gold")
    t0 = time.perf_counter()

    try:
        with db_write_lock:
            exists = conn.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{silver_table}'").fetchone()[0] > 0
            if not exists:
                return result

            result.attempted = conn.execute(f"SELECT COUNT(*) FROM {silver_table}").fetchone()[0]
            if result.attempted == 0:
                return result

            cols_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{silver_table}'"
            columns = [row[0] for row in conn.execute(cols_query).fetchall()]
            cols_str = ", ".join(columns)

            if getattr(meta, 'target_dimension', None):
                update_cols = [c for c in columns if c.upper() not in [pk.upper() for pk in pk_cols]]
                updates = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
                query = f"INSERT INTO {target_table} ({cols_str}) SELECT {cols_str} FROM {silver_table} ON CONFLICT ({pk_str}) DO UPDATE SET {updates};"
                
            else:
                query = f"INSERT INTO {target_table} ({cols_str}) SELECT {cols_str} FROM {silver_table} ON CONFLICT ({pk_str}) DO NOTHING;"

            result.inserted = conn.execute(query).fetchone()[0]
            result.skipped = result.attempted - result.inserted

    except Exception as e:
        result.success = False
        result.error = str(e)
        _dispatch_alert(target_table, result.error)
    finally:
        result.duration_ms = (time.perf_counter() - t0) * 1000
        log.info(f"Gold Load Complete", table=target_table, inserted=result.inserted)
        conn.close()

    return result