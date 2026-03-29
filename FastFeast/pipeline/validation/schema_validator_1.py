import duckdb
import pyarrow as pa
from typing import List, Optional
from FastFeast.pipeline.config.metadata import Column
from pipeline.logger import pipeline as log

def _map_type_to_duckdb(type_str: str) -> str:
    mapping = {
        'integer': 'INTEGER',
        'varchar': 'VARCHAR',
        'float': 'FLOAT',
        'boolean': 'BOOLEAN',
        'timestamp': 'TIMESTAMP',
        'date': 'DATE',
    }
    return mapping.get(type_str, 'VARCHAR')

def validate_and_filter(table: pa.Table, columns: List[Column]) -> Optional[pa.Table]:
    conn = duckdb.connect()
    conn.register("input_table", table)

    # 1. Check required columns exist
    required_cols = [col.name for col in columns if not col.nullable]
    missing = [col for col in required_cols if col not in table.column_names]
    if missing:
        log.error(f"Missing required columns: {missing}")
        conn.close()
        return None

    constraints = []
    for col in columns:
        col_name = col.name
        
        # 2. Type check using TRY_CAST (returns NULL if cast fails)
        duckdb_type = _map_type_to_duckdb(col.type)
        constraints.append(f"TRY_CAST({col_name} AS {duckdb_type}) IS NOT NULL")

        # 3. Non‑null constraint
        if not col.nullable:
            constraints.append(f"{col_name} IS NOT NULL")

    # If no constraints, return original table
    if not constraints:
        conn.close()
        return table

    # 3. Log invalid counts per constraint (optional but helpful)
    for constraint in constraints:
        count_query = f"SELECT COUNT(*) FROM input_table WHERE NOT ({constraint})"
        invalid_count = conn.execute(count_query).fetchone()[0]
        if invalid_count > 0:
            log.warning(f"Constraint '{constraint}' violated by {invalid_count} rows")

    # 4. Filter rows that satisfy all constraints
    where_clause = " AND ".join(constraints)
    filtered_query = f"SELECT * FROM input_table WHERE {where_clause}"
    filtered_table = conn.execute(filtered_query).arrow()
    if isinstance(filtered_table, pa.RecordBatchReader):
        filtered_table = pa.Table.from_batches(list(filtered_table))
        conn.close()
        return filtered_table
    