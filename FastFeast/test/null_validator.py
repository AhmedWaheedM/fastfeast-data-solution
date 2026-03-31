import pyarrow as pa
import pyarrow.compute as pc
from typing import Tuple, Optional, List
from pathlib import Path

from pipeline.config.metadata import metadata_settings
from pipeline.logger import pipeline as log

def _get_file_metadata(file_path: str, run_id: str):
    """Parses path to determine source and retrieves file metadata."""
    path = Path(file_path)
    path_str = str(path).lower()
    
    if "batch" in path_str:
        file_list = metadata_settings.batch
    elif "stream" in path_str:
        file_list = metadata_settings.stream
    else:
        log.warning(f"Path {file_path} (run_id={run_id}) unknown source – skipping.")
        return None

    return next((fm for fm in file_list if fm.file_name == path.name), None)

def _get_non_nullable_columns(file_meta, table_columns: List[str]) -> List[str]:
    """Identifies columns that must not contain nulls and exist in the table."""
    return [
        col.name for col in file_meta.columns 
        if not col.nullable and col.name in table_columns
    ]

def _create_null_mask(table: pa.Table, columns: List[str]) -> pa.BooleanArray:
    """Builds a mask where True indicates a row contains a null in the specified columns."""
    mask = None
    for col_name in columns:
        col_nulls = pc.is_null(table.column(col_name))
        mask = col_nulls if mask is None else pc.or_(mask, col_nulls)
    
    # If no columns to check, return a mask of all False (no nulls found)
    return mask if mask is not None else pa.array([False] * len(table))

def _enrich_with_run_id(table: pa.Table, run_id: str) -> pa.Table:
    """Appends the run_id metadata column to the table."""
    run_id_arr = pa.array([run_id] * len(table), type=pa.string())
    return table.append_column('run_id', run_id_arr)

def validate_nulls(
    table: pa.Table,
    file_path: str,
    run_id: str
) -> Tuple[Optional[pa.Table], Optional[pa.Table]]:
    """
    Validate that non‑nullable columns have no null values.
    Returns (valid_rows, invalid_rows) enriched with 'run_id'.
    """
    # 1. Resolve Metadata
    file_meta = _get_file_metadata(file_path, run_id)
    if not file_meta:
        return None, None

    # 2. Identify target columns and generate mask
    check_cols = _get_non_nullable_columns(file_meta, table.column_names)
    null_mask = _create_null_mask(table, check_cols)

    # 3. Split Table
    invalid_rows = table.filter(null_mask)
    valid_rows = table.filter(pc.invert(null_mask))

    # 4. Enrich results
    valid_enriched = _enrich_with_run_id(valid_rows, run_id)
    invalid_enriched = _enrich_with_run_id(invalid_rows, run_id)

    log.info(
        f"Null validation for {file_path} (run_id={run_id}): "
        f"{len(valid_rows)} valid, {len(invalid_rows)} invalid."
    )
    
    return valid_enriched, invalid_enriched