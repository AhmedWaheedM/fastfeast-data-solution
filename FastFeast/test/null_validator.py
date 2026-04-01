import pyarrow as pa
import pyarrow.compute as pc
from typing import Tuple, Optional, List
from pathlib import Path
from FastFeast.pipeline.config.metadata import metadata_settings
from pipeline.logger import pipeline as log
from FastFeast.utilities.file_utils import build_metadata_map, get_file_metadata

metadata = build_metadata_map(metadata_settings)

def _get_non_nullable_columns(file_meta, table_columns: List[str]) -> List[str]:
    """
    Identifies columns that must not contain nulls and exist in the table.
    """
    return [ col.name for col in file_meta.columns if not col.nullable and col.name in table_columns]

def _create_null_mask_from_cache(null_masks, columns, table):

    mask = None

    for col_name in columns:

        col_nulls = null_masks[col_name] 

        # empty string check
        col = table.column(col_name)

        if pa.types.is_string(col.type):
            empty_mask = pc.equal(col, pa.scalar(''))
            col_nulls = pc.or_(col_nulls, empty_mask)

        mask = col_nulls if mask is None else pc.or_(mask, col_nulls)

    return mask

def _enrich_with_run_id(table: pa.Table, run_id: str) -> pa.Table:
    """
    Appends the run_id metadata column to the table.
    """
    run_id_arr = pa.array([run_id] * len(table), type=pa.string())
    return table.append_column('run_id', run_id_arr)

def validate_nulls(table: pa.Table, file_path: str, run_id: str) -> Tuple[Optional[pa.Table], Optional[pa.Table]]:
    """
    Validate that non‑nullable columns have no null values.
    Returns (valid_rows, invalid_rows) enriched with 'run_id'.
    """
    file_meta = get_file_metadata(metadata, Path(file_path).name)
    if not file_meta:
        return None, None

    check_cols = _get_non_nullable_columns(file_meta, table.column_names)
    null_mask = _create_null_mask_from_cache(table, check_cols)
    empty_mask = pc.equal(table.column(check_cols[0]), pa.scalar('')) if check_cols else None
    if empty_mask is not None:
        null_mask = pc.or_(null_mask, empty_mask)

    invalid_rows = table.filter(null_mask)
    valid_rows = table.filter(pc.invert(null_mask))

    valid_enriched = _enrich_with_run_id(valid_rows, run_id)
    invalid_enriched = _enrich_with_run_id(invalid_rows, run_id)

    log.info(f"Null validation for {file_path} (run_id={run_id}): "f"{len(valid_rows)} valid, {len(invalid_rows)} invalid.")
    
    return valid_enriched, invalid_enriched