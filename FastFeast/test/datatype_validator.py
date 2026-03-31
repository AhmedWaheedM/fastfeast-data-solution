import pyarrow as pa
import pyarrow.compute as pc
from typing import Tuple, Optional, List, Dict
from pathlib import Path
from FastFeast.pipeline.config.metadata import metadata_settings
from pipeline.logger import validation as log
from FastFeast.utilities.file_utils import get_config_source, get_file_metadata

def _map_type_to_pyarrow(type_str: str) -> pa.DataType:
    """
    Maps metadata type strings to PyArrow DataTypes.
    """
    mapping = {
        'integer': pa.int64(),
        'varchar': pa.string(),
        'float': pa.float64(),
        'boolean': pa.bool_(),
        'timestamp': pa.timestamp('us'),
        'date': pa.date32(),
    }
    return mapping.get(type_str, pa.string())

import pyarrow as pa
import pyarrow.compute as pc
import logging

log = logging.getLogger(__name__)

def _compute_valid_mask_vectorized(table: pa.Table, expected_types: dict, run_id: str) -> pa.Array:
    """
    Row-level valid mask, vectorized safe casting using PyArrow compute.
    Invalid values become False in the mask.
    """
    num_rows = len(table)
    valid_mask = pa.array([True] * num_rows, type=pa.bool_())

    for col_name, target_type in expected_types.items():
        if col_name not in table.column_names:
            log.error(f"Column '{col_name}' missing. run_id={run_id}")
            return pa.array([False] * num_rows, type=pa.bool_())

        col = table.column(col_name)
        was_null = pc.is_null(col)

        invalid_in_col = None

        # Vectorized per-type safe casting
        try:
            if pa.types.is_integer(target_type):
                # Use pc.cast with safe=True to avoid raising errors
                casted = pc.cast(col, target_type, safe=True)
            elif pa.types.is_floating(target_type):
                casted = pc.cast(col, target_type, safe=True)
            elif pa.types.is_boolean(target_type):
                # Convert strings like "true"/"false" safely
                casted = pc.case_when([
                    (pc.equal(pc.utf8_like(col), "true"), pa.array([True]*num_rows, type=pa.bool_())),
                    (pc.equal(pc.utf8_like(col), "false"), pa.array([False]*num_rows, type=pa.bool_()))
                ], else_=pa.array([None]*num_rows, type=pa.bool_()))
            else:
                # fallback to string
                casted = pc.cast(col, pa.string(), safe=True)

            is_now_null = pc.is_null(casted)

            # Only rows that became null but weren't null before are invalid
            invalid_in_col = pc.and_(pc.invert(was_null), is_now_null)

        except Exception as e:
            log.error(f"Column '{col_name}' fatal cast error: {e}")
            # If totally incompatible, mark all as invalid for this column
            invalid_in_col = pa.array([True] * num_rows, type=pa.bool_())

        # Update global mask
        valid_mask = pc.and_(valid_mask, pc.invert(invalid_in_col))

        # Logging
        invalid_count = pc.sum(invalid_in_col.cast(pa.int64())).as_py()
        if invalid_count > 0:
            log.warning(f"{col_name}: {invalid_count} invalid rows (run_id={run_id})")

    return valid_mask



def _enrich_with_run_id(table: pa.Table, run_id: str) -> Optional[pa.Table]:
    """
    Appends the run_id column to a table.
    """
    if len(table) == 0:
        return None
    run_id_col = pa.array([run_id] * len(table), type=pa.string())
    return table.append_column('PIPELINE_RUN_ID', run_id_col)

def validate_data_types(table: pa.Table, file_path: str, run_id: str) -> Tuple[Optional[pa.Table], Optional[pa.Table]]:
    """
    Main orchestrator for data type validation.
    Returns (valid_rows, invalid_rows).
    """
    expected_types = {col.name: _map_type_to_pyarrow(col.type) for col in get_file_metadata(get_config_source(file_path), Path(file_path).name).columns}
    valid_mask = _compute_valid_mask_vectorized(table, expected_types, run_id)
    valid_rows = _enrich_with_run_id(table.filter(valid_mask), run_id)
    invalid_rows = _enrich_with_run_id(table.filter(pc.invert(valid_mask)), run_id)    
    return valid_rows, invalid_rows