# numeric_ranges_validator.py
import pyarrow as pa
import pyarrow.compute as pc
from typing import Tuple, Optional
from pathlib import Path
from FastFeast.pipeline.config.metadata import metadata_settings
from pipeline.logger import validation as log
from FastFeast.utilities.file_utils import get_config_source, get_file_metadata


def _enrich_with_run_id(table: pa.Table, run_id: str) -> Optional[pa.Table]:
    """Add a run_id column to the table."""
    if len(table) == 0:
        return None
    run_id_col = pa.array([run_id] * len(table), type=pa.string())
    return table.append_column("PIPELINE_RUN_ID", run_id_col)


def _compute_valid_range_mask(
    table: pa.Table, range_columns: list, run_id: str
) -> pa.Array:
    """
    Compute a boolean mask where True means the row's numeric values are all
    within the specified min/max (inclusive) ranges. Null values are considered
    valid (they do not break the range constraint).
    """
    num_rows = len(table)
    valid_mask = pa.array([True] * num_rows, type=pa.bool_())

    for col_def in range_columns:
        col_name = col_def["name"]
        min_val = col_def.get("min")
        max_val = col_def.get("max")
        # If both min and max are missing, skip
        if min_val is None and max_val is None:
            continue

        if col_name not in table.column_names:
            log.error(f"Column '{col_name}' missing for range validation. run_id={run_id}")
            return pa.array([False] * num_rows, type=pa.bool_())

        col = table.column(col_name)
        # Convert to float64 for comparison (works for both int and float)
        # Use safe=False because we assume the column is already numeric
        col_float = pc.cast(col, pa.float64(), safe=False)
        null_mask = pc.is_null(col_float)

        # Initialize out_of_range as all False
        out_of_range = pa.array([False] * num_rows, type=pa.bool_())

        if min_val is not None:
            # Values less than min are out of range
            less_than_min = pc.less(col_float, min_val)
            out_of_range = pc.or_(out_of_range, less_than_min)

        if max_val is not None:
            # Values greater than max are out of range
            greater_than_max = pc.greater(col_float, max_val)
            out_of_range = pc.or_(out_of_range, greater_than_max)

        # Row is invalid if it's not null and out of range
        invalid_in_col = pc.and_(pc.invert(null_mask), out_of_range)
        valid_mask = pc.and_(valid_mask, pc.invert(invalid_in_col))

        invalid_count = pc.sum(invalid_in_col.cast(pa.int64())).as_py()
        if invalid_count > 0:
            range_str = []
            if min_val is not None:
                range_str.append(f"min={min_val}")
            if max_val is not None:
                range_str.append(f"max={max_val}")
            log.warning(
                f"Column '{col_name}': {invalid_count} rows outside {', '.join(range_str)} (run_id={run_id})"
            )

    return valid_mask


def validate_numeric_ranges(
    table: pa.Table, file_path: str, run_id: str
) -> Tuple[Optional[pa.Table], Optional[pa.Table]]:
    """
    Validate that numeric columns stay within defined min/max ranges.
    Returns (valid_rows, invalid_rows).
    """
    metadata = get_file_metadata(
        get_config_source(file_path), Path(file_path).name
    )

    # Collect columns that have a 'range' attribute with at least one bound
    range_columns = []
    for col in metadata.columns:
        if hasattr(col, "range") and col.range:
            # col.range can be a dict like {'min': 0, 'max': 100}
            range_info = {"name": col.name}
            if hasattr(col.range, "get"):
                if "min" in col.range:
                    range_info["min"] = col.range["min"]
                if "max" in col.range:
                    range_info["max"] = col.range["max"]
            else:
                # If range is a simple numeric value, treat it as both min and max? Unlikely
                continue
            # Only add if at least one bound is defined
            if "min" in range_info or "max" in range_info:
                range_columns.append(range_info)

    if not range_columns:
        # No range constraints – treat whole table as valid
        return _enrich_with_run_id(table, run_id), None

    valid_mask = _compute_valid_range_mask(table, range_columns, run_id)
    valid_rows = _enrich_with_run_id(table.filter(valid_mask), run_id)
    invalid_rows = _enrich_with_run_id(table.filter(pc.invert(valid_mask)), run_id)

    return valid_rows, invalid_rows