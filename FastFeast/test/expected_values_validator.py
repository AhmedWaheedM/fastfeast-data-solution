# expected_values_validator.py
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


def _compute_valid_expected_mask(table: pa.Table, expected_columns: list, run_id: str) -> pa.Array:
    """
    Compute a boolean mask where True means the row's values are all in the
    expected set (or are null). Null values are considered valid.
    """
    num_rows = len(table)
    valid_mask = pa.array([True] * num_rows, type=pa.bool_())

    for col_def in expected_columns:
        col_name = col_def["name"]
        expected_values = col_def["expected"]

        if col_name not in table.column_names:
            log.error(f"Column '{col_name}' missing for expected values validation. run_id={run_id}")
            return pa.array([False] * num_rows, type=pa.bool_())

        col = table.column(col_name)
        # Convert column to string for uniform comparison
        col_str = pc.cast(col, pa.string(), safe=False)
        null_mask = pc.is_null(col_str)

        # Convert expected values to PyArrow string array
        expected_arr = pa.array([str(v) for v in expected_values], type=pa.string())

        # Check membership
        is_in_expected = pc.is_in(col_str, value_set=expected_arr)
        # Row is invalid if it's not null and not in expected set
        invalid_in_col = pc.and_(pc.invert(null_mask), pc.invert(is_in_expected))

        valid_mask = pc.and_(valid_mask, pc.invert(invalid_in_col))

        invalid_count = pc.sum(invalid_in_col.cast(pa.int64())).as_py()
        if invalid_count > 0:
            log.warning(
                f"Column '{col_name}': {invalid_count} rows have values not in {expected_values} (run_id={run_id})"
            )

    return valid_mask


def validate_expected_values(
    table: pa.Table, file_path: str, run_id: str
) -> Tuple[Optional[pa.Table], Optional[pa.Table]]:
    """
    Validate that columns only contain values from a predefined list.
    Returns (valid_rows, invalid_rows).
    """
    metadata = get_file_metadata(
        get_config_source(file_path), Path(file_path).name
    )

    # Collect columns with an 'expected_values' attribute
    expected_columns = []
    for col in metadata.columns:
        if hasattr(col, "expected_values") and col.expected_values:
            expected_columns.append({
                "name": col.name,
                "expected": col.expected_values
            })

    if not expected_columns:
        # No expected values constraints – treat whole table as valid
        return _enrich_with_run_id(table, run_id), None

    valid_mask = _compute_valid_expected_mask(table, expected_columns, run_id)
    valid_rows = _enrich_with_run_id(table.filter(valid_mask), run_id)
    invalid_rows = _enrich_with_run_id(table.filter(pc.invert(valid_mask)), run_id)

    return valid_rows, invalid_rows