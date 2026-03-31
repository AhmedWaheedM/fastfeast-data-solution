# date_validator.py
import pyarrow as pa
import pyarrow.compute as pc
from typing import Tuple, Optional
from pathlib import Path
from FastFeast.pipeline.config.metadata import metadata_settings
from pipeline.logger import validation as log
from FastFeast.utilities.file_utils import get_config_source, get_file_metadata

# Regex for ISO 8601 date (YYYY-MM-DD)
DATE_REGEX = r"^\d{4}-\d{2}-\d{2}$"


def _enrich_with_run_id(table: pa.Table, run_id: str) -> Optional[pa.Table]:
    if len(table) == 0:
        return None
    run_id_col = pa.array([run_id] * len(table), type=pa.string())
    return table.append_column("PIPELINE_RUN_ID", run_id_col)


def _compute_valid_date_mask(table: pa.Table, date_columns: list, run_id: str) -> pa.Array:
    num_rows = len(table)
    valid_mask = pa.array([True] * num_rows, type=pa.bool_())

    for col_name in date_columns:
        if col_name not in table.column_names:
            log.error(f"Date column '{col_name}' missing. run_id={run_id}")
            return pa.array([False] * num_rows, type=pa.bool_())

        col = table.column(col_name)
        col_str = pc.cast(col, pa.string(), safe=False)
        was_null = pc.is_null(col_str)

        # First check regex format
        regex_valid = pc.match_substring_regex(col_str, DATE_REGEX)

        # For rows that pass regex, try to cast to date32 (validates calendar)
        # We'll create a mask of rows that pass regex but fail casting
        try:
            # Create a filtered table with only regex‑valid rows for casting
            regex_valid_mask = regex_valid
            # Extract non‑null regex‑valid strings
            temp_str = col_str.filter(regex_valid_mask)
            # Attempt cast; if it fails, the entire cast will raise an error.
            # To avoid stopping the whole process, we catch and handle.
            casted = pc.cast(temp_str, pa.date32(), safe=True)
            # Since safe=True, invalid dates become null. So we compare original nullity.
            # But we only have the filtered subset. We need to map back.
            # Simpler: use try-catch around cast for each row? Not vectorized.
            # Alternative: rely on regex only – dates like 2023-02-30 will pass regex but be invalid.
            # For full validation, we can use pyarrow's `pc.strptime` with format '%Y-%m-%d'.
            # Let's use strptime to get a timestamp and then check if it's null.
            parsed = pc.strptime(col_str, format="%Y-%m-%d", unit="s")
            is_valid_date = pc.is_not_null(parsed)
        except Exception as e:
            log.error(f"Date validation error for column '{col_name}': {e}")
            is_valid_date = regex_valid  # fallback to regex only

        # A row is invalid if it's not null but fails format or date validity
        invalid_in_col = pc.and_(
            pc.invert(was_null),
            pc.invert(pc.and_(regex_valid, is_valid_date))
        )

        valid_mask = pc.and_(valid_mask, pc.invert(invalid_in_col))

        invalid_count = pc.sum(invalid_in_col.cast(pa.int64())).as_py()
        if invalid_count > 0:
            log.warning(
                f"Column '{col_name}': {invalid_count} invalid dates (run_id={run_id})"
            )

    return valid_mask


def validate_date_format(
    table: pa.Table, file_path: str, run_id: str
) -> Tuple[Optional[pa.Table], Optional[pa.Table]]:
    metadata = get_file_metadata(
        get_config_source(file_path), Path(file_path).name
    )
    date_columns = [
        col.name for col in metadata.columns if getattr(col, "format", "") == "date"
    ]

    if not date_columns:
        return _enrich_with_run_id(table, run_id), None

    valid_mask = _compute_valid_date_mask(table, date_columns, run_id)
    valid_rows = _enrich_with_run_id(table.filter(valid_mask), run_id)
    invalid_rows = _enrich_with_run_id(table.filter(pc.invert(valid_mask)), run_id)

    return valid_rows, invalid_rows