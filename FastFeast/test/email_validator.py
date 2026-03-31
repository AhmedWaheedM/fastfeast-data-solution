import pyarrow as pa
import pyarrow.compute as pc
from typing import Tuple, Optional
from pathlib import Path
from FastFeast.pipeline.config.metadata import metadata_settings
from pipeline.logger import validation as log
from FastFeast.utilities.file_utils import get_config_source, get_file_metadata

# RFC 5322‑inspired regex (simplified but covers most real‑world emails)
EMAIL_REGEX = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"


def _enrich_with_run_id(table: pa.Table, run_id: str) -> Optional[pa.Table]:
    """Add a run_id column to the table."""
    if len(table) == 0:
        return None
    run_id_col = pa.array([run_id] * len(table), type=pa.string())
    return table.append_column("PIPELINE_RUN_ID", run_id_col)


def _compute_valid_email_mask(table: pa.Table, email_columns: list, run_id: str) -> pa.Array:
    """
    Compute a boolean mask where True means the row is valid for all email columns.
    """
    num_rows = len(table)
    valid_mask = pa.array([True] * num_rows, type=pa.bool_())

    for col_name in email_columns:
        if col_name not in table.column_names:
            log.error(f"Email column '{col_name}' missing. run_id={run_id}")
            return pa.array([False] * num_rows, type=pa.bool_())

        col = table.column(col_name)
        # Convert to string (if it isn't already) for regex matching
        col_str = pc.cast(col, pa.string(), safe=False)
        was_null = pc.is_null(col_str)

        # Apply regex validation
        is_valid_format = pc.match_substring_regex(col_str, EMAIL_REGEX)
        # A row is invalid if it's not null but fails the format check
        invalid_in_col = pc.and_(pc.invert(was_null), pc.invert(is_valid_format))

        # Combine with overall mask
        valid_mask = pc.and_(valid_mask, pc.invert(invalid_in_col))

        invalid_count = pc.sum(invalid_in_col.cast(pa.int64())).as_py()
        if invalid_count > 0:
            log.warning(
                f"Column '{col_name}': {invalid_count} invalid email addresses (run_id={run_id})"
            )

    return valid_mask


def validate_email_format(
    table: pa.Table, file_path: str, run_id: str
) -> Tuple[Optional[pa.Table], Optional[pa.Table]]:
    """
    Validate email columns according to metadata.
    Returns (valid_rows, invalid_rows).
    """
    # Get column metadata and filter those with format='email'
    metadata = get_file_metadata(
        get_config_source(file_path), Path(file_path).name
    )
    email_columns = [
        col.name for col in metadata.columns if getattr(col, "format", "") == "email"
    ]

    if not email_columns:
        # No email columns to validate – treat whole table as valid
        return _enrich_with_run_id(table, run_id), None

    valid_mask = _compute_valid_email_mask(table, email_columns, run_id)
    valid_rows = _enrich_with_run_id(table.filter(valid_mask), run_id)
    invalid_rows = _enrich_with_run_id(table.filter(pc.invert(valid_mask)), run_id)

    return valid_rows, invalid_rows