# phone_validator.py
import pyarrow as pa
import pyarrow.compute as pc
from typing import Tuple, Optional
from pathlib import Path
from FastFeast.pipeline.config.metadata import metadata_settings
from pipeline.logger import validation as log
from FastFeast.utilities.file_utils import get_config_source, get_file_metadata

# A flexible international phone regex:
# - Optional leading '+'
# - Digits, spaces, parentheses, dashes allowed
# - Minimum 7 digits, maximum 15 digits (E.164 limit)
PHONE_REGEX = r"^[\+]?[\d\s\-\(\)]{7,30}$"


def _enrich_with_run_id(table: pa.Table, run_id: str) -> Optional[pa.Table]:
    if len(table) == 0:
        return None
    run_id_col = pa.array([run_id] * len(table), type=pa.string())
    return table.append_column("PIPELINE_RUN_ID", run_id_col)


def _compute_valid_phone_mask(table: pa.Table, phone_columns: list, run_id: str) -> pa.Array:
    num_rows = len(table)
    valid_mask = pa.array([True] * num_rows, type=pa.bool_())

    for col_name in phone_columns:
        if col_name not in table.column_names:
            log.error(f"Phone column '{col_name}' missing. run_id={run_id}")
            return pa.array([False] * num_rows, type=pa.bool_())

        col = table.column(col_name)
        col_str = pc.cast(col, pa.string(), safe=False)
        was_null = pc.is_null(col_str)

        is_valid_format = pc.match_substring_regex(col_str, PHONE_REGEX)
        invalid_in_col = pc.and_(pc.invert(was_null), pc.invert(is_valid_format))

        valid_mask = pc.and_(valid_mask, pc.invert(invalid_in_col))

        invalid_count = pc.sum(invalid_in_col.cast(pa.int64())).as_py()
        if invalid_count > 0:
            log.warning(
                f"Column '{col_name}': {invalid_count} invalid phone numbers (run_id={run_id})"
            )

    return valid_mask


def validate_phone_format(
    table: pa.Table, file_path: str, run_id: str
) -> Tuple[Optional[pa.Table], Optional[pa.Table]]:
    metadata = get_file_metadata(
        get_config_source(file_path), Path(file_path).name
    )
    phone_columns = [
        col.name for col in metadata.columns if getattr(col, "format", "") == "phone"
    ]

    if not phone_columns:
        return _enrich_with_run_id(table, run_id), None

    valid_mask = _compute_valid_phone_mask(table, phone_columns, run_id)
    valid_rows = _enrich_with_run_id(table.filter(valid_mask), run_id)
    invalid_rows = _enrich_with_run_id(table.filter(pc.invert(valid_mask)), run_id)

    return valid_rows, invalid_rows