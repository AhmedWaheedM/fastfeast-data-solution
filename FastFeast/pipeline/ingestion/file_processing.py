from __future__ import annotations

from pathlib import Path
from typing import Optional

import pyarrow as pa

from FastFeast.pipeline.bridge.pyarrow_table import load_file
from FastFeast.pipeline.validation.schema_validation import validate_table
from FastFeast.utilities.metadata_cache import compare_columns
from FastFeast.utilities.validation_utils import (
    expected_types,
    not_null_column,
    column_format,
    get_column_pk,
    get_column_range,
    map_format_to_pattern,
    map_type_to_pyarrow,
    map_type_to_pattern,
    compose_table,
)


def build_validated_table(
    filepath: Path,
    pipeline_type: str = "stream",
) -> tuple[Optional[pa.Table], Optional[pa.Table], list[str], Optional[str]]:
    """
    Load a source file into a PyArrow table and run schema validation pipeline.

    Returns:
    - source_table: raw loaded table (or None on error)
    - composed_table: table with validation metadata columns (or None on error)
    - status_list: per-row statuses from validator (empty list on error)
    - error_code: None, "unsupported_file_type", or "column_mismatch"
    """
    source_table = load_file(filepath)
    if source_table is None:
        return None, None, [], "unsupported_file_type"

    if not compare_columns(filepath, source_table, pipeline_type):
        return source_table, None, [], "column_mismatch"

    expected_types_str = expected_types(filepath, pipeline_type)
    expected_types_pa = map_type_to_pyarrow(expected_types_str)
    expected_pattern = map_type_to_pattern(expected_types_str)

    not_null = not_null_column(filepath, pipeline_type)
    expected_formats_str = column_format(filepath, pipeline_type)
    expected_formats = map_format_to_pattern(expected_formats_str)
    column_range_ = get_column_range(filepath, pipeline_type)
    expected_pk = get_column_pk(filepath, pipeline_type)

    status_list, error_lists = validate_table(
        source_table,
        expected_types_pa,
        expected_pattern,
        not_null,
        expected_formats,
        column_range_,
        expected_pk,
        pipeline_type,
    )
    composed_table = compose_table(source_table, status_list, error_lists)
    return source_table, composed_table, status_list, None
