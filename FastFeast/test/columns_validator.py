import pyarrow as pa
from pathlib import Path
from collections import Counter
from typing import List, Optional, Any
from FastFeast.pipeline.config.metadata import metadata_settings
from pipeline.logger import validation as log
from FastFeast.utilities.file_utils import get_config_source, get_file_metadata


def _has_duplicate_columns(actual_cols: List[str], file_path: str, run_id: str) -> bool:
    """
    Check for duplicate column names and log a warning if found.
    """
    duplicates = [name for name, count in Counter(actual_cols).items() if count > 1]
    if duplicates:
        log.warning(f"Duplicate columns in table from {file_path} (run_id={run_id}): {duplicates}")
        return True
    return False


def _has_schema_mismatch(expected_cols: List[str], actual_cols: List[str], file_path: str, run_id: str) -> bool:
    """
    Check for missing or extra columns compared to the expected metadata.
    """
    expected_set, actual_set = set(expected_cols), set(actual_cols)
    missing = expected_set - actual_set
    extra = actual_set - expected_set
    if missing or extra:
        log.warning(f"Column mismatch for {file_path} (run_id={run_id}): "f"missing columns: {missing}, extra columns: {extra}")
        return True
    return False


def valid_columns(table: pa.Table, file_path: str, run_id: str) -> bool:
    """
    Validate that the table's columns match the expected columns from metadata.
    Checks for missing, extra, and duplicate columns.
    Returns True if validation passes or is skipped, False if schema issues are found.
    """
    actual_cols = table.column_names
    if _has_duplicate_columns(actual_cols, file_path, run_id) or _has_schema_mismatch([col.name for col in get_file_metadata(get_config_source(file_path), Path(file_path).name).columns], actual_cols, file_path, run_id):
        return False
    return True



    