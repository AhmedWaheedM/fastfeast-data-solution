import pyarrow as pa
import pyarrow.compute as pc
from typing import Optional, List
from pathlib import Path
from FastFeast.pipeline.config.metadata import metadata_settings
from pipeline.logger import pipeline as log
import json

# Regex patterns
EMAIL_REGEX = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
PHONE_REGEX = r'^\+?[1-9]\d{1,14}$'
DATE_ISO_REGEX = r'^\d{4}-\d{2}-\d{2}$'
TIMESTAMP_REGEX = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'

def _get_metadata_for_file(file_path: str, run_id: str) -> Optional[List]:
    path = Path(file_path)
    file_name = path.name
    path_str = str(path).lower()
    if "batch" in path_str:
        file_list = metadata_settings.batch
    elif "stream" in path_str:
        file_list = metadata_settings.stream
    else:
        log.warning(f"Path {file_path} (run_id={run_id}) does not contain 'batch' or 'stream' – skipping profiling.")
        return None
    file_meta = next((fm for fm in file_list if fm.file_name == file_name), None)
    if not file_meta:
        log.warning(f"No metadata for {file_name} (run_id={run_id}) – skipping profiling.")
        return None
    return file_meta.columns

def _count_invalid_regex_vectorized(col: pa.Array, regex: str) -> int:
    col = pc.drop_null(col)
    if len(col) == 0:
        return 0
    str_col = pc.cast(col, pa.string())
    mask = pc.match_substring_regex(str_col, regex)
    return int(pc.sum(pc.invert(mask).cast(pa.int64())).as_py())

def _count_out_of_range(col: pa.Array, col_type: str, range_dict: dict) -> int:
    col = pc.drop_null(col)
    if len(col) == 0 or not range_dict or col_type not in ('integer', 'float'):
        return 0
    mask = None
    if 'min' in range_dict and range_dict['min'] is not None:
        mask_min = pc.less(col, pa.scalar(range_dict['min']))
        mask = mask_min if mask is None else pc.or_(mask, mask_min)
    if 'max' in range_dict and range_dict['max'] is not None:
        mask_max = pc.greater(col, pa.scalar(range_dict['max']))
        mask = mask_max if mask is None else pc.or_(mask, mask_max)
    if mask is None:
        return 0
    return int(pc.sum(mask.cast(pa.int64())).as_py())

def _count_invalid_expected(col: pa.Array, expected_values: list) -> int:
    if not expected_values:
        return 0
    col = pc.drop_null(col)
    if len(col) == 0:
        return 0
    mask = pc.is_in(col, pa.array(expected_values))
    return int(pc.sum(pc.invert(mask).cast(pa.int64())).as_py())

def profile_table(table: pa.Table, file_path: str, run_id: str) -> str:
    columns_meta = _get_metadata_for_file(file_path, run_id)
    if columns_meta is None:
        return json.dumps({"error": "No metadata found for this file"}, indent=4)

    profile_rows = []
    file_name = Path(file_path).name
    total_rows = len(table)

    expected_columns = [col_def.name for col_def in columns_meta]
    actual_columns = table.column_names
    missing_columns = [c for c in expected_columns if c not in actual_columns]
    extra_columns = [c for c in actual_columns if c not in expected_columns]

    # Table-level duplicates (entire rows) using JSON serialization
    if total_rows == 0:
        table_duplicates_count = 0
        table_duplicates_percentage = 0.0
    else:
        rows_as_strings = pa.array([json.dumps(row, default=str) for row in table.to_pylist()])
        vc = pc.value_counts(rows_as_strings)
        counts_field = vc.field("counts")
        dup_mask = pc.greater(counts_field, pa.scalar(1))
        table_duplicates_count = int(pc.sum(pc.subtract(counts_field.filter(dup_mask), pa.scalar(1))).as_py())
        table_duplicates_percentage = round(100 * table_duplicates_count / total_rows, 2)

    for col_def in columns_meta:
        col_name = col_def.name

        if col_name not in table.column_names:
            profile_rows.append({
                'column_name': col_name,
                'data_type': col_def.type,
                'total_rows': total_rows,
                'null_count': total_rows,
                'null_percentage': 100.0,
                'min': None,
                'max': None,
                'out_of_range_count': 0,
                'out_of_range_percentage': 0.0,
                'invalid_format_count': 0,
                'invalid_format_percentage': 0.0,
                'invalid_expected_count': 0,
                'invalid_expected_percentage': 0.0,
                'null_in_non_nullable': 1 if not col_def.nullable else 0,
                'null_in_non_nullable_percentage': round(100.0 if not col_def.nullable else 0.0, 2),
            })
            continue

        col = table.column(col_name)
        null_count = int(pc.sum(pc.is_null(col).cast(pa.int64())).as_py())
        non_null_col = pc.drop_null(col)

        min_val = max_val = None
        if col_def.type in ('integer', 'float') and len(non_null_col) > 0:
            min_val = float(pc.min(non_null_col).as_py())
            max_val = float(pc.max(non_null_col).as_py())

        out_of_range_count = _count_out_of_range(col, col_def.type, getattr(col_def, 'range', {}))

        invalid_format_count = 0
        if col_def.format == 'email':
            invalid_format_count = _count_invalid_regex_vectorized(col, EMAIL_REGEX)
        elif col_def.format == 'phone':
            invalid_format_count = _count_invalid_regex_vectorized(col, PHONE_REGEX)
        elif col_def.format == 'iso_date':
            invalid_format_count = _count_invalid_regex_vectorized(col, DATE_ISO_REGEX)
        elif col_def.format == 'timestamp':
            invalid_format_count = _count_invalid_regex_vectorized(col, TIMESTAMP_REGEX)
        elif col_def.type == 'date' and col_def.format is None:
            invalid_format_count = _count_invalid_regex_vectorized(col, DATE_ISO_REGEX)
        elif col_def.type == 'timestamp' and col_def.format is None:
            invalid_format_count = _count_invalid_regex_vectorized(col, TIMESTAMP_REGEX)

        invalid_expected_count = _count_invalid_expected(col, getattr(col_def, 'expected_values', []))
        null_in_non_nullable = null_count if not col_def.nullable else 0

        def pct(count):
            return round(100 * count / total_rows, 2) if total_rows > 0 else 0.0

        profile_rows.append({
            'column_name': col_name,
            'data_type': col_def.type,
            'total_rows': total_rows,
            'null_count': null_count,
            'null_percentage': pct(null_count),
            'min': min_val,
            'max': max_val,
            'out_of_range_count': out_of_range_count,
            'out_of_range_percentage': pct(out_of_range_count),
            'invalid_format_count': invalid_format_count,
            'invalid_format_percentage': pct(invalid_format_count),
            'invalid_expected_count': invalid_expected_count,
            'invalid_expected_percentage': pct(invalid_expected_count),
            'null_in_non_nullable': null_in_non_nullable,
            'null_in_non_nullable_percentage': pct(null_in_non_nullable),
        })

    result = {
        "run_id": run_id,
        "file_name": file_name,
        "total_rows": total_rows,
        "table_duplicates_count": table_duplicates_count,
        "table_duplicates_percentage": table_duplicates_percentage,
        "expected_columns": expected_columns,
        "actual_columns": actual_columns,
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "columns_profile": profile_rows
    }

    return json.dumps(result, indent=4)