from dataclasses import dataclass
from datetime import datetime
import time
from pathlib import Path
import json
import re
import pyarrow as pa
import pyarrow.csv as pv
from FastFeast.pipeline.config.metadata import metadata_settings, FileMeta
from FastFeast.pipeline.config.config import config_settings
from pipeline.logger import pipeline as log  #logger script

from concurrent.futures import ThreadPoolExecutor, as_completed

#------------------------------------------------------------------------------------
# Validation Points:
# - loop in today folder and its all files
# - fetch matched and dismached datatypes
# - fetch extra, missing columns
# - Build schema baseline from metadat file, compare it with incoming files
# - makesure of files format
# - check for null-values
# - use threadpool with 4 executors

#------------------------------------------------------------------------------------


# ----------------------------
# Supported File Types
# ----------------------------
SUPPORTED_EXTENSIONS = {".csv", ".json"}


# ----------------------------
# Type Normalization
# ----------------------------
def normalize_type(col_type: str) -> str:
    type_mapping = {
        "int": "int",
        "integer": "int",
        "bigint": "int",
        "smallint": "int",

        "float": "float",
        "double": "float",

        "string": "string",
        "varchar": "string",
        "text": "string",

        "bool": "bool",
        "boolean": "bool",

        "date": "date",
        "timestamp": "timestamp",
    }

    col_type_lower = col_type.lower()

    if col_type_lower not in type_mapping:
        raise ValueError(f"Unsupported metadata type: {col_type}")

    return type_mapping[col_type_lower]


def map_to_pyarrow_type(col_type: str):
    normalized = normalize_type(col_type)

    mapping = {
        "int": pa.int64(),
        "float": pa.float64(),
        "string": pa.string(),
        "bool": pa.bool_(),
        "date": pa.date32(),
        "timestamp": pa.timestamp("s"),
    }

    return mapping[normalized]


# ----------------------------
# Metadata Access
# ----------------------------
def get_file_meta(file_name: str) -> FileMeta:
    for f in metadata_settings.batch:
        if f.file_name == file_name:
            return f
    raise ValueError(f"Metadata not found for file: {file_name}")


# ----------------------------
# Schema Builder (Baseline)
# ----------------------------
def build_schema(file_meta: FileMeta) -> pa.Schema:
    fields = []

    for col in file_meta.columns:
        fields.append(
            pa.field(
                col.name,
                map_to_pyarrow_type(col.type),
                nullable=col.nullable
            )
        )

    return pa.schema(fields)


# ----------------------------
# Validation Functions
# ----------------------------
def validate_columns(table: pa.Table, file_meta: FileMeta):
    expected_cols = {col.name for col in file_meta.columns}
    actual_cols = set(table.column_names)

    missing = expected_cols - actual_cols
    extra = actual_cols - expected_cols

    return {
        "missing_columns": list(missing),
        "extra_columns": list(extra),
    }


def validate_types(table: pa.Table, file_meta: FileMeta):
    mismatches = []
    baseline_schema = build_schema(file_meta)

    table_fields = {f.name: f.type for f in table.schema}
    baseline_fields = {f.name: f.type for f in baseline_schema}

    for col in baseline_fields:
        if col in table_fields:
            if baseline_fields[col] != table_fields[col]:
                mismatches.append({
                    "column": col,
                    "expected": str(baseline_fields[col]),
                    "actual": str(table_fields[col])
                })

    return mismatches


def validate_non_null(table: pa.Table, file_meta: FileMeta):
    violations = []

    for col in file_meta.columns:
        if not col.nullable:
            if col.name in table.column_names:
                if table[col.name].null_count > 0:
                    violations.append({
                        "column": col.name,
                        "null_count": table[col.name].null_count
                    })

    return violations


# ----------------------------
# Full Validation Orchestrator
# ----------------------------
def validate_table(table: pa.Table, file_name: str):
    file_meta = get_file_meta(file_name)

    return {
        "file": file_name,
        "columns": validate_columns(table, file_meta),
        "types": validate_types(table, file_meta),
        "nulls": validate_non_null(table, file_meta),
    }

# ----------------------------------------------------------------------
# Clean unexpected values like : Nan
# ----------------------------------------------------------------------

def load_clean_json(file_path):
    with open(file_path, "r", encoding=config_settings.batch.encoding) as f:
        content = f.read()
    content = re.sub(r'\bNaN\b', 'null', content)
    return json.loads(content)


# ----------------------------
# File Readers
# ----------------------------
def read_file(file_path: Path) -> pa.Table:
    if file_path.suffix.lower() == ".csv":
        return pv.read_csv(file_path)

    elif file_path.suffix.lower() == ".json":
        with open(file_path, 'r') as f:
            data = load_clean_json(file_path)
            table = pa.Table.from_pylist(data)
            return table

    else:
        raise ValueError(f"Unsupported file type: {file_path.suffix}")


# ----------------------------
# Worker Function (ThreadPool)
# ----------------------------
def process_single_file(file_path: Path):

    start = time.time()

    log.info(f"Processing file: {file_path.name}")

    try:
        table = read_file(file_path)

        result = validate_table(table, file_path.name)
        result["status"] = "success"

        log.info(f"Validation succeeded: {file_path.name}")

    except Exception as e:
        result = {
            "file": file_path.name,
            "status": "failed",
            "error": str(e)
        }

        log.error(f"Validation failed: {file_path.name} | Error: {str(e)}")

    duration = time.time() - start
    result["duration_sec"] = duration

    return result


# ----------------------------
# Folder Processing
# ----------------------------
def process_folder(folder_path: str):
    folder = Path(folder_path)

    results = []

    log.info(f"Starting processing folder: {folder_path}")

    files_to_process = []

    for file_path in sorted(folder.iterdir()):

        if not file_path.is_file():
            continue

        if file_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
            log.warning(f"Skipping unsupported file type: {file_path.name}")
            continue

        files_to_process.append(file_path)

    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_file = {
            executor.submit(process_single_file, file_path): file_path
            for file_path in files_to_process
        }

        for future in as_completed(future_to_file):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                file_path = future_to_file[future]
                log.error(f"Thread failed for {file_path.name}: {str(e)}")

    log.info(f"Finished processing folder: {folder_path}")

    return results


# ----------------------------
# Entry Point
# ----------------------------
if __name__ == "__main__":
    
    batch_dir = Path(config_settings.paths.batch_dir)
    today = datetime.now().date()
    today_folder = Path(batch_dir) / today.strftime(config_settings.datetime_handling.date_key_format)
    results = process_folder(today_folder)

    print(json.dumps(results, indent=4, ensure_ascii=False))