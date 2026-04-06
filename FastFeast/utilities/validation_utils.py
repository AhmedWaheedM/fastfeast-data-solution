from FastFeast.pipeline.config.metadata import load
from FastFeast.pipeline.config.config import get_config
from pathlib import Path
import pyarrow as pa


config = get_config()

yaml_path = Path(Path(__file__).parent.parent / config.paths.metadata_yaml)
metadata_settings = load(yaml_path)

#############################################

def expected_types(file_name, pipeline_type):
    source = metadata_settings.batch if pipeline_type == 'batch' else metadata_settings.stream
    expected_type = {}

    for file_meta in source:
        if file_meta.file_name == file_name.name:
            for column in file_meta.columns:
                expected_type[column.name] = column.type

            return expected_type  

    return expected_type

############################################

def not_null_column(file_name, pipeline_type):
    source = metadata_settings.batch if pipeline_type == 'batch' else metadata_settings.stream
    notnull = []

    for file_meta in source:
        if file_meta.file_name == file_name.name:
            for column in file_meta.columns:
                if column.nullable is False:
                    notnull.append(column.name)

            return notnull  
    return notnull

############################################

def column_format(file_name, pipeline_type):
    source = metadata_settings.batch if pipeline_type == 'batch' else metadata_settings.stream
    columns_format = {}

    for file_meta in source:
        if file_meta.file_name == file_name.name:
            for column in file_meta.columns:
                if column.format:
                    columns_format[column.name] = column.format

            return columns_format  
    return columns_format

############################################

def get_column_range(file_name, pipeline_type):
    source = metadata_settings.batch if pipeline_type == 'batch' else metadata_settings.stream
    column_range = {}

    for file_meta in source:
        if file_meta.file_name == file_name.name:
            for column in file_meta.columns:
                if column.range:
                    column_range[column.name] = column.range

            return column_range  
    return column_range

############################################

def _map_type_to_pyarrow(type_str: str) -> pa.DataType:
    mapping = {
        'integer': pa.int64(),
        'varchar': pa.string(),
        'float': pa.float64(),
        'boolean': pa.bool_(),
        'timestamp': pa.timestamp('us'),
        'date': pa.date32(),
    }
    return mapping.get(type_str, pa.string())

############################################

def _map_type_to_pattern(pattern):
    mapping = {
        'integer': r"^-?\d+$",
        'varchar': "",
        'float': r"^-?\d+(\.\d+)?$",
        'boolean': r"^(?i:true|false|0|1)$",
        'timestamp': r"^\d{4}-\d{2}-\d{2}.*$",
        'date': r"^\d{4}-\d{2}-\d{2}$",
    }
    return mapping.get(pattern,"")

############################################

def _map_format_to_pattern(format):
    mapping = {
        'email': r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        'phone': r"^0\d{10}$"
    }
    return mapping.get(format,"")

############################################

# def map_type_to_pyarrow(expected_types):
#     return {t: _map_type_to_pyarrow(t) for t in expected_types}

def map_type_to_pyarrow(expected_types):
    return {
        col: _map_type_to_pyarrow(type_str)
        for col, type_str in expected_types.items()
    }

# def map_type_to_pyarrow(expected_types):
#     return {col: _map_type_to_pyarrow(type_str) for col, type_str in expected_types.items()}

def map_type_to_pattern(expected_types):
    return {t: _map_type_to_pattern(t) for t in expected_types}

# def map_type_to_pattern(expected_types):
#     return {col: _map_type_to_pattern(type_str) for col, type_str in expected_types.items()}

def map_format_to_pattern(expected_formats):
    return {f: _map_format_to_pattern(f) for f in expected_formats}

############################################
def compose_table(pa_table, record_status, error_lists):
  map_type = pa.map_(pa.string(), pa.list_(pa.string()))
  errors_array = pa.array(error_lists, type=map_type)
  pa_table = pa_table.append_column('_record_status', pa.array(record_status))
  pa_table = pa_table.append_column('_error_reasons', errors_array)
  pa_table = pa_table.append_column('_retry_count', pa.array([0 for _ in range(len(error_lists))]))
  return pa_table

############################################

def get_column_pk(file_name, pipeline_type):
    source = metadata_settings.batch if pipeline_type == 'batch' else metadata_settings.stream
    column_name = ''

    for file_meta in source:
        if file_meta.file_name == file_name.name:
            for column in file_meta.columns:
                if column.pk:
                   column_name = column.name
                   return column_name

    return column_name
