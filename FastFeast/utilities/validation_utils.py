from FastFeast.pipeline.config.metadata import load
from pathlib import Path


yaml_path = Path(Path(__file__).parent.parent / "pipeline/config/files_metadata.yaml")
metadata_settings = load(yaml_path)

#############################################

def expected_types(file_name, type):
    source = metadata_settings.batch if type == 'batch' else metadata_settings.stream
    expected_type = {}

    for file_meta in source:
        if file_meta.file_name == file_name:
            for column in file_meta.columns:
                expected_type[column.name] = column.type

            return expected_type  

    return expected_type

############################################

def not_null_column(file_name, type):
    source = metadata_settings.batch if type == 'batch' else metadata_settings.stream
    notnull = []

    for file_meta in source:
        if file_meta.file_name == file_name:
            for column in file_meta.columns:
                if column.nullable is True:
                    notnull.append(column.name)

            return notnull  
    return notnull

############################################

def column_format(file_name, type):
    source = metadata_settings.batch if type == 'batch' else metadata_settings.stream
    format = {}

    for file_meta in source:
        if file_meta.file_name == file_name:
            for column in file_meta.columns:
                if column.format:
                    format[column.name] = column.format

            return format  
    return format

############################################

def range(file_name, type):
    source = metadata_settings.batch if type == 'batch' else metadata_settings.stream
    range = {}

    for file_meta in source:
        if file_meta.file_name == file_name:
            for column in file_meta.columns:
                if column.range:
                    range[column.name] = column.range

            return range  
    return range

############################################
