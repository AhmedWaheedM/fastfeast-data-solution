from typing import List, Optional, Any
from FastFeast.pipeline.config.metadata import metadata_settings

def get_config_source(file_path: str) -> Optional[List[Any]]:
    """Determine the configuration source based on the file path."""
    path_str = file_path.lower()
    if "batch" in path_str:
        return metadata_settings.batch
    elif "stream" in path_str:
        return metadata_settings.stream
    return None

def get_file_metadata(file_list: List[Any], file_name: str) -> Optional[Any]:
    """
    Retrieve metadata specific to a given file name.
    """
    return next((fm for fm in file_list if fm.file_name == file_name), None)


