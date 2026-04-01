from typing import Dict, List, Optional, Any
from FastFeast.pipeline.config.metadata import Settings
from FastFeast.pipeline.config.metadata import FileMeta
from pipeline.logger import logging


##########################################################
# Heree we need to add log
# Need to depend on file_hash , not just nanem incase changing on this
# Need to get run_id, file_path and others from file tracking
##########################################################

########## insted of loop many times to get all information from the disk, cash meta in memory
def build_metadata_map(settings: Settings) -> Dict[str, FileMeta]: 
    all_files = settings.batch + settings.stream

    metadata_map: Dict[str, FileMeta] = {}

    for fm in all_files:
        if fm.file_name in metadata_map:
            raise ValueError(f"Duplicate file_name found in metadata: {fm.file_name}")

        metadata_map[fm.file_name] = fm

    return metadata_map



#Retrieve metadata for a given file name
#Returns None if the file is not defined in metadata
def get_file_metadata(metadata_map: Dict[str, FileMeta], file_name: str) -> Optional[FileMeta]:
   return metadata_map.get(file_name)



#Determine whether a file belongs to batch or stream based on its path
#Returns 'batch', 'stream', or None
def get_config_source(file_path: str, settings: Settings) -> Optional[str]:
    path_str = file_path.lower()

    if "batch" in path_str:
        return "batch"
    elif "stream" in path_str:
        return "stream"

    return None