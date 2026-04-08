from typing import Dict, List, Optional, Any
import hashlib
import time
from pathlib import Path
from FastFeast.pipeline.config.metadata import Settings, FileMeta
from FastFeast.pipeline.config.config import config_settings
#from pipeline.logger import logging


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
def get_file_metadata(metadata_settings, file_name: str):
    """
    Search for FileMeta inside batch and stream lists
    """

    # search in batch
    for file_meta in metadata_settings.batch:
        if file_meta.file_name == file_name:
            return file_meta

    # search in stream
    for file_meta in metadata_settings.stream:
        if file_meta.file_name == file_name:
            return file_meta

    return None



#Determine whether a file belongs to batch or stream based on its path
#Returns 'batch', 'stream', or None
def get_config_source(file_path: str, settings: Settings) -> Optional[str]:
    path_str = file_path.lower()

    if "batch" in path_str:
        return "batch"
    elif "stream" in path_str:
        return "stream"

    return None

def get_file_hash(file_path):
    sha = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha.update(chunk)
    return sha.hexdigest()

def wait_for_file(file_path, timeout_sec=config_settings.pipeline.time_wait):
    path = Path(file_path)
    start = time.time()
    while not path.exists():
        if time.time() - start > timeout_sec:
            return False
        time.sleep(1)
    return True


def resolve_silver_name(file_name: str) -> str:
    """Map an input filename to its canonical Silver table name."""
    return f"SILVER_{Path(file_name).stem.upper()}"
