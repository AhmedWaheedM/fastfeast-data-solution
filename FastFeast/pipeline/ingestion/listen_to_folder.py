import shutil
import time
from pathlib import Path
import os
import pyarrow.csv as pv
from FastFeast.support.logger import pipeline as log
from FastFeast.pipeline.config.config import get_config
from FastFeast.pipeline.bridge.pyarrow_table import load_file
from FastFeast.utilities.metadata_cash import compare_columns


####################################################

# Get configuration
config = get_config()

BASE_DIR = Path(__file__).resolve().parents[3]

SOURCE_BASE = BASE_DIR / config.paths.batch_dir
DEST_BASE = BASE_DIR / config.paths.dest_base

####################################################

# Wait for file
def wait_for_file(file_path, timeout_sec=60):
    path = Path(file_path)
    start = time.time()

    while not path.exists():
        #log("File not exist, retry in 30 seconds...")
        if time.time() - start > timeout_sec:
            return False
        time.sleep(1)

    return True
####################################################

# Copy files from source to distnation
def copy_files(source_today, dest_today, run_id , conn):

    try:
        # Convert to Path objects for easier handling
        src_path = Path(source_today).resolve()
        dest_path = Path(dest_today).resolve()

        # Validate source directory
        if not src_path.exists() or not src_path.is_dir():
            raise FileNotFoundError(f"Source directory '{src_path}' does not exist or is not a directory.")

        # Create destination directory if it doesn't exist
        dest_path.mkdir(parents=True, exist_ok=True)

        # Iterate through all items in the source directory
        for item in src_path.iterdir():
            src_item = item
            dest_item = dest_path / item.name
            shutil.copy2(src_item, dest_item)  # copy2 preserves metadata

        print(f"Successfully copied contents from '{src_path}' to '{dest_path}'.")

    except Exception as e:
        print(f"Error: {e}")

    return dest_today
    
####################################################

#Process single file
def process_single_file():
    """
    - This function takes every process we need to do in one file
    - Then we will pass this to Thread function, to perform all operations on all files
    """
    #pa_table = ''
    if compare_columns(load_file("FastFeast/input_data/today/customers.csv"), 'batch'):
        
        # expected_types (map to pyarrow types), expected_patterns, expected_not_nullable, expected_format, expected_range
        # validate_table(pa_table, expected_types, expected_patterns, expected_not_nullable, expected_format,expected_range)

        return True
    
####################################################
