import shutil
import time
from pathlib import Path
from datetime import datetime
import os
import pyarrow.csv as pv
from FastFeast.support.logger import pipeline as log
from FastFeast.utilities.db_utils import get_connection
from FastFeast.pipeline.config.config import get_config
from FastFeast.pipeline.bridge.pyarrow_table import load_file
from FastFeast.utilities.metadata_cash import compare_columns
from FastFeast.pipeline.validation.schema_validation import validate_table
from FastFeast.utilities.validation_utils import expected_types, not_null_column, column_format, get_column_range, map_format_to_pattern, map_type_to_pyarrow, map_type_to_pattern


####################################################

# Get configuration
config = get_config()
date = config.datetime_handling.date_key_format

BASE_DIR = Path(__file__).resolve().parents[3]

source = BASE_DIR / config.paths.batch_dir
dest = BASE_DIR / config.paths.dest_base

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
    file_name = Path(dest / datetime.today().strftime(date) / "cities.json")
    file = "cities.json"
    type = "batch"

    pa_table = load_file(file_name)
    print(pa_table.schema)

    if compare_columns(pa_table, 'batch'):

        # expected_types (map to pyarrow types), expected_patterns, expected_not_nullable, expected_format, expected_range
        # validate_table(pa_table, expected_types, expected_patterns, expected_not_nullable, expected_format,expected_range)

        expected_types_str = expected_types(file, type)
        expected_types_pa = map_type_to_pyarrow(expected_types_str)
        not_null = not_null_column(file, type)
        expected_formats_str = column_format(file, type)
        expected_formats = map_format_to_pattern(expected_formats_str)
        column_range = get_column_range(file, type)
        expected_pattern = map_type_to_pattern(expected_types_str)

        status_list, error_lists, retry_list = validate_table(pa_table, expected_types_pa, expected_pattern, not_null, expected_formats, column_range)

        return status_list, error_lists, retry_list
    
####################################################

# if __name__ == '__main__':

#     conn = get_connection()
#     BASE_DIR = Path(__file__).resolve().parents[3]

#     source_file = BASE_DIR / "data" / "input" / "batch" / "2026-04-05"

#     dest_file = BASE_DIR / "FastFeast" / "input_data" / "batch" / "2026-04-05"


#     #print(source_file)

#     copy_files(source_file, dest_file, "002", conn)
#     status_list, error_lists, retry_list = process_single_file()
#     print("💖status_list💖  ", status_list)
#     print("🎶error_lists🎶  ", error_lists)
#     print("🦄retry_list🦄   ", retry_list)
#     print("😃💖🐱‍👤😘")