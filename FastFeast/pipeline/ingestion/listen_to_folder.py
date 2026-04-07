import shutil
import time
from pathlib import Path
from FastFeast.support.logger import pipeline as log

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
def copy_files(source_today, dest_today):

    try:
        #print("HELOOOOOOOOOOOOOO WE ARE HERE")
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

        #print(f"Successfully copied contents from '{src_path}' to '{dest_path}'.")

    except Exception as e:
        print(f"Error: {e}")

    return dest_today
    
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