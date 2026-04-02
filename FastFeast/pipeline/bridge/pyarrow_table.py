#############################################################
# here we will convert all ingested files into pyarrow table
# convert NaN into Null
#############################################################

import json
import math
from pathlib import Path
from datetime import datetime
import pyarrow as pa
import pyarrow.csv as pv
from FastFeast.pipeline.config.config import get_config

# ------------------------------------------------------
# Config & Paths
# ------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[3]

#SOURCE_BASE = BASE_DIR / "data" / "input" / "batch"

config = get_config()


# ------------------------------------------------------
# Clean helper
# ------------------------------------------------------
def clean_value(value, target_type):
    # Handle None
    if value is None:
        return None

    # Handle NaN
    if isinstance(value, float) and math.isnan(value):
        return None

    # Type enforcement
    if target_type == "string":
        return str(value)

    if target_type == "float":
        try:
            return float(value)
        except:
            return None

    return value


# ------------------------------------------------------
# Return json as PyArrow tables
# ------------------------------------------------------
def json_to_arrow_table(file_path: Path) -> pa.Table:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)  # Accecpt Json aray

    # Main columns cause all problems
    string_columns = ["restaurant_name"]
    float_columns = ["rating_avg"]

    cleaned_data = []

    for row in data:
        cleaned_row = {}

        for key, value in row.items():

            if key in string_columns:
                cleaned_row[key] = clean_value(value, "string")

            elif key in float_columns:
                cleaned_row[key] = clean_value(value, "float")

            else:
                cleaned_row[key] = value

        cleaned_data.append(cleaned_row)

    return pa.Table.from_pylist(cleaned_data)


# ------------------------------------------------------
# File Loader Dispatcher
# ------------------------------------------------------
def load_file(file_path: Path) -> pa.Table:
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return pv.read_csv(str(file_path))

    elif suffix == ".json":
        return json_to_arrow_table(file_path)

    else:
        print(f"Unsupported file type: {suffix}")
        return None


# ------------------------------------------------------
# Main Execution
# ------------------------------------------------------
# today = datetime.now().date()
# today_folder_name = today.strftime(config.datetime_handling.date_key_format)

#source_today = SOURCE_BASE / today_folder_name


# if __name__ == "__main__":
#     if not dest_today.exists():
#         print("No folder found:", dest_today)
#         exit()

#     for file_path in dest_today.iterdir():

#         if file_path.is_file():
#             print(f"Processing: {file_path.name}")

#             table = load_file(file_path)

#             if table is None:
#                 continue

#             print("Schema:", table.schema)
#             print("Rows:", table.num_rows)
#             print(table.to_pandas().head())
#             print("-" * 50)

#         else:
#             print("There is no files")