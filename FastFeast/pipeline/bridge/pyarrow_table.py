#############################################################
# here we will convert all ingested files into pyarrow table
# convert NaN into Null
#############################################################

import json
import math
from pathlib import Path
import pyarrow as pa
import pyarrow.csv as pv
from FastFeast.pipeline.config.config import get_config

#############################################################

# Config 
config = get_config()

#############################################################

# Clean json files from NaN value
def clean_value(value, target_type):
    """
    - Handle NaN values
    """

    # Handle None
    if value is None:
        return None

    # Handle NaN
    if isinstance(value, float) and math.isnan(value):
        return None

#############################################################

# Return json as PyArrow tables
def json_value(file_path):
    """
    - Return NaN in files as a string or integer depending on its datatype in the column

    """
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

#############################################################

# Convert cleaned file to PYArrow table
def load_file(file_name):
    """
    - convert cleaned file from json/csv into PyArrow Table 

    """
    file_name = str(file_name)

    # for file_name in file_path:
    file_csv = file_name.lower().endswith('.csv')
    file_json = file_name.lower().endswith('.json')

    if file_csv:
        return pv.read_csv(str(file_name))

    elif file_json:
        return json_value(file_name)

    else:
        print(f"Unsupported file type")
        return None

#############################################################


# result = load_file("FastFeast/input_data/today/customers.csv")
# print(result)