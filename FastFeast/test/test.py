import pyarrow.csv as pv
from FastFeast.test.columns_validator import valid_columns
from FastFeast.test.datatype_validator import validate_data_types
from FastFeast.test.profiling import profile_table
import json


file_path = r"FastFeast/data/input/batch/2026-03-28/customers.csv"
table = pv.read_csv(file_path)
if valid_columns(table, file_path, "test_run_001"):
    profile_table = profile_table(table, file_path, "test_run_001")
    print(profile_table)
    # valid_rows, invalid_rows = validate_data_types(table, file_path, "test_run_001")
    # # print("🧘‍♂️🧘‍♂️🧘‍♂️Valid Rows:🧘‍♂️🧘‍♂️🧘‍♂️")
    # # print(valid_rows)
    # # print("\n🧘‍♂️🧘‍♂️🧘‍♂️Invalid Rows:🧘‍♂️🧘‍♂️🧘‍♂️")
    # # print(invalid_rows)
    