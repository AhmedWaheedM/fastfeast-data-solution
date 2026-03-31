import pyarrow.csv as pv
from FastFeast.test.columns_validator import valid_columns
from FastFeast.test.datatype_validator import validate_data_types
from FastFeast.test.null_validator import validate_nulls
from FastFeast.test.email_validator import validate_email_format
from FastFeast.test.phone_validator import validate_phone_format
from FastFeast.test.date_validator import validate_date_format
from FastFeast.test.numeric_ranges_validator import validate_numeric_ranges
from FastFeast.test.expected_values_validator import validate_expected_values
from FastFeast.test.pii_handling import handle_pii
from FastFeast.utilities.file_utils import get_file_metadata, get_config_source
from FastFeast.test.profiling import profile_table
import json


file_path = r"FastFeast/data/input/batch/2026-03-28/drivers.csv"
table = pv.read_csv(file_path)
if valid_columns(table, file_path, "test_run_001"):
    # profile_table = profile_table(table, file_path, "test_run_001")
    # print(profile_table)
    #valid_rows, invalid_rows = validate_data_types(table, file_path, "test_run_001")
    #valid_rows, invalid_rows = validate_nulls(table, file_path, "test_run_001")
    # valid_rows, invalid_rows = validate_email_format(table, file_path, "test_run_001")
    # valid_rows, invalid_rows = validate_phone_format(table, file_path, "test_run_001")
    # valid_rows, invalid_rows = validate_date_format(table, file_path, "test_run_001")
    # valid_rows, invalid_rows = validate_numeric_ranges(table, file_path, "test_run_001")
    # valid_rows, invalid_rows = validate_expected_values(table, file_path, "test_run_001")
    masked, audit = handle_pii(table, file_path, "test_run_001")
    print("🎀Valid Rows:🎀")
    print(masked)
    print("\n🎀Invalid Rows:🎀")
    print(audit)
    