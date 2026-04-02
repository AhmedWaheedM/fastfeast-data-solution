import pyarrow.csv as pv
import time
import pyarrow.compute as pc
from FastFeast.test.columns_validator import valid_columns
from FastFeast.test.null_validator import validate_nulls
from FastFeast.test.email_validator import validate_email_format
from FastFeast.test.phone_validator import validate_phone_format
from FastFeast.test.date_validator import validate_date_format
from FastFeast.test.numeric_ranges_validator import validate_numeric_ranges
from FastFeast.test.expected_values_validator import validate_expected_values
from FastFeast.test.pii_handling import handle_pii
from FastFeast.test.profiling import profile_table

from FastFeast.test.datatype_validator import validate_pipeline
from FastFeast.test.all_validation_mask import build_business_validation_mask

from FastFeast.utilities.file_utils import build_metadata_map
# from FastFeast.test.datatype_validator import validate_all_in_one_pass
from FastFeast.pipeline.config.metadata import metadata_settings

#       file_path = r"FastFeast/input_data/2026-03-30/customers.csv"
#       table = pv.read_csv(file_path)
#       if valid_columns(table, file_path, "test_run_001"):
#     # profile_table = profile_table(table, file_path, "test_run_001")
#     # print(profile_table)
#     valid_rows, invalid_rows = validate_data_types(table, file_path, "test_run_001")
#     valid_rows, invalid_rows = validate_nulls(table, file_path, "test_run_001")
#     # valid_rows, invalid_rows = validate_email_format(table, file_path, "test_run_001")
#     # valid_rows, invalid_rows = validate_phone_format(table, file_path, "test_run_001")
#     # valid_rows, invalid_rows = validate_date_format(table, file_path, "test_run_001")
#     # valid_rows, invalid_rows = validate_numeric_ranges(table, file_path, "test_run_001")
#     # valid_rows, invalid_rows = validate_expected_values(table, file_path, "test_run_001")
#     masked, audit = handle_pii(table, file_path, "test_run_001")
#     print("🎀Valid Rows:🎀")
#     print(valid_rows)
#     print("\n🎀Invalid Rows:🎀")
#     print(invalid_rows)


###################################################
# TEST
###################################################

if __name__ == "__main__":

    total_start = time.perf_counter()

    file_path = r"FastFeast/input_data/2026-03-30/categories.csv"

    print("Reading CSV...")
    read_start = time.perf_counter()
    table = pv.read_csv(file_path)
    read_end = time.perf_counter()

    print(f"CSV Read Time: {read_end - read_start:.4f} seconds")

    metadata_map = metadata_settings  # assuming already built

    print("\nRunning validation...")

    valid_rows, invalid_rows, null_masks = validate_pipeline(
        table,
        file_path,
        "test_run_001",
        metadata_map
    )

    # print("NULL MASKS:", len(null_masks))
    # print("Datatype + Business invalid rows:", invalid_rows.num_rows)

    for col, mask in null_masks.items():
        count = pc.sum(mask).as_py()
        print(col, ":", count)