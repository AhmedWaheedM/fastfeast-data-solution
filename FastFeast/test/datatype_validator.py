import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
import time

from pathlib import Path
from typing import Dict

from FastFeast.pipeline.config.metadata import metadata_settings
from FastFeast.utilities.file_utils import get_file_metadata
from FastFeast.test.regex_map import validate_with_regex


# =========================================================
# Type Mapping
# =========================================================
def _map_type_to_pyarrow(type_str: str) -> pa.DataType:
    mapping = {
        'integer': pa.int64(),
        'varchar': pa.string(),
        'float': pa.float64(),
        'boolean': pa.bool_(),
        'timestamp': pa.timestamp('us'),
        'date': pa.date32(),
    }
    return mapping.get(type_str)


# =========================================================
# Datatype Validation
# =========================================================
def _validate_column(col, target_type):
        
        if not pa.types.is_string(col.type):
            col_str = pc.cast(col, pa.string())
        else:
            col_str = col

        col_clean = pc.ascii_trim_whitespace(col_str) #remove any spces to can detect Nulls

        was_null = pc.fill_null(
        pc.or_(
            pc.is_null(col),
            pc.equal(col_clean, "")
        ),
        True
    )# return True for Null
        #print("WASSS NULLLLLLL", was_null)

        try:
            casted = pc.cast(col_clean, target_type, safe=True)
            is_now_null = pc.is_null(casted)

            datatype_invalid = pc.and_(
                pc.invert(was_null),
                is_now_null
            )

        except Exception:
            col_clean_final = pc.cast(col_clean, pa.string())
            regex_valid = validate_with_regex(col_clean_final, target_type)

            datatype_invalid = pc.and_(
                pc.invert(was_null),
                pc.invert(regex_valid)
            )

    # NULL handling (consider NULL invalid here)
        null_invalid = was_null

        final_invalid = pc.or_(datatype_invalid, null_invalid)
        valid_mask = pc.invert(final_invalid)

        return final_invalid, was_null, valid_mask



# file_path = r"FastFeast/input_data/2026-03-30/drivers.csv"
# import pyarrow.csv as pv

# table = pv.read_csv(file_path,
#     read_options=pv.ReadOptions(autogenerate_column_names=False),
#     parse_options=pv.ParseOptions(delimiter=","),
# )
# typee =  _map_type_to_pyarrow("varchar")
# result = _validate_column(table["driver_phone"],typee)
# print("33333333333333333333333333333333333333333333333333333",result)


def _compute_valid_mask_vectorized(table: pa.Table, expected_types: dict, run_id: str):

    masks = {}
    null_masks = {}
    invalid_masks = {}

    for col_name, target_type in expected_types.items():

        if col_name not in table.column_names:
            raise ValueError(f"Missing column: {col_name}")

        col = table[col_name]

        invalid_mask, was_null, valid_mask = _validate_column(col, target_type)

        masks[col_name] = valid_mask
        null_masks[col_name] = was_null
        invalid_masks[col_name] = invalid_mask

    # Combine datatype masks (AND across columns)
    final_mask = None
    for m in masks.values():
        final_mask = m if final_mask is None else pc.and_(final_mask, m)

    return final_mask, null_masks, invalid_masks


# expected_types = {
#     "driver_phone": "varchar",
#     "driver_name": "varchar"
# }

# final_mask, null_masks, invalid_masks = _compute_valid_mask_vectorized(
#     table,
#     expected_types,
#     "001"
# )

# final_mask, null_masks, invalid_masks = _compute_valid_mask_vectorized(
#     table,
#     expected_types,
#     "001"
# )


# print(null_masks["driver_phone"].to_pylist())

# =========================================================
# Business Validation Rules
# =========================================================
def _build_rule_masks(table: pa.Table, rules: Dict):

    masks = {}
    reasons = {}

    for col, rule in rules.items():

        if col not in table.column_names:
            continue

        col_data = table[col]

        if rule["type"] == "email":
            valid = validate_with_regex(col_data, "email")

        elif rule["type"] == "range":
            valid = pc.and_(
                pc.greater_equal(col_data, rule["min"]),
                pc.less_equal(col_data, rule["max"])
            )

        elif rule["type"] == "phone":
            valid = validate_with_regex(col_data, "phone")

        elif rule["type"] == "datetime":
            valid = validate_with_regex(col_data, "datetime")

        else:
            valid = pa.array([True] * table.num_rows)

        masks[col] = valid
        reasons[col] = rule["type"]

    return masks, reasons


# =========================================================
# Main Validation Pipeline
# =========================================================
def validate_pipeline(table: pa.Table, file_path: str, run_id: str, metadata_map):

    file_name = Path(file_path).name

    fm = get_file_metadata(metadata_map, file_name)

    if fm is None:
        raise ValueError(f"Unknown file: {file_name}")

    # -------------------------
    # Expected types
    # -------------------------
    expected_types = {
        col.name: _map_type_to_pyarrow(col.type)
        for col in fm.columns
    }

    # -------------------------
    # Datatype Validation
    # -------------------------
    dt_mask, null_masks, invalid_masks = _compute_valid_mask_vectorized(
        table,
        expected_types,
        run_id
    )

    valid_dt_table = table.filter(dt_mask)
    invalid_dt_table = table.filter(pc.invert(dt_mask))

    # print("Datatype valid rows:", valid_dt_table.num_rows)
    # for col, null in null_masks.items():
    #     count = pc.sum(null).as_py()
    #     print(col,":",count)

    # -------------------------
    # Business Validation
    # -------------------------
    rules = {
        "email": {"type": "email"},
        "age": {"type": "range", "min": 0, "max": 120},
        "phone": {"type": "phone"},
        "created_at": {"type": "datetime"},
        "category_id": {"type": "integer"},
        "category_name": {"type": "varchar"}
    }

    rule_masks, rule_reasons = _build_rule_masks(valid_dt_table, rules)

    # Combine business masks
    business_mask = None
    for m in rule_masks.values():
        business_mask = m if business_mask is None else pc.and_(business_mask, m)

    # -------------------------
    # Build row-level reasons
    # -------------------------
    reasons_list = []

    for i in range(valid_dt_table.num_rows):
        row_reasons = []

        for col, mask in rule_masks.items():
            if not mask[i].as_py():
                row_reasons.append(f"{col}: invalid ({rule_reasons[col]})")

        reasons_list.append(", ".join(row_reasons) if row_reasons else None)

    # Attach reasons column
    valid_dt_table = valid_dt_table.append_column(
        "validation_errors",
        pa.array(reasons_list)
    )

    # -------------------------
    # Final split
    # -------------------------
    final_valid = valid_dt_table.filter(business_mask)
    final_invalid = valid_dt_table.filter(pc.invert(business_mask))

    return final_valid, final_invalid, null_masks





metadata_map = metadata_settings

# final_valid, final_invalid, null_masks = validate_pipeline(
#     table,
#     file_path,
#     "run_id",
#     metadata_map
# )

# print(null_masks)

# for col, mask in null_masks.items():
#     count = pc.sum(mask).as_py()
#     print(col, ":", count)