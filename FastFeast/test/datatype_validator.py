import pyarrow as pa
import pyarrow.compute as pc
import time
from pathlib import Path
from typing import Dict

from FastFeast.pipeline.config.metadata import metadata_settings
from FastFeast.utilities.file_utils import get_file_metadata
from FastFeast.test.regex_map import validate_with_regex
from FastFeast.support.logger import pipeline as log


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

    # Ensure string for processing
    if not pa.types.is_string(col.type):
        col_str = pc.cast(col, pa.string())
    else:
        col_str = col

    col_clean = pc.ascii_trim_whitespace(col_str)

    # Null detection (clean version)
    was_null = pc.or_(
        pc.is_null(col),
        pc.equal(col_clean, "")
    )

    try:
        casted = pc.cast(col_clean, target_type, safe=True)
        is_now_null = pc.is_null(casted)

        datatype_invalid = pc.and_(
            pc.invert(was_null),
            is_now_null
        )

    except Exception:
        # fallback regex validation
        col_clean_final = pc.cast(col_clean, pa.string())
        regex_valid = validate_with_regex(col_clean_final, target_type)

        datatype_invalid = pc.and_(
            pc.invert(was_null),
            pc.invert(regex_valid)
        )

    null_invalid = was_null

    final_invalid = pc.or_(datatype_invalid, null_invalid)
    valid_mask = pc.invert(final_invalid)

    return final_invalid, was_null, valid_mask


# =========================================================
# Compute Datatype Masks
# =========================================================
def _compute_valid_mask_vectorized(table: pa.Table, expected_types: dict, run_id: str):

    masks = {}
    null_masks = {}
    invalid_masks = {}

    log.info("Starting datatype validation", run_id=run_id)

    for col_name, target_type in expected_types.items():

        if col_name not in table.column_names:
            log.warning("Missing column, skipping", column=col_name)
            continue

        if target_type is None:
            log.warning("Unknown type mapping, skipping", column=col_name)
            continue

        col = table[col_name]

        log.debug("Validating column", column=col_name, type=str(target_type))

        invalid_mask, was_null, valid_mask = _validate_column(col, target_type)

        masks[col_name] = valid_mask
        null_masks[col_name] = was_null
        invalid_masks[col_name] = invalid_mask

    # Combine masks safely
    final_mask = None

    for name, m in masks.items():
        if final_mask is None:
            final_mask = m
        else:
            final_mask = pc.and_(final_mask, m)

    if final_mask is None:
        raise ValueError("No valid columns found for validation")

    # Normalize mask
    if isinstance(final_mask, pa.ChunkedArray):
        final_mask = final_mask.combine_chunks()

    final_mask = pa.array(final_mask, type=pa.bool_())

    # Debug info
    log.debug("Final mask stats",
              total_rows=table.num_rows,
              valid_rows=pc.sum(final_mask).as_py())

    return final_mask, null_masks, invalid_masks


# =========================================================
# Business Rules
# =========================================================
def _build_rule_masks(table: pa.Table, rules: Dict):

    masks = {}
    reasons = {}

    for col, rule in rules.items():

        if col not in table.column_names:
            log.debug("Rule column missing, skipping", column=col)
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
            valid = pa.array([True] * table.num_rows, type=pa.bool_())

        masks[col] = valid
        reasons[col] = rule["type"]

    return masks, reasons


# =========================================================
# Main Validation Pipeline
# =========================================================
def validate_pipeline(table: pa.Table, file_path: str, run_id: str, metadata_map):

    file_name = Path(file_path).name
    log.info("Running validation", file=file_name, run_id=run_id)

    fm = get_file_metadata(metadata_map, file_name)

    if fm is None:
        raise ValueError(f"Unknown file: {file_name}")

    # -------------------------
    # Expected types
    # -------------------------
    expected_types_raw = {
        col.name: _map_type_to_pyarrow(col.type)
        for col in fm.columns
    }

    expected_types = {
        k: v for k, v in expected_types_raw.items() if v is not None
    }

    log.debug("Expected types", types={k: str(v) for k, v in expected_types.items()})

    # -------------------------
    # Datatype Validation
    # -------------------------
    dt_mask, null_masks, invalid_masks = _compute_valid_mask_vectorized(
        table,
        expected_types,
        run_id
    )

    # Safety checks
    assert len(dt_mask) == table.num_rows, "DT mask length mismatch"

    valid_dt_table = table.filter(dt_mask)
    invalid_dt_table = table.filter(pc.invert(dt_mask))

    log.info("Datatype split",
             valid_rows=valid_dt_table.num_rows,
             invalid_rows=invalid_dt_table.num_rows)

    # -------------------------
    # Business Rules
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

    if business_mask is None:
        business_mask = pa.array([True] * valid_dt_table.num_rows, type=pa.bool_())

    # -------------------------
    # Row-level reasons
    # -------------------------
    reasons_list = []

    for i in range(valid_dt_table.num_rows):
        row_reasons = []

        for col, mask in rule_masks.items():
            if not mask[i].as_py():
                row_reasons.append(f"{col}: invalid ({rule_reasons[col]})")

        reasons_list.append(", ".join(row_reasons) if row_reasons else None)

    valid_dt_table = valid_dt_table.append_column(
        "validation_errors",
        pa.array(reasons_list)
    )

    # -------------------------
    # Final split
    # -------------------------
    final_valid = valid_dt_table.filter(business_mask)
    final_invalid = valid_dt_table.filter(pc.invert(business_mask))

    log.info("Final validation done",
             valid_rows=final_valid.num_rows,
             invalid_rows=final_invalid.num_rows)

    return final_valid, final_invalid, null_masks