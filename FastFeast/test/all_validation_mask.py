import pyarrow as pa
import pyarrow.compute as pc
from typing import Dict, Any
from FastFeast.test.regex_map import validate_with_regex


# -------------------------------
# Regex helpers
# -------------------------------
def regex_match(col, pattern):
    return pc.match_substring_regex(col, pattern)


# -------------------------------
# Column Validators (vectorized)
# -------------------------------
def validate_email(col):
    return regex_match(col, r'^[^@]+@[^@]+\.[^@]+$')


def validate_phone(col):
    return regex_match(col, r'^\+?\d{10,15}$')


def validate_date(col):
    col_str = pc.cast(col, pa.string())
    return validate_with_regex(col_str, "date")


def validate_timestamp(col):
    col_str = pc.cast(col, pa.string())
    return validate_with_regex(col_str, "timestamp")


def validate_boolean(col):
    col_str = pc.cast(col, pa.string())
    return validate_with_regex(col_str, "boolean")


def validate_string(col):
    # valid if NOT empty
    return pc.invert(pc.equal(col, pa.scalar('')))


def validate_float(col):
    col_str = pc.cast(col, pa.string())
    return validate_with_regex(col_str, "float")


def validate_integer(col):
    col_str = pc.cast(col, pa.string())
    return validate_with_regex(col_str, "integer")


def validate_integer_range(col, min_val=None, max_val=None):
    col_int = pc.cast(col, pa.int64())

    mask = pc.scalar(True)

    if min_val is not None:
        mask = pc.and_(mask, pc.greater_equal(col_int, min_val))

    if max_val is not None:
        mask = pc.and_(mask, pc.less_equal(col_int, max_val))

    return mask


# -------------------------------
# Validator registry (clean mapping)
# -------------------------------
VALIDATOR_MAP = {
    "email": validate_email,
    "phone": validate_phone,
    "date": validate_date,
    "datetime": validate_timestamp,
    "bool": validate_boolean,
    "string": validate_string,
    "float": validate_float,
    "integer": validate_integer,
}


# -------------------------------
# Main Engine (column-wise vectorized)
# -------------------------------
def build_business_validation_mask(
    table: pa.Table,
    rules: Dict[str, Dict[str, Any]]
) -> pa.Array:

    column_masks = []

    for col_name, rule in rules.items():

        if col_name not in table.column_names:
            continue

        col = table[col_name]
        rule_type = rule.get("type")

        # -------------------------
        # Select validator
        # -------------------------
        mask = None

        if rule_type == "range":
            mask = validate_integer_range(
                col,
                rule.get("min"),
                rule.get("max")
            )
        else:
            validator = VALIDATOR_MAP.get(rule_type)
            if validator:
                mask = validator(col)

        if mask is None:
            continue

        # -------------------------
        # Null handling (optional strict mode)
        # -------------------------
        not_null = pc.invert(pc.is_null(col))

        # final column mask:
        # valid = not null AND passes validation
        col_valid_mask = pc.and_(not_null, mask)

        column_masks.append(col_valid_mask)

    # -------------------------
    # Combine all column masks (AND across columns)
    # -------------------------
    if not column_masks:
        return pa.array([True] * table.num_rows)

    final_mask = column_masks[0]
    for m in column_masks[1:]:
        final_mask = pc.and_(final_mask, m)

    return final_mask