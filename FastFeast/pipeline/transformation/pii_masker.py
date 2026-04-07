from logging import info

import pyarrow as pa
import pyarrow.compute as pc
from support.logger import pipeline as log  

def mask_pii(table: pa.Table, table_name: str, pii_config: dict) -> pa.Table:
    """
    Applies vectorized PII masking to a PyArrow table.
    """
    if not pii_config:
        return table  # No PII config, return original table
    log.info(f"Applying PII masking to table: {table_name}..", columns=list(pii_config.keys()))
    for col_name, mask_type in pii_config.items():
        if col_name not in table.column_names:
            log.warning(f"PII column '{col_name}' defined in config but missing from {table_name}.")            
            continue
        col_index = table.get_schema().get_field_index(col_name)
        try:
            if mask_type == 'redact':
                redacted_array = pa.array(['[REDACTED]'] * table.num_rows, type=pa.string())
                table = table.set_column(col_index, col_name, redacted_array)

            elif mask_type == 'partial_email':
                masked_email = pc.replace_substring_regex(
                    table[col_name], 
                    pattern='^[^@]+', 
                    replacement='***'
                )
                table = table.set_column(col_index, col_name, masked_email)

            elif mask_type == 'partial_prefix_3':
                # The Vodafone Trick: Keep first 3 chars, turn the rest into 8 asterisks
                masked_phone = pc.replace_substring_regex(
                    table[col_name],
                    pattern='(?<=^.{3}).*', 
                    replacement='********'
                )
                table = table.set_column(col_index, col_name, masked_phone)

            elif mask_type == 'last_four':
                masked_cc = pc.replace_substring_regex(
                    table[col_name],
                    pattern='.(?=.{4})', 
                    replacement='*'
                )
                table = table.set_column(col_index, col_name, masked_cc)

            elif mask_type == 'hash':
                # DEFER TO DUCKDB: Do absolutely nothing here.
                log.debug(f"Bypassing PyArrow for '{col_name}', deferring hash to DuckDB.")
                continue

            else:
                log.warning(f"Unknown PII mask type '{mask_type}' for column {col_name}.")

        except Exception as e:
            log.error(f"Failed to mask PII column {col_name}", error=str(e))
            raise

    return table
