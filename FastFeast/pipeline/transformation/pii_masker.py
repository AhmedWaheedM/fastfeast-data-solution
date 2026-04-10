from logging import info

import pyarrow as pa
import pyarrow.compute as pc
from FastFeast.support.logger import pipeline as log  
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
        col_index = table.schema.get_field_index(col_name)
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
                # RE2 Safe: Capture first 3 chars in group 1 (\1), replace the rest with asterisks
                masked_phone = pc.replace_substring_regex(
                    table[col_name],
                    pattern=r'^(.{3}).*', 
                    replacement=r'\1********'
                )
                table = table.set_column(col_index, col_name, masked_phone)

            elif mask_type == 'last_four':
                # RE2 Safe: Capture last 4 chars in group 1 (\1), replace the front with asterisks
                masked_cc = pc.replace_substring_regex(
                    table[col_name],
                    pattern=r'^.*(.{4})$', 
                    replacement=r'************\1'
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
