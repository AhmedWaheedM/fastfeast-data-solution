# pii_handling.py
import pyarrow as pa
import pyarrow.compute as pc
from typing import Tuple, Optional, List, Dict, Any
from pathlib import Path
import hashlib
import json
from FastFeast.pipeline.config.metadata import metadata_settings
from pipeline.logger import validation as log
from FastFeast.utilities.file_utils import get_config_source, get_file_metadata


def _enrich_with_run_id(table: pa.Table, run_id: str) -> Optional[pa.Table]:
    """Add a run_id column to the table."""
    if table is None or len(table) == 0:
        return None
    run_id_col = pa.array([run_id] * len(table), type=pa.string())
    return table.append_column("PIPELINE_RUN_ID", run_id_col)


def _hash_string(s: str) -> str:
    """Return SHA‑256 hash of a string."""
    return hashlib.sha256(s.encode()).hexdigest()


def _apply_pii_masking_vectorized(
    table: pa.Table,
    pii_columns: List[Dict[str, Any]],
    strategy: str = "hash",
) -> pa.Table:
    """
    Apply masking to PII columns. Returns a new table with those columns transformed.
    Strategy can be:
        - "hash": replace with SHA‑256 hash of the string representation.
        - "redact": replace with '***'.
        - "partial": (not implemented yet) e.g., email -> a***@b.com.
    """
    col_mapping = {}
    for col_info in pii_columns:
        col_name = col_info["name"]
        col = table.column(col_name)

        # Convert to string for uniform handling
        col_str = pc.cast(col, pa.string(), safe=False)
        null_mask = pc.is_null(col_str)

        if strategy == "hash":
            # Compute hash for each non‑null string
            # pyarrow does not have a built‑in hash function; we need a Python UDF,
            # but that would be slow. Instead, we can use a compute function like `hash`
            # but it's not available in pyarrow compute. Alternative: use Python map.
            # To stay vectorized, we'll use `pc.call_function` if available, but simpler:
            # iterate through arrays? Not vectorized. Better to use `pc.ascii_upper`? Not.
            # For now, we'll implement using Python scalar mapping, but we'll warn.
            # In production, we could use `pc.utf8_slice` and custom logic, but it's complex.
            # We'll fall back to Python mapping for simplicity.
            # Let's create a new array by applying hash function.
            def hash_string(s):
                if s is None:
                    return None
                return hashlib.sha256(s.encode()).hexdigest()

            # Using numpy/pandas might be easier, but we'll keep it pure Python for now.
            # This is not vectorized but works.
            values = col_str.to_pylist()
            masked_vals = [hash_string(v) if v is not None else None for v in values]
            masked_arr = pa.array(masked_vals, type=pa.string())
        elif strategy == "redact":
            # Replace non‑null with '***'
            redacted_str = pc.replace_substring_regex(col_str, r".*", "***")
            masked_arr = pc.if_else(pc.invert(null_mask), redacted_str, pa.scalar(None))
        else:
            raise ValueError(f"Unsupported masking strategy: {strategy}")

        col_mapping[col_name] = masked_arr

    # Create a new table with masked columns replacing originals
    columns = []
    for col_name in table.column_names:
        if col_name in col_mapping:
            columns.append(col_mapping[col_name])
        else:
            columns.append(table.column(col_name))
    return pa.Table.from_arrays(columns, names=table.column_names)


def _extract_raw_pii(
    table: pa.Table,
    pii_columns: List[Dict[str, Any]],
    run_id: str,
) -> Optional[pa.Table]:
    """
    Create an audit table containing only raw PII columns plus run_id.
    """
    if not pii_columns:
        return None

    # Select only PII columns
    col_names = [col["name"] for col in pii_columns]
    # Ensure they exist
    existing = [c for c in col_names if c in table.column_names]
    if not existing:
        return None

    pii_table = table.select(existing)
    # Enrich with run_id
    pii_table = _enrich_with_run_id(pii_table, run_id)
    return pii_table


def handle_pii(
    table: pa.Table,
    file_path: str,
    run_id: str,
    strategy: str = "hash",
) -> Tuple[Optional[pa.Table], Optional[pa.Table]]:
    """
    Handle PII columns: apply masking and return masked table plus audit table.
    Returns (masked_table, audit_table).
    """
    # Get metadata for the file
    metadata = get_file_metadata(
        get_config_source(file_path), Path(file_path).name
    )

    # Collect columns with pii: true
    pii_columns = []
    for col in metadata.columns:
        if hasattr(col, "pii") and col.pii is True:
            pii_columns.append({"name": col.name})

    if not pii_columns:
        # No PII columns – just return the original table (with run_id) and None for audit
        return _enrich_with_run_id(table, run_id), None

    # Apply masking
    masked_table = _apply_pii_masking_vectorized(table, pii_columns, strategy)
    # Enrich masked table with run_id
    masked_table = _enrich_with_run_id(masked_table, run_id)

    # Create audit table with raw PII
    audit_table = _extract_raw_pii(table, pii_columns, run_id)

    log.info(
        f"PII handling applied to {len(pii_columns)} columns in {file_path} "
        f"using strategy '{strategy}'. run_id={run_id}"
    )

    return masked_table, audit_table