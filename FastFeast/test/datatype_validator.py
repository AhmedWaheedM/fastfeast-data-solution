import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv

from typing import Optional, Dict
from pathlib import Path
import time

from FastFeast.pipeline.config.metadata import metadata_settings, FileMeta
from pipeline.logger import validation as log

from FastFeast.utilities.file_utils import get_file_metadata
from FastFeast.test.regex_map import validate_with_regex


########################################################################################################################
def _map_type_to_pyarrow(type_str: str) -> pa.DataType:
    mapping = {
        'integer': pa.int64(),
        'varchar': pa.string(),
        'float': pa.float64(),
        'boolean': pa.bool_(),
        'timestamp': pa.timestamp('us'),
        'date': pa.date32(),
    }

    if type_str not in mapping:
        log.warning(f"Unknown type '{type_str}', defaulting to string")
        return pa.string()

    return mapping[type_str]
########################################################################################################################


########################################################################################################################
def _validate_column(col, target_type):
    was_null = pc.is_null(col)

    try:
        casted = pc.cast(col, target_type, safe=True)
        is_now_null = pc.is_null(casted)

        invalid = pc.and_(pc.invert(was_null), is_now_null)

    except Exception:
        col_str = pc.cast(col, pa.string(), safe=False)
        regex_valid = validate_with_regex(col_str, target_type)

        invalid = pc.and_(pc.invert(was_null), pc.invert(regex_valid))

    return invalid , was_null
########################################################################################################################


########################################################################################################################
def _compute_valid_mask_vectorized(table: pa.Table, expected_types: dict, run_id: str) -> pa.Array:

    num_rows = table.num_rows
    masks = []
    null_mask = {}

    for col_name, target_type in expected_types.items():

        if col_name not in table.column_names:
            log.error(f"Column '{col_name}' missing. run_id={run_id}")
            return pa.array([False] * num_rows, type=pa.bool_())

        col = table[col_name]
        invalid_in_col, was_null = _validate_column(col, target_type)

        valid_col_mask = pc.invert(invalid_in_col)
        masks.append(valid_col_mask)
        null_mask[col_name] = was_null

        # Logging (non-critical path)
        if pc.any(invalid_in_col).as_py():
            invalid_count = pc.sum(invalid_in_col.cast(pa.int64())).as_py()
            log.warning(f"{col_name}: {invalid_count} invalid rows (run_id={run_id})")

    # Combine all column masks once
    valid_mask = masks[0]
    for m in masks[1:]:
        valid_mask = pc.and_(valid_mask, m)

    return valid_mask , null_mask
########################################################################################################################


########################################################################################################################
def _validate_batch(batch: pa.RecordBatch, expected_types: dict, run_id: str):
    table = pa.Table.from_batches([batch])

    valid_mask, null_mask = _compute_valid_mask_vectorized(table, expected_types, run_id)

    valid_table = table.filter(valid_mask)
    invalid_table = table.filter(pc.invert(valid_mask))

    return valid_table, invalid_table, null_mask
########################################################################################################################


#########################################################################################################################
def _enrich_with_run_id(table: pa.Table, run_id: str) -> Optional[pa.Table]:
    if table is None or len(table) == 0:
        return None

    run_id_col = pa.array([run_id] * len(table), type=pa.string())
    return table.append_column('PIPELINE_RUN_ID', run_id_col)
#########################################################################################################################


########################################################################################################################
# metadata = build_metadata_map(metadata_settings)


def validate_data_types_batched(
    table: pa.Table,
    file_path: str,
    run_id: str,
    metadata_map: Dict[str, FileMeta]
):

    file_name = Path(file_path).name
    fm = get_file_metadata(metadata_map, file_name)

    if fm is None:
        raise ValueError(f"Unknown file: {file_name}")

    expected_types = {
        col.name: _map_type_to_pyarrow(col.type)
        for col in fm.columns
    }

    valid_batches = []
    invalid_batches = []
    all_null_mask = {}

    start_batches = time.perf_counter()

    for batch in table.to_batches():
        valid_table, invalid_table, null_mask = _validate_batch(batch, expected_types, run_id)

        if valid_table is not None:
            valid_batches.append(valid_table)

        if invalid_table is not None:
            invalid_batches.append(invalid_table)

         # collect null masks
        for col, mask in null_mask.items():
            if col not in all_null_mask:
                all_null_mask[col] = [mask]
            else:
                all_null_mask[col].append(mask)

    # combine null masks
    final_null_masks = {
        col: pa.concat_arrays(masks)
        for col, masks in all_null_mask.items()
    }

    end_batches = time.perf_counter()

    print(f"Batching + Validation Time: {end_batches - start_batches:.4f} seconds")

    concat_start = time.perf_counter()

    valid_rows = pa.concat_tables(valid_batches) if valid_batches else None
    invalid_rows = pa.concat_tables(invalid_batches) if invalid_batches else None

    concat_end = time.perf_counter()

    print(f"Concatenation Time: {concat_end - concat_start:.4f} seconds")

    valid_rows = _enrich_with_run_id(valid_rows, run_id)
    invalid_rows = _enrich_with_run_id(invalid_rows, run_id)

    return valid_rows, invalid_rows, final_null_masks
########################################################################################################################
