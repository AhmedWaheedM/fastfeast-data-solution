import pyarrow as pa
import pyarrow.compute as pc

def _propagate_errors(invalid_mask, error_lists, col_name, reason):
  """
    Updates the global error tracking list based on a boolean failure mask.
    Identifies rows where the validation failed, initializes the error reason 
    for those rows if not already present, and appends the column name to the 
    corresponding error category.
  """
  failed_indices = pc.indices_nonzero(invalid_mask).to_pylist()
  for idx in failed_indices:
    if not reason in error_lists[idx]:
      error_lists[idx][reason] = []
    error_lists[idx][reason].append(col_name)


def validate_table(pa_table, expected_types, expected_patterns, expected_not_nullable, expected_format, expected_range, expected_pk, pipeline_type):
  """
    Performs multi-layered validation on a PyArrow Table against defined constraints. 
    This function checks each column for data type integrity, null constraints, 
    regex patterns, and numerical ranges. It includes a fallback mechanism: if 
    a direct type cast fails, it uses regex patterns to identify data type errors 
    while continuing to validate other constraints.
  """
  n = pa_table.num_rows
  error_lists = [{} for _ in range(n)]
  invalid_data_type_mask = invalid_null_mask = invalid_format_mask = invalid_range_mask = pa.array([False] * n)

  for col_name in pa_table.schema.names:
    try:
      col = pc.cast(pa_table[col_name], expected_types[col_name])
      if col_name in expected_not_nullable:
        invalid_null_mask = col.is_null()
        _propagate_errors(invalid_null_mask, error_lists, col_name, 'not_allowed_nulls')
      if col_name in expected_format:
        invalid_format_mask = pc.invert(pc.match_substring_regex(col, pattern= expected_format[col_name]))
        _propagate_errors(invalid_format_mask, error_lists, col_name, 'format')
      if col_name in expected_range:
        if 'min' in expected_range[col_name]:
          invalid_range_mask = pc.or_(pc.less_equal(col, expected_range[col_name]['min']), invalid_range_mask)
        if 'max' in expected_range[col_name]:
          invalid_range_mask = pc.or_(pc.greater_equal(col, expected_range[col_name]['max']), invalid_range_mask)
        _propagate_errors(invalid_range_mask, error_lists, col_name, 'range')

      if col_name == expected_pk:
        counts = pc.value_counts(col)
        dup_values = pc.filter(counts.field("values"),pc.greater(counts.field("counts"), 1))
        mask = pc.is_in(col, value_set=dup_values)
        indices = pc.indices_nonzero(mask).to_pylist()[:-1]
        for idx in indices:
          if not 'duplicated' in error_lists[idx]:
            error_lists[idx]['duplicated'] = []
          error_lists[idx]['duplicated'].append(col_name)

    except Exception as e:
      null_mask = pc.equal(pc.cast(pa_table[col_name], pa.string()).fill_null(''), pa.scalar(''))
      col = pc.if_else(pc.invert(null_mask), pa_table[col_name], pa.scalar(None, type=pa.string()))
      invalid_data_type_mask = pc.invert(pc.match_substring_regex(col, pattern= expected_patterns[col_name]))
      _propagate_errors(invalid_data_type_mask, error_lists, col_name, 'data_type')

      if col_name in expected_not_nullable:
        invalid_null_mask = col.is_null()
        _propagate_errors(invalid_null_mask, error_lists, col_name, 'not_allowed_nulls')

      col = pc.if_else(pc.invert(invalid_data_type_mask), col, pa.scalar(None, type=pa.string()))
      col = pc.cast(col, expected_types[col_name])
      if col_name in expected_format:
        invalid_format_mask = pc.invert(pc.match_substring_regex(col, pattern= expected_format[col_name]))
        _propagate_errors(invalid_format_mask, error_lists, col_name, 'format')
      if col_name in expected_range:
        if 'min' in expected_range[col_name]:
          invalid_range_mask = pc.or_(pc.less_equal(col, expected_range[col_name]['min']), invalid_range_mask)
        if 'max' in expected_range[col_name]:
          invalid_range_mask = pc.or_(pc.greater_equal(col, expected_range[col_name]['max']), invalid_range_mask)
        _propagate_errors(invalid_range_mask, error_lists, col_name, 'range')

      if col_name == expected_pk:
        counts = pc.value_counts(col)
        dup_values = pc.filter(counts.field("values"),pc.greater(counts.field("counts"), 1))
        mask = pc.is_in(col, value_set=dup_values)
        indices = pc.indices_nonzero(mask).to_pylist()[:-1]
        for idx in indices:
          if not 'duplicated' in error_lists[idx]:
            error_lists[idx]['duplicated'] = []
          error_lists[idx]['duplicated'].append(col_name)
  status_list = ['INVALID' if e else 'VALID' for e in error_lists]

  if pipeline_type == 'batch':
    valid_indices = [i for i, val in enumerate(status_list) if val == 'VALID']
    indices_to_keep = pa.array(valid_indices, type=pa.int64())
    pk_col = pc.take(pa_table[expected_pk], indices_to_keep)
    return status_list, error_lists, pk_col
  else:
    return status_list, error_lists
