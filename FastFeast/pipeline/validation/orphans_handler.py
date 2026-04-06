import pyarrow as pa
import pyarrow.compute as pc

def table_orphans(pa_table, fk_pk_map, record_status, error_lists):
  valid_indices = [i for i, val in enumerate(record_status) if val == 'VALID']
  for col_name in pa_table.schema.names:
    col = pa_table[col_name]
    if col_name in fk_pk_map:
      orphan_mask = pc.invert(pc.is_in(col, fk_pk_map[col_name]))
      indices = pc.indices_nonzero(orphan_mask).to_pylist()
      orphan_indices = [i for i in indices if i in valid_indices]
      for idx in indices:
          if not 'orphan' in error_lists[idx]:
            error_lists[idx]['orphan'] = []
          error_lists[idx]['orphan'].append(col_name)
          record_status[idx] = 'ORPHAN'
  return record_status, error_lists
