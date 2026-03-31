# import time
# import duckdb
# import json
# import re
# import pyarrow as pa
# import pyarrow.csv as pv
# import pyarrow.json as pj
# import pyarrow.compute as pc
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from datetime import datetime
# from pathlib import Path
# from FastFeast.support.logger import pipeline as log
# from FastFeast.pipeline.config.metadata import metadata_settings
# from FastFeast.pipeline.config.config import config_settings
# from FastFeast.utilities.file_utils import get_file_hash, wait_for_file
# from FastFeast.pipeline.ingestion.batch_file_tracker import get_last_state, update_state
# from FastFeast.utilities.db_utils import get_connection


# # ----------------------------------------------------------------------
# # Clean unexpected values like : Nan
# # ----------------------------------------------------------------------

# # def load_clean_json(file_path):
# #     with open(file_path, "r", encoding=config_settings.batch.encoding) as f:
# #         content = f.read()
# #     content = re.sub(r'\bNaN\b', 'null', content)
# #     return json.loads(content)


# # ----------------------------------------------------------------------
# # Read and filter new/updated records
# # ----------------------------------------------------------------------

# def read_files(file_path): #, last_checkpoint):
#     path = Path(file_path)
#     st = config_settings.batch.supported_types

#     # ------------------ READ FILE ------------------
#     if path.suffix == st.csv:
#         table = pv.read_csv(str(path))
#     elif path.suffix == st.json:
#         table = pj.read_json(str(path))
#     else:
#         log.warning("Unsupported format", file=file_path, suffix=path.suffix)
#         return None, None

#     log.info(
#         "File loaded",
#         file=file_path,
#         rows=table.num_rows,
#         columns=table.column_names
#     )

#     # ------------------ NO updated_at ------------------
# #     if 'updated_at' not in table.column_names:
# #         log.warning("No updated_at column found", file=file_path)
# #         return table, datetime.fromtimestamp(path.stat().st_mtime)

# #     updated_col = table['updated_at']

# #     log.info(
# #         "Before parsing updated_at",
# #         file=file_path,
# #         type=str(updated_col.type)
# #     )

# #     # ------------------ SAFE PARSING ------------------
# #     if not pa.types.is_timestamp(updated_col.type):
# #         parsed_col = pc.strptime(
# #             updated_col,
# #             format="%Y-%m-%d %H:%M:%S.%f",
# #             unit='us',
# #             error_is_null=True
# #         )

# #         # Count invalid values
# #         null_mask = pc.is_null(parsed_col)
# #         num_invalid = pc.sum(pc.cast(null_mask, pa.int32())).as_py()

# #         if num_invalid > 0:
# #             log.warning(
# #                 "Invalid updated_at values detected",
# #                 file=file_path,
# #                 invalid_count=num_invalid
# #             )

# #             # Sample bad rows
# #             invalid_rows = table.filter(pc.is_null(parsed_col))
# #             log.debug(
# #                 "Sample invalid rows",
# #                 file=file_path,
# #                 sample=invalid_rows.slice(0, 5).to_pydict()
# #             )

# #         table = table.set_column(
# #             table.schema.get_field_index("updated_at"),
# #             "updated_at",
# #             parsed_col
# #         )

# #         updated_col = parsed_col

# #     # ------------------ FILTERING ------------------
# #     if last_checkpoint is None:
# #         filtered = table
# #     else:
# #         checkpoint_scalar = pa.scalar(last_checkpoint, type=pa.timestamp('us'))

# #         mask = pc.greater(updated_col, checkpoint_scalar)
# #         filtered = table.filter(mask)

# #     log.info(
# #         "Filtering result",
# #         file=file_path,
# #         before_rows=table.num_rows,
# #         after_rows=filtered.num_rows,
# #         checkpoint=str(last_checkpoint)
# #     )

# #     # ------------------ MAX updated_at ------------------
# #     max_updated = None
# #     if filtered.num_rows > 0:
# #         max_scalar = pc.max(filtered['updated_at'])

# #         if max_scalar is not None and max_scalar.as_py() is not None:
# #             max_updated = max_scalar.as_py()

# #     return filtered, max_updated


# # # ----------------------------------------------------------------------
# # # Upsert bronze table
# # # ----------------------------------------------------------------------

# # def upsert_bronze(conn, table_name, arrow_table):
# #     if arrow_table is None or arrow_table.num_rows == 0:
# #         log.info("No data to upsert", table=table_name)
# #         return

# #     log.info(
# #         "Upserting to bronze",
# #         table=table_name,
# #         rows=arrow_table.num_rows
# #     )

# #     conn.execute(f"DROP TABLE IF EXISTS {table_name}")
# #     conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM arrow_table WHERE 1=0")
# #     conn.register("temp", arrow_table)
# #     conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp")
# #     conn.unregister("temp")


# # ----------------------------------------------------------------------
# # Process a single file
# # ----------------------------------------------------------------------

# def process_file(file_path, db_path):
#     start_time = time.time()
#     path = Path(file_path)
#     file_name = path.name

#     if not wait_for_file(file_path):
#         log.warning("File missing after timeout", file=file_name)
#         return True

#     conn = get_connection()

#     try:
#         log.info("Start processing file", file=file_name)

#         new_hash = get_file_hash(file_path)
#         last_hash, last_checkpoint = get_last_state(file_name)

#         log.info(
#             "File state",
#             file=file_name,
#             last_hash=last_hash,
#             new_hash=new_hash,
#             checkpoint=str(last_checkpoint)
#         )

#         if last_hash == new_hash:
#             log.info("SKIPPED (no change)", file=file_name)
#             return True

#         new_records, max_updated = read_files(file_path) #, last_checkpoint)

#         if new_records is None or new_records.num_rows == 0:
#             log.info("No new records after filtering", file=file_name)
#             update_state(file_name, new_hash, last_checkpoint)
#             return True

#         #bronze_table = f"bronze_{path.stem}"
#         #upsert_bronze(conn, bronze_table, new_records)

#         #new_checkpoint = max_updated if max_updated is not None else last_checkpoint
#         #update_state(file_name, new_hash, new_checkpoint)

#         log.info(
#             "PROCESSED",
#             file=file_name,
#             rows=new_records.num_rows,
#             #checkpoint=str(new_checkpoint),
#             duration_sec=round(time.time() - start_time, 2)
#         )

#         return True

#     except Exception as e:
#         log.error(
#             "Error processing file",
#             file=file_name,
#             error=str(e),
#             exc_info=True
#         )
#         return False

#     finally:
#         conn.close()


# # ----------------------------------------------------------------------
# # Process all files in parallel
# # ----------------------------------------------------------------------

# def process_all_batch_files(batch_dir, file_list, db_path):
#     today = datetime.now().date()
#     batch_dir = Path(config_settings.paths.batch_dir)
#     today_folder = Path(batch_dir) / today.strftime(config_settings.datetime_handling.date_key_format)

#     if not today_folder.exists():
#         log.error("Batch folder missing", path=str(today_folder))
#         return False

#     log.info("Starting batch processing", folder=str(today_folder))

#     any_error = False

#     with ThreadPoolExecutor(max_workers=config_settings.pipeline.max_workrs) as executor:
#         futures = []

#         for file_name in file_list:
#             file_path = today_folder / file_name
#             futures.append(executor.submit(process_file, str(file_path), db_path))

#         for future in as_completed(futures):
#             try:
#                 if not future.result():
#                     any_error = True
#             except Exception as e:
#                 log.error("Unexpected thread error", error=str(e), exc_info=True)
#                 any_error = True

#     if any_error:
#         log.error("Batch completed WITH errors")
#     else:
#         log.info("Batch completed WITHOUT errors")

#     return not any_error


# # ----------------------------------------------------------------------
# # Test
# # ----------------------------------------------------------------------

# if __name__ == "__main__":

#     batch_dir = Path(config_settings.paths.batch_dir)
#     EXPECTED_FILES = [f.file_name for f in metadata_settings.batch]

#     success = process_all_batch_files(
#         batch_dir,
#         EXPECTED_FILES,
#         config_settings.database.db_name
#     )

#     if success:
#         log.info("Succeeded: Batch stage completed WITHOUT errors.")
#     else:
#         log.error("Failed: Batch stage completed WITH errors.")
