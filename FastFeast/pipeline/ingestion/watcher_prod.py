from FastFeast.pipeline.config.config import config_settings
from FastFeast.pipeline.config.metadata import metadata_settings
from datetime import datetime, date, timedelta
import time as t
from pathlib import Path
from pipeline.logger import pipeline as log #Logger discussion
from FastFeast.pipeline.ingestion.files_processing_prod import process_all_batch_files

def wait_for_batch_folder():
    batch_dir = Path(config_settings.paths.batch_dir)
    today = date.today()
    date_time_config = config_settings.datetime_handling
    folder_name = today.strftime(date_time_config.date_key_format)
    folder = batch_dir / folder_name

    schedule_str = config_settings.batch.schedule
    start_time = datetime.combine(today, datetime.strptime(schedule_str, date_time_config.time_key_format).time())

    end_time = start_time + timedelta(seconds=config_settings.batch.timeout)

    while True:
        now = datetime.now()
        if now < start_time:
            wait_seconds = (start_time - now).total_seconds()
            t.sleep(wait_seconds)  

        if folder.exists():
            return folder

        if now >= end_time:
           error_msg = log.error(f"Missed Batch {folder_name}") #Logger discussion

        return error_msg

# # ----------------------------------------------------------------------
# # Test
# # ----------------------------------------------------------------------

# if __name__ == "__main__":

#     #Wait for today's batch folder
#     folder = wait_for_batch_folder()

#     if folder is None:
#         log.error("No batch folder detected. Exiting...")
#         exit(1)

#     log.info(f"Batch folder detected: {folder}")

#     #Get expected files
#     EXPECTED_FILES = [f.file_name for f in metadata_settings.batch]

#     #Trigger processing
#     success = process_all_batch_files(
#         folder,
#         EXPECTED_FILES,
#         config_settings.database.db_name
#     )

#     if success:
#         log.info("Pipeline completed successfully")
#     else:
#         log.error("Pipeline failed")