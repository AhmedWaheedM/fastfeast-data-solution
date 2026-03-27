from pipeline.config.config import config_settings
from datetime import datetime, date
import time as t
from pathlib import Path
from pipeline.logger import pipeline as log #Logger discussion

def wait_for_batch_folder():
    batch_dir = Path(config_settings.paths.batch_dir)
    today = date.today()
    date_time_config = config_settings.datetime_handling
    folder_name = today.strftime(date_time_config.date_key_format)
    folder = batch_dir / folder_name

    schedule_str = config_settings.batch.schedule
    start_time = datetime.combine(today, datetime.strptime(schedule_str, date_time_config.time_key_format).time())

    end_time = start_time + config_settings.batch.timeout

    while True:
        now = datetime.now()
        if now < start_time:
            wait_seconds = (start_time - now).total_seconds()
            t.sleep(wait_seconds)  

        if folder.exists():
            return folder

        if now >= end_time:
           log.error(f"Missed Batch {folder_name}") #Logger discussion