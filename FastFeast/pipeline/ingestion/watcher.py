from pipeline.config.config import settings
from datetime import datetime, timedelta, date
import time as t
from pathlib import Path

def wait_for_batch_folder():
    batch_dir = Path(settings.paths.batch_dir)
    today = date.today()
    
    folder_name = today.strftime(settings.datetime_handling.date_key_format)
    folder = batch_dir / folder_name

    schedule_str = settings.batch.schedule
    start_time = datetime.combine(today, datetime.strptime(schedule_str, "%H:%M").time())

    end_time = start_time + timedelta(seconds=15)

    while True:
        now = datetime.now()

        # if pipeline not started yet
        if now < start_time:
            wait_seconds = (start_time - now).total_seconds()
            print(f"Pipeline has NOT started yet. Waiting for {wait_seconds:.0f} seconds until schedule time...")
            t.sleep(min(wait_seconds, 5)) 
            continue  

        # check if folder exists or not
        if folder.exists():
            print(f"Pipeline started and batch folder found: {folder}")
            return folder
        else:
            print("Pipeline started but batch folder not found yet. Checking again soon...")

        # after starting of pipeline
        if now >= end_time:
            error_msg = f"ERROR: Pipeline started but batch folder not found: {folder}"
            print(error_msg)
            return error_msg

        t.sleep(60)  

# Usage
result = wait_for_batch_folder()
print(result)