import time
import logging
from datetime import date, datetime

from FastFeast.pipeline.ingestion.file_tracker import generate_run_id
from FastFeast.pipeline.ingestion.Micro_batch_File_Watcher import run_cycle, SLEEP_HOURS, STREAM_DIR, DB_PATH

log = logging.getLogger("daemon")


def run(explicit_date: str | None = None):
    log.info("=" * 60)
    log.info("DAEMON  starting  date=%s", explicit_date or "today")
    log.info("DAEMON  stream_dir=%s", STREAM_DIR)
    log.info("DAEMON  db_path=%s",    DB_PATH)
    log.info("=" * 60)

    cycle = 0
    try:
        while True:
            cycle       += 1
            current_date = explicit_date if explicit_date else date.today().isoformat()
            cycle_id     = generate_run_id()

            log.info("CYCLE  #%d  cycle_id=%s  date=%s", cycle, cycle_id, current_date)

            try:
                run_cycle(current_date, cycle_id)
            except Exception as exc:
                log.error("CYCLE  unhandled error — retrying after sleep  reason=%s", exc)

            wake_at = datetime.fromtimestamp(time.time() + SLEEP_HOURS * 3600)
            log.info("SLEEP  %dh  wake~%s", SLEEP_HOURS, wake_at.strftime("%H:%M:%S"))
            time.sleep(SLEEP_HOURS * 3600)

    except KeyboardInterrupt:
        log.info("DAEMON  interrupted")

    log.info("DAEMON  finished  cycles=%d", cycle)
