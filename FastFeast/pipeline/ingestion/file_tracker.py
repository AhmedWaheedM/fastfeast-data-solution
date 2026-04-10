import hashlib
from pathlib import Path
import uuid
from datetime import datetime
from FastFeast.utilities.db_utils import get_connection
from FastFeast.support.logger import pipeline as log



def hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def try_acquire(file_key: str, cycle_id: str, current_hash: str, max_attempts: int) -> bool:
    conn = get_connection()
    row  = conn.execute(
        "SELECT ATTEMPT_COUNT, LAST_HASH, STATUS FROM FILE_TRACKING WHERE FILE_PATH = ?",
        [file_key]
    ).fetchone()

    if row:
        attempts, stored_hash, status = row
        if attempts >= max_attempts:
            log.warning("SKIP  max attempts reached (%d)  path=%s", max_attempts, file_key)
            return False
        if stored_hash == current_hash and status == "SUCCESS":
            log.debug("SKIP  unchanged successful file  path=%s", file_key)
            return False

    acquire_file(file_key, current_hash, cycle_id, conn)
    conn.close()
    return True


def acquire_file(file_path: str, new_hash: str, run_id: str, conn=None) -> None:
    now  = datetime.now()
    conn = conn or get_connection()
    conn.execute(
        """
        INSERT INTO FILE_TRACKING (FILE_PATH, PIPELINE_RUN_ID, PROCESSED_AT, STATUS,
                                   CURRENT_STAGE, RECORD_COUNT, LAST_HASH, ATTEMPT_COUNT)
        VALUES (?, ?, ?, 'PROCESSING', 'PENDING', 0, ?, 1)
        ON CONFLICT (FILE_PATH, PIPELINE_RUN_ID) DO UPDATE SET
            LAST_HASH       = excluded.LAST_HASH,
            STATUS          = 'PROCESSING',
            PROCESSED_AT    = excluded.PROCESSED_AT,
            PIPELINE_RUN_ID = excluded.PIPELINE_RUN_ID,
            ATTEMPT_COUNT   = FILE_TRACKING.ATTEMPT_COUNT + 1
        """,
        [file_path, run_id, now, new_hash],
    )
    #conn.commit()
    conn.close()
    log.info("ACQUIRED  path=%s  run_id=%s", str(file_path), run_id)


def is_processed(file_path: str) -> bool:
    conn   = get_connection()
    result = conn.execute(
        "SELECT STATUS FROM FILE_TRACKING WHERE FILE_PATH = ?", [str(file_path)]
    ).fetchone()
    conn.close()
    return result is not None and result[0] == "SUCCESS"


def mark_processing(file_path: str, file_hash: str, record_count: int, run_id: str) -> None:
    conn = get_connection()

    conn.execute(
        """
        INSERT INTO FILE_TRACKING 
        (FILE_PATH, PROCESSED_AT, STATUS, CURRENT_STAGE, LAST_HASH, RECORD_COUNT, PIPELINE_RUN_ID)
        VALUES (?, ?, 'PROCESSING', 'PENDING', ?, ?, ?)
        ON CONFLICT (FILE_PATH, PIPELINE_RUN_ID) DO UPDATE SET
            PROCESSED_AT  = excluded.PROCESSED_AT,
            STATUS        = 'PROCESSING',
            CURRENT_STAGE = 'PENDING',
            LAST_HASH     = excluded.LAST_HASH,
            RECORD_COUNT  = excluded.RECORD_COUNT;
        """,
        [file_path, datetime.now(), file_hash, record_count, run_id]
    )
    conn.close()
    log.info("File marked as processing", file_path=file_path, run_id=run_id)


def mark_processed(file_path: str, status: str, record_count: int, run_id: str) -> None:
    now  = datetime.now()
    conn = get_connection()

    conn.execute(
        """
        UPDATE FILE_TRACKING
        SET STATUS          = ?,
            PROCESSED_AT    = ?,
            RECORD_COUNT    = ?
        WHERE FILE_PATH = ?
          AND PIPELINE_RUN_ID = ?
        """,
        [status, now, record_count, str(file_path), run_id],
    )
    #conn.commit()
    conn.close()
    log.info("MARKED  path=%s  status=%s  records=%d", file_path, status, record_count)


def update_stage(file_path: str, stage: str, run_id: str) -> None:
    now  = datetime.now()
    conn = get_connection()
    conn.execute(
        """
        UPDATE FILE_TRACKING
                SET CURRENT_STAGE = ?, PROCESSED_AT = ?
                WHERE FILE_PATH = ?
                    AND PIPELINE_RUN_ID = ?
        """,
                [stage, now, str(file_path), run_id],
    )
    #conn.commit()
    conn.close()
    log.info("STAGE  path=%s  stage=%s", str(file_path), stage)


def get_current_stage(file_path: str) -> str:
    conn   = get_connection()
    result = conn.execute(
        "SELECT CURRENT_STAGE FROM FILE_TRACKING WHERE FILE_PATH = ?", [str(file_path)]
    ).fetchone()
    conn.close()
    return result[0] if result else "PENDING"



def generate_run_id() -> str:
    """
    Generate a unique run ID for this pipeline run. This can be used to track which files were processed in which runs. 
    """
    return f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"