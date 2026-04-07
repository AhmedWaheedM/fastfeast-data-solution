import hashlib
<<<<<<< HEAD
import uuid
from datetime import datetime
from pathlib import Path

from utilities.db_utils import getconnection
from support.logger import pipeline as log


def hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def get_attempts(file_path: str) -> int:
    conn = getconnection()
    row  = conn.execute(
        "SELECT ATTEMPT_COUNT FROM FILE_TRACKING WHERE FILE_PATH = ?", [file_path]
    ).fetchone()
    return row[0] if row else 0


def get_stored_hash(file_path: str) -> str | None:
    conn = getconnection()
    row  = conn.execute(
        "SELECT FILE_HASH FROM FILE_TRACKING WHERE FILE_PATH = ?", [file_path]
    ).fetchone()
    return row[0] if row else None


def acquire_file(file_path: str, new_hash: str, run_id: str) -> None:
    """Atomically upsert hash, increment attempt counter, and mark as PROCESSING."""
    now  = datetime.now()
    conn = getconnection()
    conn.execute(
        """
        INSERT INTO FILE_TRACKING (FILE_PATH, FILE_HASH, ATTEMPT_COUNT, STATUS,
                                   CURRENT_STAGE, PROCESSED_AT, RECORD_COUNT, PIPELINE_RUN_ID)
        VALUES (?, ?, 1, 'PROCESSING', 'PENDING', ?, 0, ?)
        ON CONFLICT (FILE_PATH) DO UPDATE SET
            FILE_HASH       = excluded.FILE_HASH,
            ATTEMPT_COUNT   = FILE_TRACKING.ATTEMPT_COUNT + 1,
            STATUS          = 'PROCESSING',
            PROCESSED_AT    = excluded.PROCESSED_AT,
            PIPELINE_RUN_ID = excluded.PIPELINE_RUN_ID
        """,
        [file_path, new_hash, now, run_id],
    )
    conn.commit()
    log.info("File acquired", file_path=file_path, run_id=run_id)


def is_processed(file_path: str) -> bool:
    conn   = getconnection()
    result = conn.execute(
        "SELECT STATUS FROM FILE_TRACKING WHERE FILE_PATH = ?", [file_path]
    ).fetchone()
    return result is not None and result[0] == "SUCCESS"


def mark_processed(file_path: str, status: str, record_count: int, run_id: str) -> None:
    now  = datetime.now()
    conn = getconnection()
=======
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
        VALUES (?, ?, 'PROCESSING', 'PENDING', ?, ?, ?);
        """,
        [file_path, datetime.now(), file_hash, record_count, run_id]
    )
    conn.close()
    log.info("File marked as processing", file_path=file_path, run_id=run_id)


def mark_processed(file_path: str, status: str, record_count: int, run_id: str) -> None:
    now  = datetime.now()
    conn = get_connection()

>>>>>>> origin/dev
    conn.execute(
        """
        UPDATE FILE_TRACKING
        SET STATUS          = ?,
            PROCESSED_AT    = ?,
            RECORD_COUNT    = ?,
            PIPELINE_RUN_ID = ?
        WHERE FILE_PATH = ?
        """,
<<<<<<< HEAD
        [status, now, record_count, run_id, file_path],
    )
    conn.commit()
    log.info(
        "File marked as processed",
        file_path=file_path, status=status,
        record_count=record_count, run_id=run_id,
=======
        [status, now, record_count, run_id, str(file_path)],
>>>>>>> origin/dev
    )
    #conn.commit()
    conn.close()
    log.info("MARKED  path=%s  status=%s  records=%d", file_path, status, record_count)


<<<<<<< HEAD
def get_current_stage(file_path: str) -> str:
    conn   = getconnection()
    result = conn.execute(
        "SELECT CURRENT_STAGE FROM FILE_TRACKING WHERE FILE_PATH = ?", [file_path]
    ).fetchone()
    return result[0] if result else "PENDING"


def update_stage(file_path: str, stage: str, run_id: str) -> None:
    now  = datetime.now()
    conn = getconnection()
=======
def update_stage(file_path: str, stage: str, run_id: str) -> None:
    now  = datetime.now()
    conn = get_connection()
>>>>>>> origin/dev
    conn.execute(
        """
        UPDATE FILE_TRACKING
        SET CURRENT_STAGE = ?, PROCESSED_AT = ?, PIPELINE_RUN_ID = ?
        WHERE FILE_PATH = ?
        """,
<<<<<<< HEAD
        [stage, now, run_id, file_path],
=======
        [stage, now, run_id, str(file_path)],
>>>>>>> origin/dev
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
<<<<<<< HEAD
=======
    """
    Generate a unique run ID for this pipeline run. This can be used to track which files were processed in which runs. 
    """
>>>>>>> origin/dev
    return f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"