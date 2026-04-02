import hashlib
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
    conn.execute(
        """
        UPDATE FILE_TRACKING
        SET STATUS          = ?,
            PROCESSED_AT    = ?,
            RECORD_COUNT    = ?,
            PIPELINE_RUN_ID = ?
        WHERE FILE_PATH = ?
        """,
        [status, now, record_count, run_id, file_path],
    )
    conn.commit()
    log.info(
        "File marked as processed",
        file_path=file_path, status=status,
        record_count=record_count, run_id=run_id,
    )


def get_current_stage(file_path: str) -> str:
    conn   = getconnection()
    result = conn.execute(
        "SELECT CURRENT_STAGE FROM FILE_TRACKING WHERE FILE_PATH = ?", [file_path]
    ).fetchone()
    return result[0] if result else "PENDING"


def update_stage(file_path: str, stage: str, run_id: str) -> None:
    now  = datetime.now()
    conn = getconnection()
    conn.execute(
        """
        UPDATE FILE_TRACKING
        SET CURRENT_STAGE = ?, PROCESSED_AT = ?, PIPELINE_RUN_ID = ?
        WHERE FILE_PATH = ?
        """,
        [stage, now, run_id, file_path],
    )
    conn.commit()
    log.info("File stage updated", file_path=file_path, stage=stage, run_id=run_id)


def generate_run_id() -> str:
    return f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"