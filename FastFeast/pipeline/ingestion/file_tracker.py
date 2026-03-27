import uuid 
from datetime import datetime
from utilities.db_utils import get_connection
import logging

log = logging.getLogger('file_tracker')

def is_processed(file_path: str) -> bool:
    """
    return True if file has been processed successfully before
    """
    conn = get_connection()
    result = conn.execute(
        "SELECT STATUS FROM FILE_TRACKING WHERE FILE_PATH = ?", 
        [file_path]
    ).fetchone()
    return result is not None and result[0] == 'SUCCESS'

def mark_processing(file_path: str, run_id: str) -> None:
    """
    Mark a file as being processed. This should be called at the start of processing. 
    catches crashes and ensures we don't have multiple runs processing the same file
    """
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO FILE_TRACKING (FILE_PATH, PROCESSED_AT, STATUS, RECORD_COUNT, PIPELINE_RUN_ID)
        VALUES (?, ?, 'PROCESSING', 0, ?)
        ON CONFLICT (FILE_PATH) DO UPDATE SET 
            STATUS = 'PROCESSING',
            PROCESSED_AT = ? ,
            PIPELINE_RUN_ID = ?
        """, [file_path, datetime.now(), run_id, datetime.now(), run_id ]
        )
    conn.commit()
    log.info(f"File marked as processing:", extra={"file_path": file_path, "run_id": run_id})

def mark_processed( file_path: str, status: str, record_count: int, run_id: str) -> None: 
    """
    Mark a file as processed successfully. This should be called at the end of processing. 
    """
    conn = get_connection()
    conn.execute(
        """
        UPDATE FILE_TRACKING 
        SET STATUS = ? , PROCESSED_AT = ?, RECORD_COUNT = ?
        WHERE FILE_PATH = ?
        """, [status, datetime.now(), record_count, file_path]
    )
    conn.commit()
    log.info("file marked as processed:", extra={"file_path": file_path, "record_count": record_count, "run_id": run_id})

def get_current_stage(file_path: str) -> str:
    """
    Find out where a file left off (defaults to 'pending' if brand new).
    This can be used to resume processing from the last stage in case of failures. 
    """
    conn = get_connection()
    result = conn.execute(
        "SELECT CURRENT_STAGE FROM FILE_TRACKING WHERE FILE_PATH = ?", 
        [file_path]
    ).fetchone()
    return result[0] if result else 'PENDING'

def update_stage(file_path:str, stage: str, run_id: str) -> None: 
    """
    advance the file to next stage in the database
    """
    conn = get_connection()
    conn.execute(
        """
        UPDATE FILE_TRACKING 
        SET CURRENT_STAGE = ?, PROCESSED_AT = ?, PIPELINE_RUN_ID = ?
        WHERE FILE_PATH = ?
        """, [stage, datetime.now(), run_id, file_path] 
    )
    conn.commit()
    log.info("file stage updated:", extra={"file_path": file_path, "stage": stage, "run_id": run_id})


def generate_run_id() -> str:
    """
    Generate a unique run ID for this pipeline run. This can be used to track which files were processed in which runs. 
    """
    return f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
