import uuid 
from datetime import datetime
from FastFeast.utilities.db_utils import get_connection
from FastFeast.support.logger import pipeline as log

def is_processed(file_path: str) -> bool:
    """
    return True if file has been processed successfully before
    """
    conn = get_connection()
    result = conn.execute(
        "SELECT STATUS FROM FILE_TRACKING WHERE FILE_PATH = ?", 
        [file_path]
    ).fetchone()
    conn.close()
    return result is not None and result[0] == 'SUCCESS'
    


from datetime import datetime
from FastFeast.utilities.db_utils import get_connection
from FastFeast.support.logger import pipeline as log

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

    log.info("File marked as processing", file_path=file_path, run_id=run_id)
    conn.close()
    


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
    #conn.commit()
    log.info(
        "File marked as processed",
        file_path=file_path,
        record_count=record_count,
        run_id=run_id,
    )
    conn.close()


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
    #conn.commit()
    log.info("File stage updated", file_path=file_path, stage=stage, run_id=run_id)

    conn.close()



def generate_run_id() -> str:
    """
    Generate a unique run ID for this pipeline run. This can be used to track which files were processed in which runs. 
    """
    return f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
