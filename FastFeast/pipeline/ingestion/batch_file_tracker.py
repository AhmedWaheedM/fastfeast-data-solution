from FastFeast.utilities.db_utils import get_connection


#######################################################
# In each function add conn.close to open safely thread
#######################################################

def get_last_state(file_name):

    conn = get_connection()
    row = conn.execute(
        "SELECT last_hash, last_checkpoint FROM BATCH_FILE_TRACKING WHERE file_name = ?",
        (file_name,)
    ).fetchone()
    return row if row else (None, None)

        


def update_state(file_name, file_hash, checkpoint):
    conn = get_connection()
    conn.execute("""
        INSERT OR REPLACE INTO BATCH_FILE_TRACKING (file_name, last_hash, last_checkpoint)
        VALUES (?, ?, ?)
    """, (file_name, file_hash, checkpoint))

