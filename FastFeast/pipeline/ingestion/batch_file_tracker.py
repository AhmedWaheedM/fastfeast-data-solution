





def get_last_state(conn, file_name):
    row = conn.execute(
        "SELECT last_hash, last_checkpoint FROM BATCH_FILE_TRACKING WHERE file_name = ?",
        (file_name,)
    ).fetchone()
    return row if row else (None, None)


def update_state(conn, file_name, file_hash, checkpoint):
    conn.execute("""
        INSERT OR REPLACE INTO BATCH_FILE_TRACKING (file_name, last_hash, last_checkpoint)
        VALUES (?, ?, ?)
    """, (file_name, file_hash, checkpoint))
