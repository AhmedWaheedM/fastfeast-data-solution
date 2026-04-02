from FastFeast.utilities.db_utils import get_connection

conn = get_connection()

rows = conn.execute("SELECT PROCESSED_AT, STATUS, CURRENT_STAGE, LAST_HASH, RECORD_COUNT, PIPELINE_RUN_ID FROM FILE_TRACKING").fetchall()

for r in rows:
    print(r)

conn.close()