import duckdb

def init_db(db_path: str):
    conn = duckdb.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS FILE_TRACKING (
            FILE_PATH        VARCHAR PRIMARY KEY,
            PIPELINE_RUN_ID  VARCHAR,
            PROCESSED_AT     TIMESTAMP,
            STATUS           VARCHAR,
            CURRENT_STAGE    VARCHAR,
            RECORD_COUNT     INTEGER,
            FILE_HASH        VARCHAR,
            ATTEMPT_COUNT    INTEGER DEFAULT 0
        )
    """)
    return conn