import os
import argparse
import duckdb

from pipeline.logger     import pipeline as log
from pipeline.validation import validate_table

BATCH_DIR = "data/input/batch"
DB_PATH   = "fastfeast.duckdb"

CSV_TABLES  = [
    "customers", "drivers", "agents", "regions", "reasons",
    "categories", "segments", "teams", "channels", "priorities",
    "reason_categories",
]
JSON_TABLES = ["restaurants", "cities"]


def run(date: str):
    batch_dir = os.path.join(BATCH_DIR, date)
    conn = duckdb.connect(DB_PATH)
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")

    for table in CSV_TABLES:
        path = os.path.join(batch_dir, f"{table}.csv")
        if not os.path.exists(path):
            continue
        conn.execute(f"DROP TABLE IF EXISTS staging.{table}")
        conn.execute(f"""
                     
            CREATE TABLE staging.{table} AS
            SELECT * FROM read_csv_auto('{path}', header=true, ignore_errors=true)
            
        """)
        log.info("loaded", table=table, source=path)
        validate_table(conn, f"staging.{table}", table)

    for table in JSON_TABLES:
        path = os.path.join(batch_dir, f"{table}.json")
        if not os.path.exists(path):
            continue
        conn.execute(f"DROP TABLE IF EXISTS staging.{table}")
        conn.execute(f"""
            CREATE TABLE staging.{table} AS
            SELECT * FROM read_json_auto(
                '{path}',
                ignore_errors   = true,
                timestampformat = '%Y-%m-%d %H:%M:%S'
            )
        """)
        log.info("loaded", table=table, source=path)
        validate_table(conn, f"staging.{table}", table)

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    run(parser.parse_args().date)
