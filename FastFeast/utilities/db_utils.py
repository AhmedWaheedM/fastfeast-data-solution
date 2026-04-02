import duckdb
import threading
from pathlib import Path
from pipeline.config.config import load

HERE       = Path(__file__).resolve().parent
ROOT       = HERE.parent
PIPELINE   = ROOT / "pipeline"
CONFIG_PATH = PIPELINE / "config" / "config.yaml"

DDL_DIRS = [
    ROOT / "dwh" / "bronze",
    ROOT / "dwh" / "silver",
    ROOT / "dwh" / "gold",
]

connection  = None
conn_lock   = threading.Lock()


def getconnection():
    global connection
    if connection is None:                       
        with conn_lock:                           
            if connection is None:               
                cfg     = load(str(CONFIG_PATH))
                db_path = (PIPELINE / cfg.database.file).resolve()
                db_path.parent.mkdir(parents=True, exist_ok=True)
                conn = duckdb.connect(str(db_path))
                run_ddl(conn)
                connection = conn                 
    return connection


def run_ddl(conn):
    for directory in DDL_DIRS:
        if not directory.exists():
            continue
        for filepath in sorted(directory.glob("*.sql")):
            try:
                conn.execute(filepath.read_text())
            except Exception as e:
                raise RuntimeError(f"DDL failed: {filepath.name}") from e