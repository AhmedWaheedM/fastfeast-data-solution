import duckdb 
from pipeline.config.config import config_settings
from dwh.bronze.file_tracking import init_db
import os
from pathlib import Path
from FastFeast.dwh.bronze.file_tracking import init_db
from FastFeast.pipeline.config.config import load

_HERE     = Path(__file__).resolve().parent
_ROOT     = _HERE.parent
_PIPELINE = _ROOT / "pipeline"
_CONFIG   = _PIPELINE / "config" / "config.yaml"

_cfg = load(str(_CONFIG))

# ── Single canonical DB path ──────────────────────────────────────
DB_PATH = (_ROOT / _cfg.paths.output_dir / _cfg.database.file).resolve()


#_connection = None
def get_connection():
    #_connection = None
    #if _connection is None:
    cfg = config_settings
    db_path = os.path.join(cfg.paths.output_dir, cfg.database.file) # merge output dir and db file name to get full path to db
    os.makedirs(cfg.paths.output_dir, exist_ok=True)
    #_connection = duckdb.connect(db_path)
        #_run_ddl(_connection)
    return init_db(db_path)

def _run_ddl(conn):
    """
    Run All DDL files to ensure tables exist 
    """
    import os 
    ddl_dirs = ['FastFeast/dwh/bronze', 'FastFeast/dwh/silver', 'FastFeast/dwh/gold']
    for directory in ddl_dirs: 
        if not os.path.exists(directory):
            continue
        for filename in sorted(os.listdir(directory)):
            if filename.endswith('.sql'):
                with open(os.path.join(directory, filename), 'r') as f:
                    sql = f.read()
                    conn.execute(sql)
