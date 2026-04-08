from pathlib import Path
import threading
from FastFeast.dwh.bronze.file_tracking import init_db
from FastFeast.pipeline.config.config import load

_HERE     = Path(__file__).resolve().parent
_ROOT     = _HERE.parent
_PIPELINE = _ROOT / "pipeline"
_CONFIG   = _PIPELINE / "config" / "config.yaml"

_cfg = load(str(_CONFIG))

def _db_filename(cfg) -> str:
    # Prefer the explicit db_name when available, otherwise fallback to file.
    return getattr(cfg.database, "db_name", "") or cfg.database.file


# ── Canonical output + DB paths (anchored to FastFeast/) ──────────
OUTPUT_DIR = (_ROOT / _cfg.paths.output_dir).resolve()
DB_PATH = (OUTPUT_DIR / _db_filename(_cfg)).resolve()


def get_output_dir() -> Path:
    return OUTPUT_DIR


def get_db_path() -> Path:
    return DB_PATH


#_connection = None
def get_connection():
    output_dir = get_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    conn = init_db(str(get_db_path()))
    _ensure_ddl(conn)
    return conn

def _run_ddl(conn):
    """
    Run All DDL files to ensure tables exist 
    """
    ddl_dirs = [
        _ROOT / "dwh" / "bronze",
        _ROOT / "dwh" / "silver",
        _ROOT / "dwh" / "gold",
        _ROOT / "dwh" / "analytics",
    ]
    for directory in ddl_dirs:
        if not directory.exists():
            continue
        for sql_file in sorted(directory.glob("*.sql")):
            try:
                conn.execute(sql_file.read_text(encoding="utf-8"))
            except Exception as exc:
                # DDL scripts use plain CREATE TABLE in several files; tolerate re-runs.
                error_msg = str(exc).lower()
                if "already exists" not in error_msg and "does not exist" not in error_msg:
                    raise


_ddl_lock = threading.Lock()
_ddl_ready = False


def _ensure_ddl(conn):
    global _ddl_ready
    if _ddl_ready:
        return

    with _ddl_lock:
        if _ddl_ready:
            return
        _run_ddl(conn)
        _ddl_ready = True
