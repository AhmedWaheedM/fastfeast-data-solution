# db_utils.py
import os
from pathlib import Path
from dwh.bronze.file_tracking import init_db
from pipeline.config.config import load

_HERE     = Path(__file__).resolve().parent
_ROOT     = _HERE.parent
_PIPELINE = _ROOT / "pipeline"
_CONFIG   = _PIPELINE / "config" / "config.yaml"

_cfg = load(str(_CONFIG))

# ── Single canonical DB path ──────────────────────────────────────
DB_PATH = (_ROOT / _cfg.paths.output_dir / _cfg.database.file).resolve()

def get_connection():
    os.makedirs(DB_PATH.parent, exist_ok=True)
    return init_db(str(DB_PATH))