import logging
import shutil
from pathlib import Path

log = logging.getLogger("bronze_writer")

_SCRIPT_DIR  = Path(__file__).resolve().parent         
_PIPELINE_DIR = _SCRIPT_DIR.parent                      
_ROOT_DIR     = _PIPELINE_DIR.parent                    
BRONZE_ROOT   = _ROOT_DIR / "data" / "bronze"


def write(src_filepath: Path, date_str: str, hour: str) :
    src  = Path(src_filepath)
    dest = BRONZE_ROOT / date_str / hour / src.name

    log.info(
        "BRONZE  copy  src=%s  dest=%s",
        src.name, dest,
    )

    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)         
        log.info(
            "BRONZE  ok    file=%s  size=%d bytes",
            src.name, dest.stat().st_size,
        )
        return True

    except Exception as exc:
        log.error(
            "BRONZE  FAIL  file=%s  reason=%s",
            src.name, exc,
        )
        return False