import logging
import shutil
from pathlib import Path

from support.logger import pipeline as log

_PIPELINE_DIR = Path(__file__).resolve().parent.parent   # pipeline/
_ROOT_DIR     = _PIPELINE_DIR.parent                     # FastFeast/
BRONZE_ROOT   = _ROOT_DIR / "data" / "bronze"



def write(src_filepath: Path, date_str: str) -> bool:
    """
    Copy a raw file into the bronze layer, preserving the hour subfolder.
    Path: bronze / 2026-03-31 / 08 / orders.json
    """
    src       = Path(src_filepath)
    hour_part = src.parent.name          # e.g. "08"
    dest      = BRONZE_ROOT / date_str / hour_part / src.name

    log.info("BRONZE  copy  src=%s  dest=%s", src.name, dest)

    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        log.info("BRONZE  ok  file=%s  size=%d bytes", src.name, dest.stat().st_size)
        return True
    except Exception as exc:
        log.error("BRONZE  FAIL  file=%s  reason=%s", src.name, exc)
        return False