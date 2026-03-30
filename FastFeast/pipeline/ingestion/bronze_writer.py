"""
bronze_writer.py  —  FastFeast bronze landing layer
====================================================

Strategy: copy-as-is
---------------------
Files that pass the read stage in the watcher are copied verbatim to a
bronze/ folder that mirrors the stream directory structure:

    stream/  2026-03-30 / 17 / orders.json
    bronze/  2026-03-30 / 17 / orders.json   ← exact byte copy

No DuckDB tables are created, no validation is performed, no schema
casting is applied.  The bronze layer is a raw archive of every file
that was successfully ingested from the stream.

Files that failed to read (e.g. DuckDB crash, empty body) are skipped
and logged as FAILED — they are never copied.

Public API
----------
write(src_filepath, date_str, hour) -> bool
    Copy *src_filepath* into bronze/<date_str>/<hour>/<filename>.
    Returns True on success, False on any error.
"""

import logging
import shutil
from pathlib import Path

log = logging.getLogger("bronze_writer")

# ── Bronze root is resolved relative to this file ────────────────────────────
# Layout:  .../FastFeast/pipeline/ingestion/bronze_writer.py
#          .../FastFeast/data/bronze/
_SCRIPT_DIR  = Path(__file__).resolve().parent          # .../pipeline/ingestion/
_PIPELINE_DIR = _SCRIPT_DIR.parent                      # .../pipeline/
_ROOT_DIR     = _PIPELINE_DIR.parent                    # .../FastFeast/
BRONZE_ROOT   = _ROOT_DIR / "data" / "bronze"


def write(src_filepath: Path, date_str: str, hour: str) -> bool:
    """
    Copy *src_filepath* to bronze/<date_str>/<hour>/<filename>.

    Parameters
    ----------
    src_filepath : Path
        Absolute path to the source file in the stream directory.
    date_str : str
        Partition date in YYYY-MM-DD format (e.g. "2026-03-30").
    hour : str
        Hour directory name as it appears in the stream tree (e.g. "17").

    Returns
    -------
    bool
        True if the copy succeeded, False otherwise.
    """
    src  = Path(src_filepath)
    dest = BRONZE_ROOT / date_str / hour / src.name

    log.info(
        "BRONZE  copy  src=%s  dest=%s",
        src.name, dest,
    )

    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)          # copy2 preserves file metadata
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