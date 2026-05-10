from __future__ import annotations

import json
from pathlib import Path

from FastFeast.utilities.helper_utils import now_iso, short_id
try:
    from FastFeast.observability import logger as pipeline_log
    LOG_AVAILABLE = True
except ImportError:
    LOG_AVAILABLE = False
    pipeline_log  = None


# STORE

def make_store(run_id = ""):
    return {
        "run_id":     run_id or short_id(),
        "started_at": now_iso(),
        "files":      [],
    }


# ─────────────────────────────────────────────────────────────────────────────
# RECORDING
# ─────────────────────────────────────────────────────────────────────────────

def record_file(store,validation_result,elapsed_ms= 0.0,):
   
    r = validation_result
    null = type = enum = range = fmt = missing = 0

    for issue in r.issues:
        c = issue.check
        if   c == "null":    null    += issue.count
        elif c == "type":    type   += issue.count
        elif c == "enum":    enum    += issue.count
        elif c == "range":   range  += issue.count
        elif c == "format":  fmt     += issue.count
        elif c == "missing": missing += 1

    store["files"].append({
        "file_name":     r.file_name,
        "run_id":        store["run_id"],
        "recorded_at":   now_iso(),
        "total_rows":    r.row_count,
        "clean_rows":    max(0, r.row_count - len(r.bad_row_indices)),
        "issue_count":   r.issue_count,
        "is_clean":      r.is_clean,
        "null_issues":   null,
        "type_issues":   type,
        "enum_issues":   enum,
        "range_issues":  range,
        "format_issues": fmt,
        "missing_cols":  missing,
        "elapsed_ms":    elapsed_ms,
    })
    return store


# SNAPSHOT

def snapshot(store) :
    """Build and return a JSON-serialisable summary of all recorded files."""
    files        = store["files"]
    total_rows   = sum(f["total_rows"]  for f in files)
    total_issues = sum(f["issue_count"] for f in files)
    clean_files  = sum(1 for f in files if f["is_clean"])

    return {
        "run_id":       store["run_id"],
        "recorded_at":  now_iso(),
        "started_at":   store["started_at"],
        "files_total":  len(files),
        "files_clean":  clean_files,
        "files_failed": len(files) - clean_files,
        "total_rows":   total_rows,
        "total_issues": total_issues,
        "per_file":     list(files),
    }


# LOGGING

def log_metrics(snap, ctx):
    if not (LOG_AVAILABLE and ctx):
        return

    pipeline_log.info(
        ctx,
        f"Metrics snapshot: {snap['files_total']} file(s), "
        f"{snap['total_rows']} rows, {snap['total_issues']} issues",
        table="observability",
        context={"files_clean": snap["files_clean"], "files_failed": snap["files_failed"]},
    )
    for f in snap["per_file"]:
        level = "info" if f["is_clean"] else "warn"
        getattr(pipeline_log, level)(
            ctx,
            f"  {f['file_name']}: {f['total_rows']} rows | "
            f"issues={f['issue_count']} | {f['elapsed_ms']:.1f}ms",
            source=f["file_name"],
            count=f["issue_count"],
        )


# PERSISTENCE

def write_report(snap, output_path ) :
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(snap, fh, indent=2)
    return output_path