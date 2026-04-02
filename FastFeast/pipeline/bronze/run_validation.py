from __future__ import annotations

import atexit
import argparse
import time
import uuid
from datetime import date
from pathlib import Path
import sys

HERE      = Path(__file__).resolve().parent
PIPELINE  = HERE.parent
FASTFEAST = PIPELINE.parent
ROOT      = FASTFEAST.parent
if str(FASTFEAST) not in sys.path:
    sys.path.insert(0, str(FASTFEAST))

from pipeline.config import config as cfg_loader
from pipeline.bronze.bronze_reader import read_all_sources
from pipeline.bronze.schema_validator import load_schema, validate_table

from pipeline.observability.metrics import (
    make_store,
    record_file,
    snapshot,
    log_metrics,
    write_report
)
from pipeline.observability.alerts import shutdown, send_failure_alert, send_success_alert
from pipeline.observability.logger import setup_logger

try:
    from pipeline.observability import logger as pipeline_log
    LOG_AVAILABLE = True
except ImportError:
    LOG_AVAILABLE = False
    pipeline_log  = None


CONFIG_PATH   = PIPELINE / "config" / "config.yaml"
METADATA_PATH = PIPELINE / "config" / "files_metadata.yaml"

try:
    cfg = cfg_loader.load(str(CONFIG_PATH))
    DATAROOT = ROOT / cfg.paths.data_dir
    LOG_DIR  = FASTFEAST / cfg.paths.log_dir
    print("Log directory:", LOG_DIR)

    atexit.unregister(shutdown)
    atexit.register(shutdown, cfg.alerts)

except Exception as exc:
    print(f"[run_validation] config load failed ({exc}), using built-in defaults")
    cfg      = None
    DATAROOT = ROOT / "data"
    LOG_DIR  = FASTFEAST / "logs"


STREAM_FILES = {"orders.json", "tickets.csv", "ticket_events.json"}
BATCH_FILES  = {
    "customers.csv", "drivers.csv", "agents.csv",
    "channels.csv",  "priorities.csv", "reasons.csv",
}
MASTER_FILES = {
    "cities.json",    "restaurants.json", "regions.csv",
    "categories.csv", "segments.csv",     "teams.csv",
    "reason_categories.csv",
}

def section_for(file_name):
    if file_name in STREAM_FILES:
        return "stream"
    if file_name in BATCH_FILES:
        return "batch"
    if file_name in MASTER_FILES:
        return "master"
    return "batch"


STEM_MAP = {
    "orders":            "orders.json",
    "tickets":           "tickets.csv",
    "ticket_events":     "ticket_events.json",
    "cities":            "cities.json",
    "restaurants":       "restaurants.json",
    "regions":           "regions.csv",
    "categories":        "categories.csv",
    "segments":          "segments.csv",
    "teams":             "teams.csv",
    "reason_categories": "reason_categories.csv",
    "customers":         "customers.csv",
    "drivers":           "drivers.csv",
    "agents":            "agents.csv",
    "channels":          "channels.csv",
    "priorities":        "priorities.csv",
    "reasons":           "reasons.csv",
}

def stem_to_filename(stem):
    return STEM_MAP.get(stem, f"{stem}.csv")


def run_bronze_validation(
    run_date,
    hour          = None,
    stream_only   = False,
    dataROOT      = DATAROOT,
    metadata_path = METADATA_PATH,
    log_dir       = LOG_DIR,
    alert_email   = None,
):
    if cfg is None:
        raise RuntimeError("Config failed to load — cannot run validation.")

    alerts_cfg    = cfg.alerts
    smtp_password = cfg.smtp_password

    if alert_email:
        alerts_cfg.email = alert_email

    dataROOT    = Path(dataROOT)
    hour_label  = f"{int(hour):02d}" if hour is not None else "all"
    run_log_dir = Path(log_dir) / run_date / f"hour={hour_label}"

    obs_logger = setup_logger(
        name           = f"fastfeast_validation_{run_date}_{hour_label}",
        log_dir        = str(run_log_dir),
        level          = cfg.logging.level,
        max_bytes      = cfg.logging.max_bytes,
        backup_count   = cfg.logging.backup_count,
        fmt            = cfg.logging.fmt,
        datefmt        = cfg.logging.datefmt,
        timed_rotation = (cfg.logging.rotate == "daily"),
        when           = "midnight",
    )

    obs_logger.info("=" * 60)
    obs_logger.info(f"Bronze validation starting  date={run_date}  hour={hour}  stream_only={stream_only}")
    obs_logger.info(f"Data root:   {dataROOT}")
    obs_logger.info(f"Log folder:  {run_log_dir}")

    ctx = None
    if LOG_AVAILABLE:
        ctx = pipeline_log.init(
            run_date,
            phase   = "quality_check",
            hour    = int(hour) if hour is not None else None,
            log_dir = str(run_log_dir),
        )

    run_id = str(uuid.uuid4())[:8]
    store  = make_store(run_id=run_id)

    all_partitions = read_all_sources(
        data_root     = dataROOT,
        run_date      = run_date,
        hour          = hour,
        include_batch = not stream_only,
        ctx           = ctx,
    )

    if not all_partitions:
        obs_logger.warning(
            f"No data found under {dataROOT} for date={run_date}. "
            f"Check that stream/{run_date}/, batch/, and master/ exist."
        )
        if ctx and LOG_AVAILABLE:
            pipeline_log.warn(ctx, f"No data found for {run_date}")
        return snapshot(store)

    for partition_key, files in sorted(all_partitions.items()):
        obs_logger.info(f"  Partition: {partition_key}  ({len(files)} file(s))")

        for file_stem, table in files.items():
            file_name = stem_to_filename(file_stem)
            section   = section_for(file_name)
            schema    = load_schema(metadata_path, section, file_name)

            if not schema:
                obs_logger.warning(
                    f"    No schema for {file_name} (section='{section}') — skipped"
                )
                if ctx and LOG_AVAILABLE:
                    pipeline_log.warn(
                        ctx, f"No schema for {file_name} in '{section}'",
                        source=file_name,
                    )
                continue

            obs_logger.info(
                f"    Validating {file_name} "
                f"({table.num_rows} rows, {len(schema)} col-defs) …"
            )

            t0         = time.perf_counter()
            result     = validate_table(table, schema, file_name=file_name, ctx=ctx)
            elapsed_ms = (time.perf_counter() - t0) * 1000

            store = record_file(store, result, elapsed_ms=elapsed_ms)
            m     = store["files"][-1]

            status = "CLEAN" if result.is_clean else f"ISSUES={result.issue_count}"
            obs_logger.info(
                f"    → {status}  rows={m['total_rows']}  "
                f"null={m['null_issues']}  type={m['type_issues']}  "
                f"enum={m['enum_issues']}  range={m['range_issues']}  "
                f"format={m['format_issues']}  [{elapsed_ms:.1f}ms]"
            )

    snap = snapshot(store)
    log_metrics(snap, ctx)

    report_path = run_log_dir / f"validation_metrics_{run_id}.json"
    write_report(snap, report_path)
    obs_logger.info(f"Metrics report saved: {report_path}")

    if ctx and LOG_AVAILABLE:
        pipeline_log.finalize(ctx)

    if snap["files_failed"] > 0:
        send_failure_alert(alerts_cfg, smtp_password, run_id, snap)
        obs_logger.warning(
            f"Run {run_id} finished with FAILURES: "
            f"{snap['files_failed']} file(s) had issues"
        )
    else:
        send_success_alert(alerts_cfg, smtp_password, run_id, snap)
        obs_logger.info(f"Run {run_id} finished CLEAN ✓")

    obs_logger.info("=" * 60)
    return snap


def parse_args():
    p = argparse.ArgumentParser(
        description="FastFeast — stream + batch + master validation pipeline"
    )
    p.add_argument("--date",        default=date.today().strftime("%Y-%m-%d"),
                   help="Run date YYYY-MM-DD (default: today)")
    p.add_argument("--hour",        type=int, default=None,
                   help="Stream hour 0-23; omit to process all stream hours")
    p.add_argument("--data-root",   default=str(DATAROOT),
                   help="Path to FASTFEAST_200/data (contains stream/batch/master)")
    p.add_argument("--alert-email", default=None,
                   help="Alert recipient email (overrides config.yaml)")
    p.add_argument("--stream-only", action="store_true",
                   help="Validate stream files only, skip batch and master")
    return p.parse_args()


if __name__ == "__main__":
    args    = parse_args()
    summary = run_bronze_validation(
        run_date    = args.date,
        hour        = args.hour,
        stream_only = args.stream_only,
        dataROOT    = args.data_root,
        alert_email = args.alert_email,
    )

    import json
    print("\n── Final summary ──────────────────────────────────")
    print(json.dumps(summary, indent=2))