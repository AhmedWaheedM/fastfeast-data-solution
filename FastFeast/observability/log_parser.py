from __future__ import annotations
import re
import json
import glob
import os
from datetime import datetime
from pathlib import Path
from FastFeast.pipeline.config import config as cfg_loader
import pyarrow as pa
import pyarrow.compute as pc


LOG_PATTERN = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"
    r"\s{2}(?P<level>\w+)\s+"
    r"\[(?P<logger>[^\]]+)\]\s+"
    r"(?P<message>.+)$"
)

LEVEL_ORDER = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "WARN": 2, "ERROR": 3, "CRITICAL": 4}

LEVEL_COLOURS = {
    "DEBUG":    "#6c757d",
    "INFO":     "#0d6efd",
    "WARNING":  "#ffc107",
    "WARN":     "#ffc107",
    "ERROR":    "#dc3545",
    "CRITICAL": "#6f0000",
}

LOG_SCHEMA = pa.schema([
    pa.field("timestamp",   pa.timestamp("s")),
    pa.field("level",       pa.string()),
    pa.field("logger",      pa.string()),
    pa.field("message",     pa.string()),
    pa.field("level_rank",  pa.int8()),
    pa.field("colour",      pa.string()),
    pa.field("source_file", pa.string()),
])

SUMMARY_SCHEMA = pa.schema([
    pa.field("run_id",       pa.string()),
    pa.field("recorded_at",  pa.timestamp("s")),
    pa.field("files_total",  pa.int64()),
    pa.field("files_clean",  pa.int64()),
    pa.field("files_failed", pa.int64()),
    pa.field("total_rows",   pa.int64()),
    pa.field("total_issues", pa.int64()),
    pa.field("clean_rate",   pa.float64()),
    pa.field("source_file",  pa.string()),
])

PER_FILE_SCHEMA = pa.schema([
    pa.field("run_id",        pa.string()),
    pa.field("file_name",     pa.string()),
    pa.field("total_rows",    pa.int64()),
    pa.field("issue_count",   pa.int64()),
    pa.field("is_clean",      pa.bool_()),
    pa.field("null_issues",   pa.int64()),
    pa.field("type_issues",   pa.int64()),
    pa.field("enum_issues",   pa.int64()),
    pa.field("range_issues",  pa.int64()),
    pa.field("format_issues", pa.int64()),
    pa.field("missing_cols",  pa.int64()),
    pa.field("elapsed_ms",    pa.float64()),
])


def find_log_files(log_root):
    pattern = os.path.join(log_root, "**", "*.log")
    files   = glob.glob(pattern, recursive=True)
    return sorted(files, key=os.path.getmtime, reverse=True)


def find_metric_files(log_root):
    pattern = os.path.join(log_root, "**", "validation_metrics_*.json")
    files   = glob.glob(pattern, recursive=True)
    return sorted(files, key=os.path.getmtime, reverse=True)


def find_jsonl_files(log_root):
    pattern = os.path.join(log_root, "**", "*.jsonl")
    files   = glob.glob(pattern, recursive=True)
    return sorted(files, key=os.path.getmtime, reverse=True)


def parse_log_line(line):
    line = line.rstrip()
    if not line:
        return None
    m = LOG_PATTERN.match(line)
    if not m:
        return None
    return {
        "timestamp": m.group("timestamp"),
        "level":     m.group("level").upper(),
        "logger":    m.group("logger"),
        "message":   m.group("message").strip(),
    }


def parse_log_file(path, tail=None):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except OSError:
        return _empty_log_table()

    if tail:
        lines = lines[-tail:]

    rows = [parse_log_line(ln) for ln in lines]
    rows = [r for r in rows if r is not None]

    if not rows:
        return _empty_log_table()

    timestamps  = []
    levels      = []
    loggers     = []
    messages    = []
    level_ranks = []
    colours     = []
    source_file = os.path.basename(path)

    for r in rows:
        try:
            ts = datetime.fromisoformat(r["timestamp"])
        except ValueError:
            ts = None
        timestamps.append(ts)

        lv = r["level"]
        levels.append(lv)
        loggers.append(r["logger"])
        messages.append(r["message"])
        level_ranks.append(LEVEL_ORDER.get(lv, 1))
        colours.append(LEVEL_COLOURS.get(lv, "#6c757d"))

    table = pa.table(
        {
            "timestamp":   pa.array(timestamps,                    type=pa.timestamp("s")),
            "level":       pa.array(levels,                        type=pa.string()),
            "logger":      pa.array(loggers,                       type=pa.string()),
            "message":     pa.array(messages,                      type=pa.string()),
            "level_rank":  pa.array(level_ranks,                   type=pa.int8()),
            "colour":      pa.array(colours,                       type=pa.string()),
            "source_file": pa.array([source_file] * len(rows),    type=pa.string()),
        },
        schema=LOG_SCHEMA,
    )

    sort_idx = pc.sort_indices(table, sort_keys=[("timestamp", "descending")])
    return table.take(sort_idx)


def parse_multiple_log_files(paths, tail_per_file=None):
    from FastFeast.pipeline.config import config as cfg_loader
    if tail_per_file is None:
        try:
            cfg           = cfg_loader.load(str(Path(__file__).resolve().parents[2] / "config" / "config.yaml"))
            tail_per_file = cfg.log_parser.tail_per_file
        except Exception:
            tail_per_file = 500

    tables = [parse_log_file(p, tail=tail_per_file) for p in paths]
    tables = [t for t in tables if t.num_rows > 0]
    if not tables:
        return _empty_log_table()

    combined = pa.concat_tables(tables)
    sort_idx = pc.sort_indices(combined, sort_keys=[("timestamp", "descending")])
    return combined.take(sort_idx)

def _empty_log_table():
    return pa.table(
        {col: pa.array([], type=LOG_SCHEMA.field(col).type) for col in LOG_SCHEMA.names},
        schema=LOG_SCHEMA,
    )


def parse_jsonl_file(path):
    rows = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except OSError:
        pass

    if not rows:
        return pa.table({})

    all_keys        = list(dict.fromkeys(k for row in rows for k in row))
    columns         = {k: [] for k in all_keys}

    for row in rows:
        for k in all_keys:
            columns[k].append(row.get(k))

    arrays = {}
    for k, vals in columns.items():
        try:
            arrays[k] = pa.array(vals)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            arrays[k] = pa.array([str(v) if v is not None else None for v in vals])

    table = pa.table(arrays)

    if "timestamp" in table.schema.names:
        ts_col = table.column("timestamp")
        try:
            ts_cast = pc.strptime(ts_col.cast(pa.string()), format="%Y-%m-%dT%H:%M:%S", unit="s")
        except Exception:
            ts_cast = ts_col
        idx   = table.schema.get_field_index("timestamp")
        table = table.set_column(idx, "timestamp", ts_cast)

    return table


def parse_metric_file(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        data["_source_file"] = os.path.basename(path)
        data["_source_path"] = path
        return data
    except (OSError, json.JSONDecodeError):
        return {}


def load_all_metrics(paths):
    results = [parse_metric_file(p) for p in paths]
    return [r for r in results if r]


def metrics_summary_table(metrics_list):
    if not metrics_list:
        return pa.table(
            {col: pa.array([], type=SUMMARY_SCHEMA.field(col).type)
             for col in SUMMARY_SCHEMA.names},
            schema=SUMMARY_SCHEMA,
        )

    run_ids       = []
    recorded_ats  = []
    files_totals  = []
    files_cleans  = []
    files_faileds = []
    total_rows_l  = []
    total_issues_l= []
    clean_rates   = []
    source_files  = []

    for m in metrics_list:
        files_total = m.get("files_total", 0)
        files_clean = m.get("files_clean", 0)
        ts_str      = m.get("recorded_at", "")
        try:
            ts = datetime.fromisoformat(ts_str) if ts_str else None
        except ValueError:
            ts = None

        run_ids.append(str(m.get("run_id", "?")))
        recorded_ats.append(ts)
        files_totals.append(int(files_total))
        files_cleans.append(int(files_clean))
        files_faileds.append(int(m.get("files_failed", 0)))
        total_rows_l.append(int(m.get("total_rows", 0)))
        total_issues_l.append(int(m.get("total_issues", 0)))
        clean_rates.append(round(files_clean / max(files_total, 1) * 100, 1))
        source_files.append(str(m.get("_source_file", "")))

    table = pa.table(
        {
            "run_id":       pa.array(run_ids,        type=pa.string()),
            "recorded_at":  pa.array(recorded_ats,   type=pa.timestamp("s")),
            "files_total":  pa.array(files_totals,   type=pa.int64()),
            "files_clean":  pa.array(files_cleans,   type=pa.int64()),
            "files_failed": pa.array(files_faileds,  type=pa.int64()),
            "total_rows":   pa.array(total_rows_l,   type=pa.int64()),
            "total_issues": pa.array(total_issues_l, type=pa.int64()),
            "clean_rate":   pa.array(clean_rates,    type=pa.float64()),
            "source_file":  pa.array(source_files,   type=pa.string()),
        },
        schema=SUMMARY_SCHEMA,
    )

    sort_idx = pc.sort_indices(table, sort_keys=[("recorded_at", "descending")])
    return table.take(sort_idx)


def metrics_summary_df(metrics_list):
    return metrics_summary_table(metrics_list).to_pandas()


def per_file_issues_table(metrics_list):
    if not metrics_list:
        return pa.table(
            {col: pa.array([], type=PER_FILE_SCHEMA.field(col).type)
             for col in PER_FILE_SCHEMA.names},
            schema=PER_FILE_SCHEMA,
        )

    cols = {name: [] for name in PER_FILE_SCHEMA.names}

    for m in metrics_list:
        run_id = str(m.get("run_id", "?"))
        for pf in m.get("per_file", []):
            cols["run_id"].append(run_id)
            cols["file_name"].append(str(pf.get("file_name",     "")))
            cols["total_rows"].append(int(pf.get("total_rows",    0)))
            cols["issue_count"].append(int(pf.get("issue_count",  0)))
            cols["is_clean"].append(bool(pf.get("is_clean",       True)))
            cols["null_issues"].append(int(pf.get("null_issues",  0)))
            cols["type_issues"].append(int(pf.get("type_issues",  0)))
            cols["enum_issues"].append(int(pf.get("enum_issues",  0)))
            cols["range_issues"].append(int(pf.get("range_issues",0)))
            cols["format_issues"].append(int(pf.get("format_issues", 0)))
            cols["missing_cols"].append(int(pf.get("missing_cols",0)))
            cols["elapsed_ms"].append(float(pf.get("elapsed_ms",  0.0)))

    if not cols["run_id"]:
        return pa.table(
            {col: pa.array([], type=PER_FILE_SCHEMA.field(col).type)
             for col in PER_FILE_SCHEMA.names},
            schema=PER_FILE_SCHEMA,
        )

    return pa.table(
        {name: pa.array(vals, type=PER_FILE_SCHEMA.field(name).type)
         for name, vals in cols.items()},
        schema=PER_FILE_SCHEMA,
    )


def per_file_issues_df(metrics_list):
    return per_file_issues_table(metrics_list).to_pandas()


def count_by_level(table):
    if table.num_rows == 0 or "level" not in table.schema.names:
        return {}
    level_col     = table.column("level")
    unique_levels = level_col.unique().to_pylist()
    result        = {}
    for lv in unique_levels:
        if lv is not None:
            mask        = pc.equal(level_col, lv)
            result[lv]  = int(pc.sum(mask.cast(pa.int64())).as_py())
    return result


def filter_log_table(
    table,
    min_level      = "DEBUG",
    logger_filter  = "",
    message_filter = "",
    last_n         = None,
):

    if last_n is None:
        try:
            cfg    = cfg_loader.load(str(Path(__file__).resolve().parents[2] / "config" / "config.yaml"))
            last_n = cfg.log_parser.filter_last_n
        except Exception:
            last_n = 200

    if table.num_rows == 0:
        return table

    min_rank = LEVEL_ORDER.get(min_level.upper(), 0)
    mask     = pc.greater_equal(table.column("level_rank"), min_rank)
    table    = table.filter(mask)

    if logger_filter and table.num_rows > 0:
        match_mask = pc.match_substring(table.column("logger"), logger_filter, ignore_case=True)
        table      = table.filter(match_mask)

    if message_filter and table.num_rows > 0:
        match_mask = pc.match_substring(table.column("message"), message_filter, ignore_case=True)
        table      = table.filter(match_mask)

    return table.slice(0, last_n)


def filter_log_df(table, min_level="DEBUG", logger_filter="", message_filter="", last_n=None):
    return filter_log_table(table, min_level, logger_filter, message_filter, last_n).to_pandas()


def extract_partition_from_message(message):
    m = re.search(r"Partition:\s*(\S+)", message)
    if m:
        return m.group(1)
    return ""


def extract_file_result(message):
    m = re.search(
        r"→\s*(CLEAN|ISSUES=(\d+))\s+rows=(\d+).*?\[(\d+(?:\.\d+)?)ms\]",
        message,
    )
    if not m:
        return None
    return {
        "is_clean":    m.group(1) == "CLEAN",
        "issue_count": int(m.group(2) or 0),
        "rows":        int(m.group(3)),
        "elapsed_ms":  float(m.group(4)),
    }