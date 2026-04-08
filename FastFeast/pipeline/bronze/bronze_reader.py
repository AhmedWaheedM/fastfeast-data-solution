from __future__ import annotations

import io
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.json as pa_json

try:
    from FastFeast.observability import logger as pipeline_log
    LOG_AVAILABLE = True
except ImportError:
    LOG_AVAILABLE = False
    pipeline_log = None


def log_info(ctx, msg, **kw):
    if LOG_AVAILABLE and ctx:
        pipeline_log.info(ctx, msg, **kw)

def log_warn(ctx, msg, **kw):
    if LOG_AVAILABLE and ctx:
        pipeline_log.warn(ctx, msg, **kw)

def log_error(ctx, msg, **kw):
    if LOG_AVAILABLE and ctx:
        pipeline_log.error(ctx, msg, **kw)


def read_json_file(path):
    raw = path.read_bytes()

    try:
        return pa_json.read_json(io.BytesIO(raw))
    except Exception:
        pass

    records = json.loads(raw)
    if isinstance(records, dict):
        records = [records]

    if not records:
        return pa.table({})

    keys   = list(records[0].keys())
    arrays = {}
    for k in keys:
        vals = [r.get(k) for r in records]
        try:
            arrays[k] = pa.array(vals)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            arrays[k] = pa.array(
                [str(v) if v is not None else None for v in vals],
                type=pa.string(),
            )
    return pa.table(arrays)


def read_csv_file(path):
    opts = pa_csv.ConvertOptions(
        null_values=["", "NULL", "null", "None", "NA", "N/A"],
        strings_can_be_null=True,
        quoted_strings_can_be_null=True,
    )
    return pa_csv.read_csv(
        str(path),
        read_options=pa_csv.ReadOptions(encoding="utf-8"),
        parse_options=pa_csv.ParseOptions(delimiter=","),
        convert_options=opts,
    )


SUPPORTED_EXTENSIONS = {".json", ".csv"}


def read_file(path, ctx=None):
    path = Path(path)

    if not path.exists():
        log_error(ctx, f"File not found: {path}", source=str(path),
                  issue_type="missing_file")
        return pa.table({})

    if path.stat().st_size == 0:
        log_warn(ctx, f"Empty file skipped: {path.name}", source=str(path))
        return pa.table({})

    ext = path.suffix.lower()
    if ext not in SUPPORTED_EXTENSIONS:
        log_warn(ctx, f"Unsupported extension '{ext}': {path.name}",
                 source=str(path))
        return pa.table({})

    try:
        table = read_json_file(path) if ext == ".json" else read_csv_file(path)
        log_info(ctx, f"Read {table.num_rows:,} rows from {path.name}",
                 source=str(path), table=path.stem)
        return table
    except Exception as exc:
        log_error(ctx, f"Failed to read {path.name}: {exc}", source=str(path))
        return pa.table({})


def read_stream_hour(run_date, hour, stream_root="data/stream", ctx=None):
    hour_str = f"{int(hour):02d}"
    folder   = Path(stream_root) / run_date / hour_str

    if not folder.exists():
        log_warn(ctx, f"Stream hour folder not found: {folder}")
        return {}

    results = {}
    for fpath in sorted(folder.iterdir()):
        if fpath.suffix.lower() in SUPPORTED_EXTENSIONS:
            table = read_file(fpath, ctx=ctx)
            if table.num_columns > 0:
                results[fpath.stem] = table

    log_info(ctx,
             f"Stream {run_date}/{hour_str}: loaded {len(results)} file(s)",
             table="stream_hour")
    return results


def read_stream_all(stream_root="data/stream", ctx=None):
    stream_root = Path(stream_root)
    all_data    = {}

    if not stream_root.exists():
        log_error(ctx, f"Stream root not found: {stream_root}")
        return {}

    for date_dir in sorted(stream_root.iterdir()):
        if not date_dir.is_dir():
            continue
        for hour_dir in sorted(date_dir.iterdir()):
            if not hour_dir.is_dir():
                continue
            key  = f"{date_dir.name}/{hour_dir.name}"
            data = read_stream_hour(date_dir.name, hour_dir.name,
                                    stream_root=stream_root, ctx=ctx)
            if data:
                all_data[key] = data

    log_info(ctx, f"Stream scan complete: {len(all_data)} partition(s) found")
    return all_data


def read_batch(batch_root="data/batch", ctx=None):
    batch_root = Path(batch_root)

    if not batch_root.exists():
        log_warn(ctx, f"Batch root not found: {batch_root}")
        return {}

    results = {}
    for fpath in sorted(batch_root.iterdir()):
        if fpath.is_file() and fpath.suffix.lower() in SUPPORTED_EXTENSIONS:
            table = read_file(fpath, ctx=ctx)
            if table.num_columns > 0:
                results[fpath.stem] = table

    log_info(ctx, f"Batch: loaded {len(results)} file(s)", table="batch")
    return results


def read_master(master_root="data/master", ctx=None):
    master_root = Path(master_root)

    if not master_root.exists():
        log_warn(ctx, f"Master root not found: {master_root}")
        return {}

    results = {}
    for fpath in sorted(master_root.iterdir()):
        if fpath.is_file() and fpath.suffix.lower() in SUPPORTED_EXTENSIONS:
            table = read_file(fpath, ctx=ctx)
            if table.num_columns > 0:
                results[fpath.stem] = table

    log_info(ctx, f"Master: loaded {len(results)} file(s)", table="master")
    return results


def read_all_sources(data_root, run_date, hour=None, include_batch=True, ctx=None):
    data_root   = Path(data_root)
    stream_root = data_root / "stream"
    batch_root  = data_root / "batch"
    master_root = data_root / "master"

    all_data = {}

    if hour is not None:
        stream_files = read_stream_hour(run_date, hour,
                                        stream_root=stream_root, ctx=ctx)
        if stream_files:
            all_data[f"stream/{run_date}/{int(hour):02d}"] = stream_files
    else:
        for partition_key, files in read_stream_all(stream_root, ctx=ctx).items():
            if partition_key.startswith(run_date):
                all_data[f"stream/{partition_key}"] = files

    if include_batch:
        batch_files = read_batch(batch_root, ctx=ctx)
        if batch_files:
            all_data["batch"] = batch_files

        master_files = read_master(master_root, ctx=ctx)
        if master_files:
            all_data["master"] = master_files

    log_info(ctx,
             f"All sources loaded: {len(all_data)} partition(s) "
             f"({sum(len(v) for v in all_data.values())} total files)")
    return all_data


def read_bronze_hour(run_date, hour, bronze_root="data/stream", ctx=None):
    return read_stream_hour(run_date, hour, stream_root=bronze_root, ctx=ctx)

def read_bronze_all(bronze_root="data/stream", ctx=None):
    return read_stream_all(stream_root=bronze_root, ctx=ctx)