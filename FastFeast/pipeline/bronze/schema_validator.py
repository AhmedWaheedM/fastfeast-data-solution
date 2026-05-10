from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import yaml


try:
    from FastFeast.observability import logger as pipeline_log
    LOG_AVAILABLE = True
except ImportError:
    LOG_AVAILABLE = False
    pipeline_log = None


@dataclass
class ColumnIssue:
    column: str
    check:  str          
    count:  int = 0
    detail: str = ""


@dataclass
class ValidationResult:
    file_name:  str
    row_count:  int
    issues:     list[ColumnIssue] = field(default_factory=list)
    bad_row_indices: list[int]    = field(default_factory=list)

    @property
    def is_clean(self):
        return len(self.issues) == 0

    @property
    def issue_count(self):
        return sum(i.count for i in self.issues)

    def summary(self):
        return {
            "file":        self.file_name,
            "rows":        self.row_count,
            "is_clean":    self.is_clean,
            "issue_count": self.issue_count,
            "issues": [
                {"column": i.column, "check": i.check,
                 "count": i.count, "detail": i.detail}
                for i in self.issues
            ],
        }


def load_schema(metadata_path, section, file_name):
    with open(metadata_path, "r", encoding="utf-8") as f:
        meta = yaml.safe_load(f)

    for entry in meta.get(section, []):
        if entry.get("file_name") == file_name:
            return entry.get("columns", [])
    return []


EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PHONE_RE = re.compile(r"^\+?[\d\s\-().]{7,20}$")

FORMAT_CHECKS = {
    "email": EMAIL_RE,
    "phone": PHONE_RE,
}

TYPE_MAP = {
    "integer":   pa.int64(),
    "float":     pa.float64(),
    "varchar":   pa.string(),
    "boolean":   pa.bool_(),
    "timestamp": pa.timestamp("us"),
}


def validate_table(table, schema, file_name="unknown", ctx=None):
    result = ValidationResult(file_name=file_name, row_count=table.num_rows)
    actual_cols = set(table.schema.names)

    for col_def in schema:
        col_name = col_def["name"]
        nullable = col_def.get("nullable", True)

        if col_name not in actual_cols:
            issue = ColumnIssue(col_name, "missing", count=1,
                                detail="column absent from table")
            result.issues.append(issue)
            emit_issue(ctx, file_name, issue)
            continue

        col_arr = table[col_name]

        if not nullable:
            null_count = col_arr.null_count
            if null_count > 0:
                issue = ColumnIssue(col_name, "null", count=null_count,
                                    detail=f"{null_count} null(s) in non-nullable column")
                result.issues.append(issue)
                emit_issue(ctx, file_name, issue)

        declared_type = col_def.get("type", "varchar")
        arrow_type    = TYPE_MAP.get(declared_type)
        if arrow_type and col_arr.type != arrow_type:
            bad = count_bad_cast(col_arr, arrow_type)
            if bad > 0:
                issue = ColumnIssue(col_name, "type", count=bad,
                                    detail=f"cannot cast {bad} value(s) to {declared_type}")
                result.issues.append(issue)
                emit_issue(ctx, file_name, issue)

        expected = col_def.get("expected_values")
        if expected:
            bad = count_bad_enum(col_arr, expected)
            if bad > 0:
                issue = ColumnIssue(col_name, "enum", count=bad,
                                    detail=f"{bad} value(s) not in {expected}")
                result.issues.append(issue)
                emit_issue(ctx, file_name, issue)

        range_def = col_def.get("range")
        if range_def:
            bad = count_out_of_range(col_arr, range_def)
            if bad > 0:
                lo = range_def.get("min", "—")
                hi = range_def.get("max", "—")
                issue = ColumnIssue(col_name, "range", count=bad,
                                    detail=f"{bad} value(s) outside [{lo}, {hi}]")
                result.issues.append(issue)
                emit_issue(ctx, file_name, issue)

        fmt = col_def.get("format")
        if fmt and fmt in FORMAT_CHECKS:
            bad = count_bad_format(col_arr, FORMAT_CHECKS[fmt])
            if bad > 0:
                issue = ColumnIssue(col_name, "format", count=bad,
                                    detail=f"{bad} value(s) fail {fmt} format")
                result.issues.append(issue)
                emit_issue(ctx, file_name, issue)

    if LOG_AVAILABLE and ctx:
        if result.is_clean:
            pipeline_log.info(ctx,
                f"Validation CLEAN: {file_name} ({result.row_count} rows)",
                source=file_name, table=Path(file_name).stem)
        else:
            pipeline_log.warn(ctx,
                f"Validation found {result.issue_count} issue(s) in {file_name}",
                source=file_name, table=Path(file_name).stem,
                issue_type="schema_mismatch", count=result.issue_count)

    return result


def count_bad_cast(arr, target_type):
    if arr.type == target_type:
        return 0
    try:
        pc.cast(arr, target_type, safe=True)
        return 0
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        pass
    bad = 0
    for val in arr:
        if val.is_valid:
            try:
                pc.cast(pa.array([val.as_py()]), target_type, safe=True)
            except Exception:
                bad += 1
    return bad


def count_bad_enum(arr, allowed):
    allowed_set = set(str(v) for v in allowed)
    bad = 0
    for val in arr:
        if val.is_valid:
            if str(val.as_py()) not in allowed_set:
                bad += 1
    return bad


def count_out_of_range(arr, range_def):
    lo = range_def.get("min")
    hi = range_def.get("max")
    bad = 0
    for val in arr:
        if val.is_valid:
            v = val.as_py()
            try:
                v = float(v)
                if lo is not None and v < lo:
                    bad += 1
                elif hi is not None and v > hi:
                    bad += 1
            except (TypeError, ValueError):
                bad += 1
    return bad


def count_bad_format(arr, pattern):
    bad = 0
    for val in arr:
        if val.is_valid:
            s = str(val.as_py())
            if not pattern.match(s):
                bad += 1
    return bad


def emit_issue(ctx, file_name, issue):
    if not (LOG_AVAILABLE and ctx):
        return
    pipeline_log.log_quality_issue(
        ctx,
        issue_type=check_to_issue_type(issue.check),
        message=issue.detail,
        table=Path(file_name).stem,
        column=issue.column,
        count=issue.count,
        source=file_name,
    )


CHECK_ISSUE_MAP = {
    "missing": "schema_mismatch",
    "null":    "null_value",
    "type":    "schema_mismatch",
    "enum":    "invalid_format",
    "range":   "out_of_range",
    "format":  "invalid_format",
}

def check_to_issue_type(check):
    return CHECK_ISSUE_MAP.get(check, "unknown")