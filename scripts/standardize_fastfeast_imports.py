#!/usr/bin/env python3
"""Standardize imports under FastFeast/ to absolute FastFeast.* imports.

Usage (from repository root):
  python scripts/standardize_fastfeast_imports.py           # dry run
  python scripts/standardize_fastfeast_imports.py --apply   # write changes

What it rewrites:
- from pipeline...           -> from FastFeast.pipeline...
- from utilities...          -> from FastFeast.utilities...
- from support...            -> from FastFeast.support...
- from dwh...                -> from FastFeast.dwh...
- from orchestration...      -> from FastFeast.orchestration...
- from data_generation...    -> from FastFeast.data_generation...
- from observability...      -> from FastFeast.observability...

Special remap:
- pipeline.observability.*   -> FastFeast.observability.*

Also updates import references:
- metadata_cash -> metadata_cache (import paths only)
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable

BASE_MODULES = (
    "pipeline",
    "utilities",
    "support",
    "dwh",
    "orchestration",
    "data_generation",
)


def _normalize_metadata_segment(module_path: str) -> str:
    parts = module_path.split(".")
    parts = ["metadata_cache" if p == "metadata_cash" else p for p in parts]
    return ".".join(parts)


def _remap_module(module_path: str) -> str:
    module_path = _normalize_metadata_segment(module_path)

    if module_path.startswith("FastFeast."):
        return module_path

    if module_path == "observability" or module_path.startswith("observability."):
        return "FastFeast." + module_path

    if module_path.startswith("pipeline.observability"):
        suffix = module_path[len("pipeline.observability") :]
        return "FastFeast.observability" + suffix

    for root in BASE_MODULES:
        if module_path == root or module_path.startswith(root + "."):
            return "FastFeast." + module_path

    return module_path


def _rewrite_from_import_line(line: str) -> str:
    # Example: from pipeline.config import get_config
    m = re.match(r"^(\s*from\s+)([A-Za-z_][\w\.]*)(\s+import\s+.*)$", line)
    if not m:
        return line

    before, module_path, after = m.groups()
    new_module_path = _remap_module(module_path)
    if new_module_path == module_path:
        return line
    return f"{before}{new_module_path}{after}"


def _rewrite_import_line(line: str) -> str:
    # Example: import pipeline.config as cfg, utilities.file_utils
    m = re.match(r"^(\s*import\s+)(.+)$", line)
    if not m:
        return line

    prefix, tail = m.groups()
    code, hash_sep, comment = tail.partition("#")

    chunks = [c.strip() for c in code.split(",")]
    new_chunks: list[str] = []
    changed = False

    for chunk in chunks:
        if not chunk:
            continue

        # Keep alias form: "module.path as alias"
        alias_match = re.match(r"^([A-Za-z_][\w\.]*)(\s+as\s+[A-Za-z_][\w]*)?$", chunk)
        if not alias_match:
            new_chunks.append(chunk)
            continue

        module_path, alias = alias_match.groups()
        new_module_path = _remap_module(module_path)
        if new_module_path != module_path:
            changed = True

        new_chunks.append(new_module_path + (alias or ""))

    rebuilt = prefix + ", ".join(new_chunks)
    if hash_sep:
        rebuilt += hash_sep + comment

    if not changed:
        return line
    return rebuilt


def rewrite_text(text: str) -> str:
    out_lines: list[str] = []
    for line in text.splitlines(keepends=True):
        # Preserve original EOL exactly to avoid merging adjacent import lines.
        line_core = line.rstrip("\r\n")
        line_eol = line[len(line_core):]

        new_line = line_core
        stripped = line_core.lstrip()

        if stripped.startswith("from "):
            new_line = _rewrite_from_import_line(line_core)
        elif stripped.startswith("import "):
            new_line = _rewrite_import_line(line_core)

        out_lines.append(new_line + line_eol)

    return "".join(out_lines)


def iter_python_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        yield path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Write updates to disk")
    parser.add_argument(
        "--root",
        default="FastFeast",
        help="Root package directory to process (default: FastFeast)",
    )
    args = parser.parse_args()

    repo_root = Path.cwd()
    target_root = (repo_root / args.root).resolve()

    if not target_root.exists():
        print(f"Target root not found: {target_root}")
        return 2

    changed_files: list[Path] = []

    for py_file in iter_python_files(target_root):
        original = py_file.read_text(encoding="utf-8")
        rewritten = rewrite_text(original)

        if rewritten != original:
            changed_files.append(py_file)
            if args.apply:
                py_file.write_text(rewritten, encoding="utf-8")

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"[{mode}] Changed files: {len(changed_files)}")
    for path in changed_files:
        rel = path.relative_to(repo_root)
        print(f" - {rel.as_posix()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
