"""
FastFeast — Configuration Loader
==================================
Loads config.yaml into typed dataclasses.
Secrets (smtp_password) are injected from the environment — never stored
in config.yaml.  Use a .env file for local development.

    pip install python-dotenv pyyaml
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import yaml

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass   # python-dotenv is optional; os.environ still works


# ─────────────────────────────────────────────────────────────────────────────
# DATACLASSES  (one per yaml section)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Database:
    type:      str
    name:      str
    file:      str
    read_only: bool


@dataclass
class Paths:
    # FIX #1: added data_dir — was missing, causing AttributeError on load
    # which silenced the entire config block (including alert email/SMTP creds)
    data_dir:       str
    bronze_dir:     str
    master_dir:     str
    batch_dir:      str
    stream_dir:     str
    output_dir:     str
    log_dir:        str
    quarantine_dir: str
    report_dir:     str
    alert_dir:      str
    checkpoint_dir: str
    metadata_file:  str


@dataclass
class Pipeline:
    batch_size:     int
    retry_attempts: int
    log_level:      str
    mode:           str
    max_workers:    int
    sleep_hours:    float
    max_attempts:   int


@dataclass
class Export:
    enabled:      bool
    format:       str
    target_layer: str


@dataclass
class DatetimeHandling:
    extract_date_key:        bool
    extract_time_key:        bool
    date_key_format:         str
    time_key_format:         str
    keep_original_timestamp: bool


@dataclass
class Logging:
    level:        str
    rotate:       str   # "daily" | "size"
    backup_count: int
    max_bytes:    int
    fmt:          str
    datefmt:      str


@dataclass
class Alerts:
    email:         str
    on_fail:       bool
    smtp_host:     str
    smtp_port:     int
    smtp_user:     str
    slack_webhook: str   
    max_queue_size:       int 
    send_timeout_sec:     int 
    shutdown_timeout_sec: int 

@dataclass
class Stream:
    poll_interval_sec: int
    file_pattern:      list


@dataclass
class Batch:
    schedule:          str
    file_pattern:      list
    max_files_per_run: int


@dataclass
class Threshold:
    max_open:     int
    max_response: int


@dataclass
class LogParser:
    tail_per_file:  500
    filter_last_n:  200

@dataclass
class Settings:
    database:          Database
    paths:             Paths
    pipeline:          Pipeline
    export:            Export
    datetime_handling: DatetimeHandling
    logging:           Logging
    alerts:            Alerts
    stream:            Stream
    batch:             Batch
    threshold:         Threshold
    log_parser:        LogParser = field(default_factory=LogParser)
    smtp_password: str = field(default="", repr=False)


# ─────────────────────────────────────────────────────────────────────────────
# LOADER
# ─────────────────────────────────────────────────────────────────────────────

def load(path: str) -> Settings:

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    return Settings(
        database          = Database(**data["database"]),
        paths             = Paths(**data["paths"]),
        pipeline          = Pipeline(**data["pipeline"]),
        export            = Export(**data["export"]),
        datetime_handling = DatetimeHandling(**data["datetime_handling"]),
        logging           = Logging(**data["logging"]),
        alerts            = Alerts(**data["alerts"]),
        stream            = Stream(**data["stream"]),
        batch             = Batch(**data["batch"]),
        threshold         = Threshold(**data["threshold"]),
        smtp_password     = os.environ.get("FASTFEAST_SMTP_PASSWORD", ""),
        log_parser    = LogParser(**data["log_parser"]) if "log_parser" in data else LogParser(),
    )
import os
from pathlib import Path

DEFAULT_CONFIG_PATH = Path(__file__).parent / "config.yaml"
config_settings = load(str(DEFAULT_CONFIG_PATH))