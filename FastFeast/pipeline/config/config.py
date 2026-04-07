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
from pathlib import Path
import os
from dacite import from_dict, Config

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
<<<<<<< HEAD
    type:      str
    name:      str
    file:      str
=======
    type: str
    db_name: str
    file: str
>>>>>>> origin/dev
    read_only: bool


@dataclass
class Paths:
<<<<<<< HEAD
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

=======
    master_dir: str
    batch_dir: str
    dest_base: str
    stream_dir: str
    output_dir: str
    log_file: str
    quarantine_file: str
    report_file : str
    alert_file : str
    check_point_file: str
    metadata_yaml: str
>>>>>>> origin/dev

@dataclass
class Pipeline:
    batch_size:     int
    retry_attempts: int
<<<<<<< HEAD
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
=======
    max_workers: int
    log_level: str
    mode: str
    time_wait: int
    sleep_hours: int
    max_attempts: int

# @dataclass
# class Format:
#     date: Datetime

>>>>>>> origin/dev


@dataclass
class DatetimeHandling:
    extract_date_key:        bool
    extract_time_key:        bool
    date_key_format:         str
    time_key_format:         str
    keep_original_timestamp: bool
    date_time: str


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
<<<<<<< HEAD
class Stream:
    poll_interval_sec: int
    file_pattern:      list


@dataclass
class Batch:
    schedule:          str
    file_pattern:      list
    max_files_per_run: int

=======
class SupportedTypes:
   csv: str
   json: str

@dataclass
class Batch:
    schedule: str
    timeout: int
    supported_types: SupportedTypes
    max_files_per_run: int
    encoding: str

@dataclass
class Stream:
   poll_interval_sec: int     
   supported_types: SupportedTypes
>>>>>>> origin/dev

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
<<<<<<< HEAD
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
=======
    database: Database
    paths: Paths
    pipeline: Pipeline
    datetime_handling: Datetime
    logging: Logging
    alerts: Alerts
    stream: Stream
    batch: Batch
    threshold: Threshold
>>>>>>> origin/dev


# ─────────────────────────────────────────────────────────────────────────────
# LOADER
# ─────────────────────────────────────────────────────────────────────────────

def load(path: str) -> Settings:

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

<<<<<<< HEAD
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
=======
    return from_dict(
        data_class=Settings,
        data=data,
        config=Config(strict=True) 
    )

#Set path of config.yaml and call load function
yaml_path = Path(Path(__file__).parent / "config.yaml")
#print("YAMLLLLLLLLLLLLLLLL", yaml_path)
>>>>>>> origin/dev

config_settings = load(yaml_path)


# ------------------------------------------------------
# Lazy Config Loader
# ------------------------------------------------------
_config = None

def get_config():
    global _config
    if _config is None:
        _config = load(yaml_path)
    return _config
