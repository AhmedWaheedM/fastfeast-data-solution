from dataclasses import dataclass
import yaml


@dataclass
class Database:
    type: str
    name: str
    file: str
    read_only: bool


@dataclass
class Paths:
    master_dir: str
    batch_dir: str
    stream_dir: str
    output_dir: str
    log_file: str
    quarantine_file: str
    report_file : str
    alert_file : str
    check_point_file: str

@dataclass
class Pipeline:
    batch_size: int
    retry_attempts: int
    log_level: str
    mode: str

@dataclass
class Format:
    file_type: str

@dataclass
class Datetime:
    extract_date_key: bool
    extract_time_key: bool
    date_key_format: str
    time_key_format: str
    keep_original_timestamp: bool

@dataclass
class Logging:
  rotate: str
  backup_count: int
  level: str

@dataclass
class Alerts:
  email: str
  on_fail: bool

@dataclass
class Stream:
  poll_interval_sec: int     
  file_pattern: str 

@dataclass
class Batch:
  schedule: str
  file_pattern: str 
  max_files_per_run: int 

@dataclass
class Settings:
    database: Database
    paths: Paths
    pipeline: Pipeline
    format: Format
    datetime_handling: Datetime
    logging: Logging
    alerts: Alerts
    stream: Stream
    batch: Batch


def load(path: str) -> Settings:
    with open(path, "r") as f:
        data = yaml.safe_load(f)

    return Settings(
        database=Database(**data["database"]),
        paths=Paths(**data["paths"]),
        pipeline=Pipeline(**data["pipeline"]),
        format=Format(**data["format"]),
        datetime_handling=Datetime(**data["datetime_handling"]),
        logging=Logging(**data["logging"]),
        alerts=Alerts(**data["alerts"]),
        stream=Stream(**data["stream"]),
        batch=Batch(**data["batch"])
    )