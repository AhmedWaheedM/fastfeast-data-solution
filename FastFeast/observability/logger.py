from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path


def setup_logger(
    name           = "fastfeast",
    log_dir        = "logs",
    level          = "INFO",
    max_bytes      = 5242880,
    backup_count   = 7,
    timed_rotation = False,
    when           = "midnight",
    fmt            = "%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
    datefmt        = "%Y-%m-%dT%H:%M:%S",
):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_path    = Path(log_dir) / f"{name}.log"
    console_lvl = getattr(logging, level.upper(), logging.INFO)
    formatter   = logging.Formatter(fmt=fmt, datefmt=datefmt)

    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    if timed_rotation:
        fh = logging.handlers.TimedRotatingFileHandler(
            filename    = str(log_path),
            when        = when,
            backupCount = backup_count,
            encoding    = "utf-8",
        )
    else:
        fh = logging.handlers.RotatingFileHandler(
            filename    = str(log_path),
            maxBytes    = max_bytes,
            backupCount = backup_count,
            encoding    = "utf-8",
        )

    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(console_lvl)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger


def get_logger(name):
    return logging.getLogger(name)