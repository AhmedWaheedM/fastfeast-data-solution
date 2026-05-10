import os
import structlog

from FastFeast.observability.logger import setup_logger

LOG_DIR = os.getenv("FF_LOG_DIR", "logs")
MAX_BYTES = int(os.getenv("FF_LOG_MAX_BYTES", str(5 * 1024 * 1024)))
BACKUP_COUNT = int(os.getenv("FF_LOG_BACKUPS", "7"))
LOG_LEVEL = os.getenv("FF_LOG_LEVEL", "INFO")


structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)


def _setup(name: str):
    setup_logger(
        name=name,
        log_dir=LOG_DIR,
        level=LOG_LEVEL,
        max_bytes=MAX_BYTES,
        backup_count=BACKUP_COUNT,
    )
    return structlog.get_logger(name)


pipeline = _setup("pipeline")
validation = _setup("validation")
