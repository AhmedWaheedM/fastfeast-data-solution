import os
import logging
import logging.handlers
import structlog

LOG_DIR      = os.getenv("FF_LOG_DIR",          "logs")
MAX_BYTES    = int(os.getenv("FF_LOG_MAX_BYTES", str(5 * 1024 * 1024)))
BACKUP_COUNT = int(os.getenv("FF_LOG_BACKUPS",   "7"))


# ── configure structlog processors ───────────────────────
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


def _setup(name: str, filename: str):
    os.makedirs(LOG_DIR, exist_ok=True)

    stdlib = logging.getLogger(name)
    if stdlib.handlers:          # if already configured
        return structlog.get_logger(name)

    stdlib.setLevel(logging.DEBUG)
    stdlib.propagate = False

    fmt = logging.Formatter("%(message)s")

    # rotating file
    fh = logging.handlers.RotatingFileHandler(
        os.path.join(LOG_DIR, filename),
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    stdlib.addHandler(fh)

    # console
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    stdlib.addHandler(ch)

    return structlog.get_logger(name)


# ── two loggers ─────────────────────────────────────
pipeline   = _setup("pipeline",   "pipeline.log")
validation = _setup("validation", "validation.log")
