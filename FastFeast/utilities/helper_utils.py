import uuid
from datetime import datetime, timezone


def now_iso() -> str:
    """Returns the current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def short_id() -> str:
    """Returns a short, 8-character unique identifier."""
    return str(uuid.uuid4())[:8]
