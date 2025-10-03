"""
Structured logging setup (CloudWatch-friendly).
"""

import json
import logging
import os
from typing import Any, Dict


class JsonFormatter(logging.Formatter):
    """JSON log formatter with stable keys."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        payload: Dict[str, Any] = {
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        # Attach contextual fields if the caller added them to `record.__dict__`
        for k in ("stage", "key", "job_id", "status", "latency_ms"):
            if k in record.__dict__:
                payload[k] = record.__dict__[k]
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(level: str | int = None) -> None:
    """Initialize root logger with JSON formatting."""
    root = logging.getLogger()
    if not level:
        level = os.getenv("LOG_LEVEL", "INFO")
    root.setLevel(level)
    # Clear existing handlers (e.g., when re-importing in AWS Lambda warm starts)
    root.handlers.clear()
    h = logging.StreamHandler()
    h.setFormatter(JsonFormatter())
    root.addHandler(h)
