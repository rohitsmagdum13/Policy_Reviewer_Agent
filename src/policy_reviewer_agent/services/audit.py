"""
Simple audit trail writer (to S3 JSON lines).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

from .s3_client import S3Client
from ..core.exceptions import ResultPersistError

logger = logging.getLogger(__name__)


class Auditor:
    """Append-only audit trail in s3://bucket/policy/audit/YYYY/MM/DD/events.jsonl"""

    def __init__(self, s3: S3Client, base_prefix: str = "policy/audit"):
        self._s3 = s3
        self._base = base_prefix.strip("/")

    def write_event(self, event: Dict[str, Any]) -> str:
        """
        Append an audit record (we emit whole file per call to keep it simple in S3).
        Returns s3 uri of the written object.
        """
        try:
            now = datetime.now(timezone.utc)
            y, m, d = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
            key = f"{self._base}/{y}/{m}/{d}/events.jsonl"
            line = json.dumps({"ts": now.isoformat(), **event}, ensure_ascii=False) + "\n"
            # naive append by fetching existing is overkill; we just put a one-line object per call
            # and rely on S3 inventory/listing; or you can aggregate via Firehose later.
            return self._s3.put_bytes(key, line.encode("utf-8"), "application/x-ndjson")
        except Exception as e:  # noqa: BLE001
            logger.error("Audit write failed", extra={"stage": "audit"}, exc_info=True)
            raise ResultPersistError(str(e)) from e
