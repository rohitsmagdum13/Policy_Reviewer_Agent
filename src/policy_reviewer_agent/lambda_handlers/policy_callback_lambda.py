"""
Lambda entrypoint for Textract-completion SNS notifications.

- Parses SNS -> Textract JobId and Status
- Uses a simple JobTag (source object key) strategy:
  If you prefer, set `JobTag` when starting jobs and read it back here;
  otherwise store a lightweight (job_id -> key, mode) mapping in an audit KV (S3/DB).

For simplicity, we pull source_key from the SNS message's `DocumentLocation` when available.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from ..core.logging_config import setup_logging
from ..core.settings import Settings
from ..core.exceptions import ConfigError, TextractJobError
from ..orchestrators.policy_pipeline import PolicyPipeline

setup_logging()
logger = logging.getLogger(__name__)


def _parse_sns(event: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Extract JobId, Status, and (best effort) source_key from the SNS message.
    """
    record = event["Records"][0]
    msg_str = record["Sns"]["Message"]
    msg = json.loads(msg_str)

    job_id = msg.get("JobId")
    status = msg.get("Status")
    # Attempt to reconstruct source key from the message
    source_key = None
    if "DocumentLocation" in msg and "S3ObjectName" in msg["DocumentLocation"]:
        source_key = msg["DocumentLocation"]["S3ObjectName"]
    # Mode is not included by default; if you used FeatureTypes, treat as "analysis"
    # Here we default to "text"; change if you strictly separate topics per mode.
    mode = "text"
    return {"job_id": job_id, "status": status, "source_key": source_key, "mode": mode}


def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler for SNS -> fetch & persist.
    """
    try:
        cfg = Settings.from_env()
    except Exception as e:  # noqa: BLE001
        logger.error("Configuration error", exc_info=True)
        raise ConfigError(str(e)) from e

    pipeline = PolicyPipeline(cfg)
    meta = _parse_sns(event)

    job_id = meta["job_id"]
    status = meta["status"]
    source_key = meta["source_key"] or "<unknown>"
    mode = meta["mode"] or "text"

    if status != "SUCCEEDED":
        # Log and exit; you can extend to handle FAILED or PARTIAL_SUCCESS as needed.
        logger.error(
            "Textract job did not succeed",
            extra={"stage": "callback", "job_id": job_id, "key": source_key, "status": status},
        )
        return {"status": "ignored", "reason": f"status={status}"}

    if not job_id:
        logger.error("Missing JobId in SNS", extra={"stage": "callback"})
        raise TextractJobError("Missing JobId in SNS message")

    manifest = pipeline.fetch_and_persist(job_id, mode, source_key)
    logger.info(
        "Callback complete",
        extra={"stage": "callback", "job_id": job_id, "key": source_key, "status": "SUCCESS"},
    )
    return {"status": "ok", "manifest": manifest}
