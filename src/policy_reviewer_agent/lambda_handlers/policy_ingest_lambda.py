"""
Lambda entrypoint for S3:ObjectCreated events on policy/pdf/.
- Validates object key
- Starts async Textract (text detection by default)
- Writes audit

Environment: see core.settings for required variables.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict

from ..core.logging_config import setup_logging
from ..core.settings import Settings
from ..core.exceptions import ConfigError, ValidationError
from ..orchestrators.policy_pipeline import PolicyPipeline

setup_logging()
logger = logging.getLogger(__name__)


def _extract_s3_key(event: Dict[str, Any]) -> str:
    """
    Extract the S3 object key from the event. Raises KeyError if malformed.
    """
    record = event["Records"][0]
    return record["s3"]["object"]["key"]


def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler.
    Returns a JSON-friendly dict describing the started job.
    """
    try:
        cfg = Settings.from_env()
    except Exception as e:  # noqa: BLE001
        logger.error("Configuration error", exc_info=True)
        raise ConfigError(str(e)) from e

    pipeline = PolicyPipeline(cfg)

    # Decide analysis vs text detection. You can use key-based rules if needed.
    # Here we default to text detection; toggle to analysis by naming convention if desired.
    try:
        object_key = _extract_s3_key(event)
        result = pipeline.validate_and_start(object_key, analysis=False)
        logger.info(
            "Ingest started",
            extra={"stage": "ingest", "key": object_key, "job_id": result.job_id, "status": "STARTED"},
        )
        return {"job_id": result.job_id, "key": object_key, "mode": result.mode}
    except ValidationError:
        logger.warning("Validation failed", extra={"stage": "ingest"}, exc_info=True)
        # Swallowing to avoid S3 retry storm; or re-raise for DLQ based on your policy.
        return {"error": "validation_failed"}
    except Exception as e:  # noqa: BLE001
        logger.error("Unhandled error in ingest", extra={"stage": "ingest"}, exc_info=True)
        raise
