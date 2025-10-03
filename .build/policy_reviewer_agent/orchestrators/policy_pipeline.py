"""
Orchestrator that coordinates S3 validation, Textract job start, and result persistence.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, Optional

from ..core.exceptions import ValidationError
from ..core.settings import Settings
from ..services.file_utils import is_pdf_key, is_under_prefix
from ..services.s3_client import S3Client
from ..services.textract_client import TextractClient
from ..services.result_persistor import ResultPersistor
from ..services.audit import Auditor

logger = logging.getLogger(__name__)


@dataclass
class IngestResult:
    """Return type for ingest operation."""
    job_id: str
    source_key: str
    mode: str  # "text" or "analysis"


class PolicyPipeline:
    """Coordinates the microservices workflow for Textract."""

    def __init__(self, cfg: Settings):
        self._cfg = cfg
        self._s3 = S3Client(cfg.s3_bucket)
        self._tx = TextractClient(cfg.aws_region, cfg.textract_sns_topic_arn, cfg.textract_publish_role_arn)
        self._persist = ResultPersistor(self._s3, cfg.policy_output_prefix)
        self._audit = Auditor(self._s3)

    # ---------- INGEST STAGE (S3 -> start Textract) ----------

    def validate_and_start(self, object_key: str, analysis: bool = False) -> IngestResult:
        """
        Validate the S3 key and start an async Textract job.
        Raises ValidationError if key is invalid.
        """
        if not is_under_prefix(object_key, self._cfg.policy_pdf_prefix):
            raise ValidationError(f"Key not under prefix: {object_key} (expected {self._cfg.policy_pdf_prefix})")
        if not is_pdf_key(object_key):
            raise ValidationError(f"Not a PDF: {object_key}")

        mode = "analysis" if analysis else "text"
        if analysis:
            job_id = self._tx.start_analysis(self._cfg.s3_bucket, object_key)
        else:
            job_id = self._tx.start_text_detection(self._cfg.s3_bucket, object_key)

        self._audit.write_event(
            {
                "stage": "ingest_start",
                "key": object_key,
                "status": "STARTED",
                "job_id": job_id,
                "mode": mode,
            }
        )
        return IngestResult(job_id=job_id, source_key=object_key, mode=mode)

    # ---------- CALLBACK STAGE (SNS -> fetch & persist) ----------

    def fetch_and_persist(self, job_id: str, mode: str, source_key: str) -> dict:
        """
        Fetch all Textract pages (paginated) and persist to S3, then audit.
        Returns manifest.
        """
        if mode == "analysis":
            pages = self._tx.get_analysis_results(job_id)
        else:
            pages = self._tx.get_text_results(job_id)

        manifest = self._persist.persist_text_results(job_id, source_key, pages)
        self._audit.write_event(
            {
                "stage": "ingest_complete",
                "key": source_key,
                "status": "SUCCESS",
                "job_id": job_id,
                "output_prefix": self._cfg.policy_output_prefix,
            }
        )
        return manifest
