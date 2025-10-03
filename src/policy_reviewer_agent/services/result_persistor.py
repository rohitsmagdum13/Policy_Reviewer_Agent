"""
Result persistence: write raw Textract JSON pages and a compact index to S3.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Dict, Iterable, List

from .s3_client import S3Client
from ..core.exceptions import ResultPersistError

logger = logging.getLogger(__name__)


class ResultPersistor:
    """Saves Textract outputs to s3://bucket/{output_prefix}/..."""

    def __init__(self, s3: S3Client, output_prefix: str):
        self._s3 = s3
        self._out = output_prefix.rstrip("/")

    def persist_text_results(self, job_id: str, source_key: str, pages: Iterable[Dict]) -> Dict:
        """
        Persist each page as JSON and an index.json for quick lookup.
        Returns a manifest dict with URIs.
        """
        try:
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            base_prefix = f"{self._out}/{ts}/{job_id}"
            page_uris: List[str] = []
            for i, page in enumerate(pages, start=1):
                key = f"{base_prefix}/pages/page_{i:04d}.json"
                uri = self._s3.put_bytes(key, json.dumps(page, ensure_ascii=False).encode("utf-8"), "application/json")
                page_uris.append(uri)

            manifest = {
                "job_id": job_id,
                "source_key": source_key,
                "pages": page_uris,
                "created_utc": ts,
            }
            self._s3.put_json(f"{base_prefix}/index.json", manifest)
            logger.info("Persisted results", extra={"stage": "persist", "job_id": job_id, "key": base_prefix, "status": "OK"})
            return manifest
        except Exception as e:  # noqa: BLE001
            logger.error("Persist failed", extra={"stage": "persist", "job_id": job_id}, exc_info=True)
            raise ResultPersistError(str(e)) from e
