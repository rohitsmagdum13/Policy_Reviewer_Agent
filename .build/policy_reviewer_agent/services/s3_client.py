"""
S3 client wrapper: scoped get/put and simple utilities.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from ..core.exceptions import S3ReadError, S3WriteError

logger = logging.getLogger(__name__)


class S3Client:
    """Thin S3 wrapper for common operations used by the pipeline."""

    def __init__(self, bucket: str):
        self._bucket = bucket
        self._s3 = boto3.client("s3")

    @property
    def bucket(self) -> str:
        return self._bucket

    def put_json(self, key: str, payload: Dict[str, Any]) -> str:
        """Write JSON to s3://bucket/key; returns s3 URI."""
        try:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self._s3.put_object(Bucket=self._bucket, Key=key, Body=body, ContentType="application/json")
            uri = f"s3://{self._bucket}/{key}"
            logger.info("Wrote JSON", extra={"stage": "s3_put", "key": key, "status": "OK"})
            return uri
        except (ClientError, BotoCoreError) as e:
            logger.error("S3 put_json failed", extra={"stage": "s3_put", "key": key}, exc_info=True)
            raise S3WriteError(str(e)) from e

    def copy_object(self, src_key: str, dst_key: str) -> None:
        """Server-side copy within same bucket."""
        try:
            self._s3.copy_object(Bucket=self._bucket, Key=dst_key, CopySource={"Bucket": self._bucket, "Key": src_key})
            logger.info("Copied object", extra={"stage": "s3_copy", "key": dst_key, "status": "OK"})
        except (ClientError, BotoCoreError) as e:
            logger.error("S3 copy failed", extra={"stage": "s3_copy", "key": dst_key}, exc_info=True)
            raise S3WriteError(str(e)) from e

    def put_bytes(self, key: str, data: bytes, content_type: Optional[str] = None) -> str:
        """Write bytes; returns s3 URI."""
        try:
            kwargs: Dict[str, Any] = {"Bucket": self._bucket, "Key": key, "Body": data}
            if content_type:
                kwargs["ContentType"] = content_type
            self._s3.put_object(**kwargs)
            uri = f"s3://{self._bucket}/{key}"
            logger.info("Wrote bytes", extra={"stage": "s3_put", "key": key, "status": "OK"})
            return uri
        except (ClientError, BotoCoreError) as e:
            logger.error("S3 put_bytes failed", extra={"stage": "s3_put", "key": key}, exc_info=True)
            raise S3WriteError(str(e)) from e
