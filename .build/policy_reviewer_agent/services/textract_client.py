"""
Amazon Textract client wrapper for async jobs and result pagination.
"""

from __future__ import annotations

import logging
from typing import Dict, Iterable, List, Optional, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from ..core.exceptions import TextractJobError

logger = logging.getLogger(__name__)


class TextractClient:
    """
    Encapsulates async Textract flows:
    - start_document_text_detection or start_document_analysis
    - get_document_text_detection / get_document_analysis pagination
    """

    def __init__(self, region: str, sns_topic_arn: Optional[str] = None, publish_role_arn: Optional[str] = None):
        self._region = region
        self._sns_topic_arn = sns_topic_arn
        self._publish_role_arn = publish_role_arn
        self._client = boto3.client("textract", region_name=region)

    def start_text_detection(self, bucket: str, key: str) -> str:
        """
        Start async text detection and return JobId.
        If SNS is configured, set NotificationChannel so SNS → Lambda callback fires.
        """
        try:
            kwargs: Dict = {
                "DocumentLocation": {"S3Object": {"Bucket": bucket, "Name": key}},
            }
            if self._sns_topic_arn and self._publish_role_arn:
                kwargs["NotificationChannel"] = {
                    "SNSTopicArn": self._sns_topic_arn,
                    "RoleArn": self._publish_role_arn,
                }
            resp = self._client.start_document_text_detection(**kwargs)
            job_id = resp["JobId"]
            logger.info("Started Textract text detection", extra={"stage": "textract_start", "key": key, "job_id": job_id})
            return job_id
        except (ClientError, BotoCoreError) as e:
            logger.error("Textract start failed", extra={"stage": "textract_start", "key": key}, exc_info=True)
            raise TextractJobError(str(e)) from e

    def start_analysis(self, bucket: str, key: str, feature_types: Optional[List[str]] = None) -> str:
        """Start async document analysis (FORMS/TABLES)."""
        feature_types = feature_types or ["FORMS", "TABLES"]
        try:
            kwargs: Dict = {
                "DocumentLocation": {"S3Object": {"Bucket": bucket, "Name": key}},
                "FeatureTypes": feature_types,
            }
            if self._sns_topic_arn and self._publish_role_arn:
                kwargs["NotificationChannel"] = {
                    "SNSTopicArn": self._sns_topic_arn,
                    "RoleArn": self._publish_role_arn,
                }
            resp = self._client.start_document_analysis(**kwargs)
            job_id = resp["JobId"]
            logger.info("Started Textract analysis", extra={"stage": "textract_start", "key": key, "job_id": job_id})
            return job_id
        except (ClientError, BotoCoreError) as e:
            logger.error("Textract start failed", extra={"stage": "textract_start", "key": key}, exc_info=True)
            raise TextractJobError(str(e)) from e

    def get_text_results(self, job_id: str) -> Iterable[Dict]:
        """Yield all pages from GetDocumentTextDetection."""
        token: Optional[str] = None
        try:
            while True:
                args = {"JobId": job_id}
                if token:
                    args["NextToken"] = token
                resp = self._client.get_document_text_detection(**args)
                yield resp
                token = resp.get("NextToken")
                if not token:
                    break
        except (ClientError, BotoCoreError) as e:
            logger.error("Textract get results failed", extra={"stage": "textract_get", "job_id": job_id}, exc_info=True)
            raise TextractJobError(str(e)) from e

    def get_analysis_results(self, job_id: str) -> Iterable[Dict]:
        """Yield all pages from GetDocumentAnalysis."""
        token: Optional[str] = None
        try:
            while True:
                args = {"JobId": job_id}
                if token:
                    args["NextToken"] = token
                resp = self._client.get_document_analysis(**args)
                yield resp
                token = resp.get("NextToken")
                if not token:
                    break
        except (ClientError, BotoCoreError) as e:
            logger.error("Textract get results failed", extra={"stage": "textract_get", "job_id": job_id}, exc_info=True)
            raise TextractJobError(str(e)) from e
