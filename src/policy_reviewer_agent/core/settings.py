"""
Settings loader for Policy Reviewer Textract pipeline.

- Loads config from environment (Lambda) or .env (local).
- Validates required keys.
- Exposes a frozen, typed dataclass for easy/explicit usage.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

try:
    # Safe: present in local dev; absent in Lambda (where env vars are already set)
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    # No-op if python-dotenv isn't available in Lambda
    pass


@dataclass(frozen=True)
class Settings:
    aws_region: str
    s3_bucket: str
    policy_pdf_prefix: str
    policy_output_prefix: str
    textract_sns_topic_arn: Optional[str] = None
    textract_publish_role_arn: Optional[str] = None

    @staticmethod
    def from_env() -> "Settings":
        """Construct settings from environment variables. Raises on missing keys."""
        def req(k: str) -> str:
            v = os.getenv(k)
            if not v:
                raise ValueError(f"Missing required environment variable: {k}")
            return v

        return Settings(
            aws_region=req("AWS_REGION"),
            s3_bucket=req("S3_BUCKET"),
            policy_pdf_prefix=req("POLICY_PDF_PREFIX"),
            policy_output_prefix=req("POLICY_OUTPUT_PREFIX"),
            textract_sns_topic_arn=os.getenv("TEXTRACT_SNS_TOPIC_ARN"),
            textract_publish_role_arn=os.getenv("TEXTRACT_PUBLISH_ROLE_ARN"),
        )
