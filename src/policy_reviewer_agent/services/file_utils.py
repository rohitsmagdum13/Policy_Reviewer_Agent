"""
Lightweight file utilities: key validation, suffix checks.
"""

from __future__ import annotations

import mimetypes


def is_pdf_key(object_key: str) -> bool:
    """True if the object key looks like a PDF."""
    return object_key.lower().endswith(".pdf")


def is_under_prefix(object_key: str, expected_prefix: str) -> bool:
    """True if object_key is under the expected prefix."""
    p = expected_prefix if expected_prefix.endswith("/") else (expected_prefix + "/")
    return object_key.startswith(p)


def guess_mime_from_key(object_key: str) -> str | None:
    """Best-effort MIME type based on filename."""
    mt, _enc = mimetypes.guess_type(object_key)
    return mt
