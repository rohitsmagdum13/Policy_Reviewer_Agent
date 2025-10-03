"""
Typed exceptions to keep failure modes explicit and testable.
"""

class ConfigError(RuntimeError):
    """Configuration missing/invalid."""


class ValidationError(RuntimeError):
    """Bad input (e.g., wrong S3 prefix or non-PDF)."""


class S3ReadError(RuntimeError):
    """S3 read/list failures."""


class S3WriteError(RuntimeError):
    """S3 write/put failures."""


class TextractJobError(RuntimeError):
    """Failed to start or fetch Textract job/results."""


class ResultPersistError(RuntimeError):
    """Failed to persist outputs/audit."""
