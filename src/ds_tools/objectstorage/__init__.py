"""Object storage operations for S3-compatible services."""

from .analysis import PrefixMetrics, S3PrefixAnalyzer, analyze_prefix
from .clients import S3ClientConfig, S3ClientManager
from .listing import S3PrefixLister, list_objects_by_prefix
from .permissions import S3AccessVerifier, verify_s3_access

__all__ = [
    "PrefixMetrics",
    "S3PrefixAnalyzer",
    "analyze_prefix",
    "S3ClientConfig",
    "S3ClientManager",
    "S3PrefixLister",
    "list_objects_by_prefix",
    "S3AccessVerifier",
    "verify_s3_access",
]
