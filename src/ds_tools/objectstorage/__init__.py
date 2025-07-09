"""Object storage operations for S3-compatible services."""

from .s3_operations import (
    PrefixMetrics,
    S3ClientConfig,
    S3ClientManager,
    analyze_prefix,
    analyze_s3_prefix,
    list_objects_by_prefix,
    list_s3_objects,
    list_s3_prefixes,
    verify_s3_access,
)

__all__ = [
    "PrefixMetrics",
    "S3ClientConfig",
    "S3ClientManager",
    "analyze_prefix",
    "analyze_s3_prefix",
    "list_objects_by_prefix",
    "list_s3_prefixes",
    "list_s3_objects",
    "verify_s3_access",
]
