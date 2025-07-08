"""Unified interface for all storage types (filesystem and object storage)."""

from .storage_operations import (
    StorageMetrics,
    analyze_storage,
    list_storage_contents,
    verify_storage_access,
)

__all__ = [
    "StorageMetrics",
    "analyze_storage",
    "list_storage_contents",
    "verify_storage_access",
]
