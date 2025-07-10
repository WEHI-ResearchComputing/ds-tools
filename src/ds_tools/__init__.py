"""A library of tools and integrations for dynamic UIs and complex workflows.

This package provides a unified interface for storage operations across different
backends including local filesystems, remote SSH connections, and S3-compatible
object storage. It's designed for integration with the WEHI Datasets Service.

Key Features:
    - Unified storage operations API
    - Multi-backend support (local, SSH, S3)
    - Storage analysis and metrics
    - Access verification
    - CLI interface

Recommended Usage:
    Use the unified interface from this module for most operations:

    >>> from ds_tools import list_storage_contents, SSHStorageConfig
    >>> config = SSHStorageConfig(
    ...     hostname="server.com", username="user", ssh_key_path="/path/to/key"
    ... )
    >>> contents = list_storage_contents("/data/path", config)

Advanced Usage:
    Import specific modules for advanced operations:

    >>> from ds_tools.filesystem import LocalDirectoryAnalyzer
    >>> from ds_tools.objectstorage import S3PrefixAnalyzer
"""

__version__ = "0.1.0"

# Storage configuration schemas
# Individual modules (for advanced usage)
from .filesystem import (
    DirectoryMetrics,
    FilesystemType,
    calculate_directory_metrics,
    list_subdirectories,
)
from .objectstorage import (
    PrefixMetrics,
    S3ClientConfig,
    analyze_prefix,
    list_objects_by_prefix,
    verify_s3_access,
)
from .schemas import (
    NFS4StorageConfig,
    NFSStorageConfig,
    S3StorageConfig,
    SSHStorageConfig,
    StorageConfig,
)

# Unified interface (recommended)
from .unified import (
    StorageMetrics,
    analyze_storage,
    list_storage_contents,
    verify_storage_access,
)

__all__ = [
    # Storage configurations
    "NFS4StorageConfig",
    "NFSStorageConfig",
    "S3StorageConfig",
    "SSHStorageConfig",
    "StorageConfig",
    # Unified interface
    "StorageMetrics",
    "analyze_storage",
    "list_storage_contents",
    "verify_storage_access",
    # Filesystem-specific
    "DirectoryMetrics",
    "FilesystemType",
    "calculate_directory_metrics",
    "list_subdirectories",
    # Object storage-specific
    "PrefixMetrics",
    "S3ClientConfig",
    "analyze_prefix",
    "list_objects_by_prefix",
    "verify_s3_access",
]
