"""A library of datasets tools with a corresponding CLI."""

__version__ = "0.1.0"

from .path import (
    path_stats,
    LocalPathStats,
    RemotePathStats,
    subfolders,
)

from .permission_verifier import (
    PermissionVerifierFactory,
    FilesystemType,
    S3Config,
)

__all__ = [
    "path_stats",
    "LocalPathStats",
    "RemotePathStats",
    "subfolders",
    "PermissionVerifierFactory",
    "FilesystemType",
    "S3Config",
]
