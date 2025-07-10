"""Unified storage operations that work across filesystem and object storage."""

from dataclasses import dataclass
from typing import Optional

from ds_tools.core import get_logger
from ds_tools.core.exceptions import ValidationError
from ds_tools.filesystem import (
    analyze_local_directory,
    analyze_remote_directory,
    list_local_subdirectories,
    list_remote_subdirectories,
)
from ds_tools.filesystem.permissions import FilesystemType, verify_directory_access
from ds_tools.objectstorage.s3_operations import (
    analyze_prefix,
    list_objects_by_prefix,
    verify_s3_access,
)
from ds_tools.schemas import (
    NFS4StorageConfig,
    NFSStorageConfig,
    S3StorageConfig,
    SSHStorageConfig,
    StorageConfig,
)

logger = get_logger(__name__)


@dataclass(frozen=True)
class StorageMetrics:
    """Unified metrics for any storage type."""
    item_count: int
    total_bytes: int
    storage_type: str
    location: str


def analyze_storage(
    path: str,
    config: StorageConfig,
    timeout: int = 300,
) -> StorageMetrics:
    """
    Analyze storage to get item count and total size.

    Args:
        path: Storage path
        config: Storage configuration (NFSStorageConfig, NFS4StorageConfig,
            SSHStorageConfig, or S3StorageConfig)
        timeout: Operation timeout in seconds

    Returns:
        StorageMetrics with unified format

    Raises:
        ValidationError: If storage operation fails
    """
    logger.info("Analyzing storage", path=path, storage_type=config.type)

    try:
        if isinstance(config, S3StorageConfig):
            metrics = analyze_prefix(
                s3_path=path,
                access_key_id=config.access_key_id,
                secret_access_key=config.secret_access_key,
                session_token=config.session_token,
                region_name=config.region_name or "us-east-1",
                endpoint_url=config.endpoint_url,
                aws_profile=config.aws_profile,
            )
            return StorageMetrics(
                item_count=metrics.object_count,
                total_bytes=metrics.total_bytes,
                storage_type="s3",
                location=path,
            )

        elif isinstance(config, SSHStorageConfig):
            metrics = analyze_remote_directory(
                hostname=config.hostname,
                username=config.username,
                ssh_key=config.ssh_key_path,
                path=path,
                timeout=timeout,
            )
            return StorageMetrics(
                item_count=metrics.file_count,
                total_bytes=metrics.total_bytes,
                storage_type="ssh",
                location=path,
            )

        else:  # NFSStorageConfig or NFS4StorageConfig
            metrics = analyze_local_directory(path, timeout)
            return StorageMetrics(
                item_count=metrics.file_count,
                total_bytes=metrics.total_bytes,
                storage_type=config.type,
                location=path,
            )

    except Exception as e:
        error_msg = f"Failed to analyze storage '{path}': {e}"
        logger.error(error_msg, error=str(e))
        raise ValidationError(error_msg)


def list_storage_contents(
    path: str,
    config: StorageConfig,
    content_type: str = "subdirectories",
    max_items: int = 1000,
    timeout: int = 300,
) -> list[str]:
    """
    List storage contents (subdirectories/prefixes or files/objects).

    Args:
        path: Storage path
        config: Storage configuration (NFSStorageConfig, NFS4StorageConfig,
            SSHStorageConfig, or S3StorageConfig)
        content_type: "subdirectories" for dirs/prefixes, "files" for files/objects
        max_items: Maximum number of items to return
        timeout: Operation timeout in seconds

    Returns:
        List of paths to subdirectories/prefixes or files/objects

    Raises:
        ValidationError: If parameters are invalid or operation fails
    """
    if content_type not in ("subdirectories", "files"):
        raise ValidationError(
            f"content_type must be 'subdirectories' or 'files', got: {content_type}"
        )

    logger.info(
        "Listing storage contents",
        path=path,
        storage_type=config.type,
        content_type=content_type,
    )

    try:
        if isinstance(config, S3StorageConfig):
            if content_type == "subdirectories":
                list_type = "prefixes"
            else:
                list_type = "objects"

            return list_objects_by_prefix(
                s3_path=path,
                list_type=list_type,
                access_key_id=config.access_key_id,
                secret_access_key=config.secret_access_key,
                session_token=config.session_token,
                region_name=config.region_name or "us-east-1",
                endpoint_url=config.endpoint_url,
                aws_profile=config.aws_profile,
                max_keys=max_items,
            )

        elif isinstance(config, SSHStorageConfig):
            if content_type == "files":
                raise ValidationError("File listing not implemented for SSH storage")

            return list_remote_subdirectories(
                hostname=config.hostname,
                username=config.username,
                ssh_key=config.ssh_key_path,
                path=path,
                timeout=timeout,
            )

        else:  # NFSStorageConfig or NFS4StorageConfig
            if content_type == "files":
                raise ValidationError("File listing not implemented for NFS storage")

            return list_local_subdirectories(path, timeout)

    except Exception as e:
        error_msg = f"Failed to list storage contents '{path}': {e}"
        logger.error(error_msg, error=str(e))
        raise ValidationError(error_msg)


def verify_storage_access(
    path: str,
    config: StorageConfig,
    operation: str = "read",
    timeout: int = 300,
    username: Optional[str] = None,
) -> bool:
    """
    Verify access to storage location.

    Args:
        path: Storage path
        config: Storage configuration (NFSStorageConfig, NFS4StorageConfig,
            SSHStorageConfig, or S3StorageConfig)
        operation: Operation to test ("read", "write", "list")
        timeout: Operation timeout in seconds
        username: Username to check access for (local filesystem only)

    Returns:
        True if access is permitted

    Raises:
        ValidationError: If parameters are invalid or operation fails
    """
    logger.info(
        "Verifying storage access",
        path=path,
        storage_type=config.type,
        operation=operation,
    )

    try:
        if isinstance(config, S3StorageConfig):
            return verify_s3_access(
                s3_path=path,
                operation=operation,
                access_key_id=config.access_key_id,
                secret_access_key=config.secret_access_key,
                session_token=config.session_token,
                region_name=config.region_name or "us-east-1",
                endpoint_url=config.endpoint_url,
                aws_profile=config.aws_profile,
            )

        elif isinstance(config, SSHStorageConfig):
            try:
                # Test access by attempting to list the directory
                list_storage_contents(
                    path=path,
                    config=config,
                    content_type="subdirectories",
                    max_items=1,
                    timeout=timeout,
                )

                if operation in ("read", "list"):
                    return True
                elif operation == "write":
                    logger.warning(
                        "SSH write access verification requested but not fully "
                        "implemented",
                        path=path,
                    )
                    return True
                else:
                    raise ValidationError(f"Unknown operation: {operation}")

            except ValidationError:
                raise
            except (PermissionError, FileNotFoundError):
                logger.info("SSH access denied or path not found", path=path)
                return False
            except Exception as e:
                logger.warning("SSH access check failed", path=path, error=str(e))
                return False

        else:  # NFSStorageConfig or NFS4StorageConfig
            if not username:
                raise ValidationError(
                    "Username required for NFS filesystem access verification"
                )

            if operation in ("read", "list"):
                if isinstance(config, NFSStorageConfig):
                    return verify_directory_access(FilesystemType.nfs, path, username)
                elif isinstance(config, NFS4StorageConfig):
                    return verify_directory_access(FilesystemType.nfs4, path, username)
                else:
                    return verify_directory_access(FilesystemType.nfs, path, username)
            elif operation == "write":
                raise ValidationError(
                    "Write permission verification is not implemented for "
                    "NFS filesystem. Consider implementing write permission checks."
                )
            else:
                raise ValidationError(f"Unknown operation: {operation}")

    except Exception as e:
        error_msg = f"Failed to verify storage access '{path}': {e}"
        logger.error(error_msg, error=str(e))
        raise ValidationError(error_msg)
