"""Unified storage operations that work across filesystem and object storage.

This module provides a single interface for storage operations across different
backends including local filesystems, remote SSH connections, and S3-compatible
object storage. It automatically detects storage type based on path format and
routes operations to the appropriate backend implementation.

The unified interface abstracts away the differences between storage backends,
providing consistent semantics and error handling across all supported storage
types.

Key Functions:
    - analyze_storage: Calculate metrics (file/object count, total size)
    - list_storage_contents: List subdirectories/prefixes or files/objects
    - verify_storage_access: Verify read/write/list permissions

Storage Type Detection:
    Storage type is automatically detected based on path format:
    - s3://bucket/prefix -> S3 storage
    - ssh://user@host/path or user@host:/path -> SSH remote filesystem
    - Any other path -> Local filesystem

Path Format Examples:
    - Local: "/home/user/data", "./relative/path"
    - SSH: "ssh://user@server.com/data", "user@server:/data"
    - S3: "s3://my-bucket/data/", "s3://bucket"

Error Handling:
    All functions provide consistent error handling through ValidationError
    for invalid inputs and configuration issues. Backend-specific errors
    are wrapped in appropriate exception types with descriptive messages.
"""

from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

from ds_tools.core import get_logger
from ds_tools.core.exceptions import ValidationError
from ds_tools.filesystem import (
    LocalDirectoryAnalyzer,
    LocalSubdirectoryLister,
    RemoteDirectoryAnalyzer,
    RemoteSubdirectoryLister,
    calculate_directory_metrics,
    list_subdirectories,
)
from ds_tools.filesystem.permissions import FilesystemType, verify_directory_access
from ds_tools.objectstorage import (
    analyze_prefix,
    list_objects_by_prefix,
    verify_s3_access,
)

logger = get_logger(__name__)


@dataclass(frozen=True)
class StorageMetrics:
    """Unified metrics for any storage type.

    Attributes:
        item_count: Number of files/objects
        total_bytes: Total size in bytes
        storage_type: Type of storage ("local", "remote", "s3")
        location: Original storage location/path
    """

    item_count: int
    total_bytes: int
    storage_type: str
    location: str


def _detect_storage_type(path: str) -> str:
    """Detect storage type from path format.

    Uses path prefixes and patterns to determine the appropriate storage backend.
    This function is critical for routing operations to the correct implementation.

    Args:
        path: Storage path to analyze

    Returns:
        Storage type: "s3", "ssh", or "local"

    Detection Logic:
        - S3: Starts with "s3://"
        - SSH: Starts with "ssh://" OR contains ":" and "/" (scp-style format)
        - Local: Everything else (absolute or relative paths)

    Examples:
        >>> _detect_storage_type("s3://bucket/prefix")
        "s3"
        >>> _detect_storage_type("ssh://user@host/path")
        "ssh"
        >>> _detect_storage_type("user@host:/path")
        "ssh"
        >>> _detect_storage_type("/local/path")
        "local"
    """
    if path.startswith("s3://"):
        return "s3"
    elif path.startswith("ssh://") or (":" in path and "/" in path):
        # SSH format: explicit ssh:// URL or traditional scp format (user@host:/path)
        return "ssh"
    else:
        # Default to local filesystem for all other paths
        return "local"


def _parse_ssh_path(
    path: str, hostname: Optional[str] = None, username: Optional[str] = None
) -> tuple[str, str, str]:
    """Parse SSH path and extract hostname, username, and actual path.

    Handles both modern ssh:// URLs and traditional scp-style paths.
    Parameters can override values extracted from the path.

    Args:
        path: SSH path in various formats
        hostname: Override hostname (takes precedence over path)
        username: Override username (takes precedence over path)

    Returns:
        Tuple of (hostname, username, actual_path)

    Supported Path Formats:
        - ssh://user@host/path -> ("host", "user", "/path")
        - ssh://host/path -> ("host", None, "/path")
        - user@host:/path -> ("host", "user", "/path")
        - host:/path -> ("host", None, "/path")
        - /path (when hostname provided) -> (hostname, username, "/path")

    Note:
        If hostname or username are provided as parameters, they override
        any values extracted from the path. This allows for flexible
        configuration where connection details can come from multiple sources.
    """
    if path.startswith("ssh://"):
        # Parse full SSH URL format
        parsed = urlparse(path)
        if not hostname:
            hostname = parsed.hostname
        if not username:
            username = parsed.username
        actual_path = parsed.path
    else:
        # Handle traditional scp format: user@host:/path or host:/path
        if ":" in path and not hostname:
            user_host, actual_path = path.split(":", 1)
            if "@" in user_host:
                # Extract both username and hostname from user@host
                username, hostname = user_host.split("@", 1)
            else:
                # Only hostname provided
                hostname = user_host
        else:
            # No hostname in path, use the path as-is
            actual_path = path

    return hostname, username, actual_path


def analyze_storage(
    path: str,
    # SSH parameters
    hostname: Optional[str] = None,
    username: Optional[str] = None,
    ssh_key: Optional[str] = None,
    # S3 parameters
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    session_token: Optional[str] = None,
    region_name: str = "us-east-1",
    endpoint_url: Optional[str] = None,
    aws_profile: Optional[str] = None,
    # Common parameters
    timeout: int = 300,
) -> StorageMetrics:
    """Analyze storage to get item count and total size.

    Auto-detects storage type from path format and routes to appropriate backend.

    Args:
        path: Storage path (local path, ssh://user@host/path, or s3://bucket/prefix)
        hostname: SSH hostname (for ssh:// paths or when auto-detection fails)
        username: SSH username (for ssh:// paths or when auto-detection fails)
        ssh_key: SSH private key path (for ssh:// paths or when auto-detection fails)
        access_key_id: AWS access key ID (for s3:// paths)
        secret_access_key: AWS secret access key (for s3:// paths)
        session_token: AWS session token (for s3:// paths)
        region_name: AWS region name (for s3:// paths)
        endpoint_url: Custom S3 endpoint URL (for s3:// paths)
        aws_profile: AWS CLI profile name (for s3:// paths)
        timeout: Command timeout in seconds (for filesystem operations)

    Returns:
        StorageMetrics with unified format

    Raises:
        ValidationError: If storage type cannot be determined or parameters are missing
    """
    storage_type = _detect_storage_type(path)
    logger.info("Analyzing storage", path=path, storage_type=storage_type)

    try:
        if storage_type == "s3":
            metrics = analyze_prefix(
                s3_path=path,
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                session_token=session_token,
                region_name=region_name,
                endpoint_url=endpoint_url,
                aws_profile=aws_profile,
            )
            return StorageMetrics(
                item_count=metrics.object_count,
                total_bytes=metrics.total_bytes,
                storage_type="s3",
                location=path,
            )

        elif storage_type == "ssh":
            hostname, username, actual_path = _parse_ssh_path(path, hostname, username)

            if not all([hostname, username, ssh_key]):
                raise ValidationError(
                    "SSH operations require hostname, username, and ssh_key parameters"
                )

            # Type checker safety: all values guaranteed to be str at this point
            assert hostname is not None and username is not None and ssh_key is not None
            analyzer = RemoteDirectoryAnalyzer(hostname, username, ssh_key)
            metrics = calculate_directory_metrics(analyzer, actual_path, timeout)
            return StorageMetrics(
                item_count=metrics.file_count,
                total_bytes=metrics.total_bytes,
                storage_type="ssh",
                location=path,
            )

        else:  # local
            analyzer = LocalDirectoryAnalyzer()
            metrics = calculate_directory_metrics(analyzer, path, timeout)
            return StorageMetrics(
                item_count=metrics.file_count,
                total_bytes=metrics.total_bytes,
                storage_type="local",
                location=path,
            )

    except Exception as e:
        error_msg = f"Failed to analyze storage '{path}': {e}"
        logger.error(error_msg, error=str(e))
        raise ValidationError(error_msg)


def list_storage_contents(
    path: str,
    content_type: str = "subdirectories",
    # SSH parameters
    hostname: Optional[str] = None,
    username: Optional[str] = None,
    ssh_key: Optional[str] = None,
    # S3 parameters
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    session_token: Optional[str] = None,
    region_name: str = "us-east-1",
    endpoint_url: Optional[str] = None,
    aws_profile: Optional[str] = None,
    # Common parameters
    timeout: int = 300,
    max_items: int = 1000,
) -> list[str]:
    """List storage contents (subdirectories/prefixes or files/objects).

    Args:
        path: Storage path
        content_type: "subdirectories" for dirs/prefixes, "files" for files/objects
        hostname: SSH hostname (for remote operations)
        username: SSH username (for remote operations)
        ssh_key: SSH private key path (for remote operations)
        access_key_id: AWS access key ID (for S3 operations)
        secret_access_key: AWS secret access key (for S3 operations)
        session_token: AWS session token (for S3 operations)
        region_name: AWS region name (for S3 operations)
        endpoint_url: Custom S3 endpoint URL (for S3 operations)
        aws_profile: AWS CLI profile name (for S3 operations)
        timeout: Command timeout in seconds (for filesystem operations)
        max_items: Maximum number of items to return

    Returns:
        List of paths to subdirectories/prefixes or files/objects

    Raises:
        ValidationError: If parameters are invalid or missing
    """
    if content_type not in ("subdirectories", "files"):
        raise ValidationError(
            f"content_type must be 'subdirectories' or 'files', got: {content_type}"
        )

    storage_type = _detect_storage_type(path)
    logger.info(
        "Listing storage contents",
        path=path,
        storage_type=storage_type,
        content_type=content_type,
    )

    try:
        if storage_type == "s3":
            if content_type == "subdirectories":
                list_type = "prefixes"
            else:
                list_type = "objects"

            return list_objects_by_prefix(
                s3_path=path,
                list_type=list_type,
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                session_token=session_token,
                region_name=region_name,
                endpoint_url=endpoint_url,
                aws_profile=aws_profile,
                max_keys=max_items,
            )

        elif storage_type == "ssh":
            if content_type == "files":
                raise ValidationError("File listing not implemented for SSH storage")

            hostname, username, actual_path = _parse_ssh_path(path, hostname, username)

            if not all([hostname, username, ssh_key]):
                raise ValidationError(
                    "SSH operations require hostname, username, and ssh_key parameters"
                )

            # Type checker safety: all values guaranteed to be str at this point
            assert hostname is not None and username is not None and ssh_key is not None
            lister = RemoteSubdirectoryLister(hostname, username, ssh_key)
            return list_subdirectories(lister, actual_path, timeout)

        else:  # local
            if content_type == "files":
                raise ValidationError("File listing not implemented for local storage")

            lister = LocalSubdirectoryLister()
            return list_subdirectories(lister, path, timeout)

    except Exception as e:
        error_msg = f"Failed to list storage contents '{path}': {e}"
        logger.error(error_msg, error=str(e))
        raise ValidationError(error_msg)


def verify_storage_access(
    path: str,
    username: Optional[str] = None,
    operation: str = "read",
    # SSH parameters
    hostname: Optional[str] = None,
    ssh_username: Optional[str] = None,
    ssh_key: Optional[str] = None,
    # S3 parameters
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    session_token: Optional[str] = None,
    region_name: str = "us-east-1",
    endpoint_url: Optional[str] = None,
    aws_profile: Optional[str] = None,
) -> bool:
    """Verify access to storage location.

    Args:
        path: Storage path
        username: Username to check access for (filesystem only)
        operation: Operation to test ("read", "write", "list")
        hostname: SSH hostname (for remote operations)
        ssh_username: SSH username (for remote operations)
        ssh_key: SSH private key path (for remote operations)
        access_key_id: AWS access key ID (for S3 operations)
        secret_access_key: AWS secret access key (for S3 operations)
        session_token: AWS session token (for S3 operations)
        region_name: AWS region name (for S3 operations)
        endpoint_url: Custom S3 endpoint URL (for S3 operations)
        aws_profile: AWS CLI profile name (for S3 operations)

    Returns:
        True if access is permitted

    Raises:
        ValidationError: If parameters are invalid or missing
    """
    storage_type = _detect_storage_type(path)
    logger.info(
        "Verifying storage access",
        path=path,
        storage_type=storage_type,
        operation=operation,
    )

    try:
        if storage_type == "s3":
            return verify_s3_access(
                s3_path=path,
                operation=operation,
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                session_token=session_token,
                region_name=region_name,
                endpoint_url=endpoint_url,
                aws_profile=aws_profile,
            )

        elif storage_type == "ssh":
            raise ValidationError(
                "SSH access verification is not implemented. "
                "Consider implementing remote permission checks via SSH."
            )

        else:  # local
            if not username:
                raise ValidationError(
                    "Username required for local filesystem access verification"
                )

            # Map operation to filesystem type (simplified)
            if operation in ("read", "list"):
                return verify_directory_access(FilesystemType.nfs, path, username)
            elif operation == "write":
                raise ValidationError(
                    "Write permission verification is not implemented for "
                    "local filesystem. Consider implementing write permission checks."
                )
            else:
                raise ValidationError(f"Unknown operation: {operation}")

    except Exception as e:
        error_msg = f"Failed to verify storage access '{path}': {e}"
        logger.error(error_msg, error=str(e))
        raise ValidationError(error_msg)
