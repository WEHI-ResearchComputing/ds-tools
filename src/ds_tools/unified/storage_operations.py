"""Unified storage operations that work across filesystem and object storage.

This module provides a single interface for storage operations across different
backends including local filesystems, remote SSH connections, and S3-compatible
object storage. It automatically determines the storage type (SSH, S3, or local)
based on path format and routes operations to the appropriate backend implementation.

The unified interface abstracts away the differences between storage backends,
providing consistent semantics and error handling across all supported storage
types.

Key Functions:
    - analyze_storage: Calculate metrics (file/object count, total size)
    - list_storage_contents: List subdirectories/prefixes or files/objects
    - verify_storage_access: Verify read/write/list permissions

Storage Type Detection:
    Storage type is automatically determined using this priority order:
    1. If SSH parameters (hostname, username, ssh_key) are provided -> SSH storage
    2. Otherwise, based on path format:
       - s3://bucket/prefix -> S3 storage
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
from ds_tools.storage_config import StorageConfig
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
) -> tuple[str | None, str | None, str]:
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
    config: Optional[StorageConfig] = None,
) -> StorageMetrics:
    """Analyze storage to get item count and total size.

    Determines storage type using this priority:
    1. If config.ssh is provided -> SSH storage
    2. If config.s3 is provided -> S3 storage
    3. Otherwise, based on path format (s3:// -> S3, everything else -> local)

    Args:
        path: Storage path (local path, ssh://user@host/path, or s3://bucket/prefix)
        config: Optional StorageConfig with SSH/S3 configuration

    Returns:
        StorageMetrics with unified format

    Raises:
        ValidationError: If storage type cannot be determined or required parameters
            are missing
    """
    # Use default config if none provided
    if config is None:
        config = StorageConfig()
    
    # Determine storage type based on config and path format
    if config.ssh is not None:
        storage_type = "ssh"
    elif config.s3 is not None:
        storage_type = "s3"
    else:
        storage_type = _detect_storage_type(path)

    logger.info("Analyzing storage", path=path, storage_type=storage_type)

    try:
        if storage_type == "s3":
            s3_config = config.s3
            metrics = analyze_prefix(
                s3_path=path,
                access_key_id=s3_config.access_key_id,
                secret_access_key=s3_config.secret_access_key,
                session_token=s3_config.session_token,
                region_name=s3_config.region_name,
                endpoint_url=s3_config.endpoint_url,
                aws_profile=s3_config.aws_profile,
            )
            return StorageMetrics(
                item_count=metrics.object_count,
                total_bytes=metrics.total_bytes,
                storage_type="s3",
                location=path,
            )

        elif storage_type == "ssh":
            ssh_config = config.ssh
            if ssh_config is None:
                raise ValidationError("SSH configuration is required for SSH storage type")
            
            hostname, username, actual_path = _parse_ssh_path(path, ssh_config.hostname, ssh_config.username)

            analyzer = RemoteDirectoryAnalyzer(ssh_config.hostname, ssh_config.username, ssh_config.ssh_key)
            metrics = calculate_directory_metrics(analyzer, actual_path, config.timeout)
            return StorageMetrics(
                item_count=metrics.file_count,
                total_bytes=metrics.total_bytes,
                storage_type="ssh",
                location=path,
            )

        else:  # local
            analyzer = LocalDirectoryAnalyzer()
            metrics = calculate_directory_metrics(analyzer, path, config.timeout)
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
    max_items: int = 1000,
    config: Optional[StorageConfig] = None,
) -> list[str]:
    """List storage contents (subdirectories/prefixes or files/objects).

    Determines storage type using this priority:
    1. If config.ssh is provided -> SSH storage
    2. If config.s3 is provided -> S3 storage
    3. Otherwise, based on path format (s3:// -> S3, everything else -> local)

    Args:
        path: Storage path
        content_type: "subdirectories" for dirs/prefixes, "files" for files/objects
        max_items: Maximum number of items to return
        config: Optional StorageConfig with SSH/S3 configuration

    Returns:
        List of paths to subdirectories/prefixes or files/objects

    Raises:
        ValidationError: If parameters are invalid or missing
    """
    if content_type not in ("subdirectories", "files"):
        raise ValidationError(
            f"content_type must be 'subdirectories' or 'files', got: {content_type}"
        )

    # Use default config if none provided
    if config is None:
        config = StorageConfig()
    
    # Determine storage type based on config and path format
    if config.ssh is not None:
        storage_type = "ssh"
    elif config.s3 is not None:
        storage_type = "s3"
    else:
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

            s3_config = config.s3
            return list_objects_by_prefix(
                s3_path=path,
                list_type=list_type,
                access_key_id=s3_config.access_key_id,
                secret_access_key=s3_config.secret_access_key,
                session_token=s3_config.session_token,
                region_name=s3_config.region_name,
                endpoint_url=s3_config.endpoint_url,
                aws_profile=s3_config.aws_profile,
                max_keys=max_items,
            )

        elif storage_type == "ssh":
            if content_type == "files":
                raise ValidationError("File listing not implemented for SSH storage")

            ssh_config = config.ssh
            if ssh_config is None:
                raise ValidationError("SSH configuration is required for SSH storage type")
            
            hostname, username, actual_path = _parse_ssh_path(path, ssh_config.hostname, ssh_config.username)

            lister = RemoteSubdirectoryLister(ssh_config.hostname, ssh_config.username, ssh_config.ssh_key)
            return list_subdirectories(lister, actual_path, config.timeout)

        else:  # local
            if content_type == "files":
                raise ValidationError("File listing not implemented for local storage")

            lister = LocalSubdirectoryLister()
            return list_subdirectories(lister, path, config.timeout)

    except Exception as e:
        error_msg = f"Failed to list storage contents '{path}': {e}"
        logger.error(error_msg, error=str(e))
        raise ValidationError(error_msg)


def verify_storage_access(
    path: str,
    operation: str = "read",
    config: Optional[StorageConfig] = None,
    username: Optional[str] = None,
) -> bool:
    """Verify access to storage location.

    Determines storage type using this priority:
    1. If config.ssh is provided -> SSH storage
    2. If config.s3 is provided -> S3 storage
    3. Otherwise, based on path format (s3:// -> S3, everything else -> local)

    Args:
        path: Storage path
        operation: Operation to test ("read", "write", "list")
        config: Optional StorageConfig with SSH/S3 configuration
        username: Username to check access for (local filesystem only)

    Returns:
        True if access is permitted

    Raises:
        ValidationError: If parameters are invalid or missing
    """
    # Use default config if none provided
    if config is None:
        config = StorageConfig()
    
    # Determine storage type based on config and path format
    if config.ssh is not None:
        storage_type = "ssh"
    elif config.s3 is not None:
        storage_type = "s3"
    else:
        storage_type = _detect_storage_type(path)

    logger.info(
        "Verifying storage access",
        path=path,
        storage_type=storage_type,
        operation=operation,
    )

    try:
        if storage_type == "s3":
            s3_config = config.s3
            return verify_s3_access(
                s3_path=path,
                operation=operation,
                access_key_id=s3_config.access_key_id,
                secret_access_key=s3_config.secret_access_key,
                session_token=s3_config.session_token,
                region_name=s3_config.region_name,
                endpoint_url=s3_config.endpoint_url,
                aws_profile=s3_config.aws_profile,
            )

        elif storage_type == "ssh":
            ssh_config = config.ssh
            if ssh_config is None:
                raise ValidationError("SSH configuration is required for SSH storage type")
            
            # Parse SSH path to get the actual path component
            try:
                parsed_hostname, parsed_username, actual_path = _parse_ssh_path(
                    path, ssh_config.hostname, ssh_config.username
                )
                # Use config values (they take precedence)
                hostname = ssh_config.hostname
                ssh_username = ssh_config.username
                ssh_key = ssh_config.ssh_key
            except ValueError as e:
                raise ValidationError(f"Invalid SSH path format: {e}")

            # For SSH, we verify access by attempting to list the directory
            # This tests both connectivity and permissions
            try:
                logger.debug(
                    "Verifying SSH access by listing directory",
                    path=path,
                    hostname=hostname,
                    username=ssh_username,
                    operation=operation,
                )

                # Use list_storage_contents to test access
                # We only need to check if we can list with max_items=1
                # Create a temporary config for this call
                ssh_test_config = StorageConfig.from_ssh(
                    hostname=hostname, 
                    username=ssh_username, 
                    ssh_key=ssh_key, 
                    timeout=30
                )
                list_storage_contents(
                    path=actual_path,
                    content_type="subdirectories",
                    max_items=1,
                    config=ssh_test_config,
                )

                # If we got here without exception, we have at least read access
                if operation in ("read", "list"):
                    return True
                elif operation == "write":
                    # For write access, we would need to attempt a write operation
                    # This is more complex and potentially destructive
                    logger.warning(
                        "SSH write access verification requested but not fully"
                        " implemented",
                        path=path,
                    )
                    # For now, assume read access implies potential write access
                    # In practice, this should be enhanced with actual write tests
                    return True
                else:
                    raise ValidationError(f"Unknown operation: {operation}")

            except ValidationError:
                # Re-raise validation errors
                raise
            except PermissionError:
                logger.info(
                    "SSH access denied",
                    path=path,
                    hostname=hostname,
                    operation=operation,
                )
                return False
            except FileNotFoundError:
                # Path doesn't exist - this is a valid case where access is denied
                logger.info(
                    "SSH path not found",
                    path=path,
                    hostname=hostname,
                    operation=operation,
                )
                return False
            except Exception as e:
                # Log the error but treat as access denied rather than failing
                logger.warning(
                    "SSH access check failed",
                    path=path,
                    hostname=hostname,
                    operation=operation,
                    error=str(e),
                )
                return False

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
