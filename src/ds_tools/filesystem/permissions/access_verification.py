"""Directory access verification for different filesystem types."""

import os
import subprocess
from abc import ABC, abstractmethod
from enum import Enum

from ds_tools.core import get_logger
from ds_tools.core.exceptions import ValidationError

logger = get_logger(__name__)


class FilesystemType(str, Enum):
    """Supported filesystem types for access verification."""

    nfs = "nfs"
    nfs4 = "nfs4"


class DirectoryAccessVerifier(ABC):
    """Abstract base class for verifying directory access permissions."""

    @abstractmethod
    def verify_directory_access(self, path: str, username: str) -> bool:
        """Verify that a user has read and execute access to a directory.

        Args:
            path: The directory path to check
            username: The username to verify access for

        Returns:
            True if user has read and execute access

        Raises:
            NotADirectoryError: If path doesn't exist or isn't a directory
            ValidationError: If access verification fails
        """
        pass


class NFSDirectoryAccessVerifier(DirectoryAccessVerifier):
    """Verifies directory access using getfacl for NFS (POSIX ACLs)."""

    def verify_directory_access(self, path: str, username: str) -> bool:
        """Verify NFS directory access using POSIX ACLs.

        Args:
            path: Directory path to check
            username: Username to verify access for

        Returns:
            True if user has read and execute access

        Raises:
            NotADirectoryError: If path is not a directory
            ValidationError: If user lacks required permissions
        """
        logger.info("Verifying NFS directory access", path=path, username=username)

        if not os.path.isdir(path):
            raise NotADirectoryError(
                f"Path {path} does not exist or is not a directory"
            )

        try:
            command = ["getfacl", path]
            result = subprocess.run(command, check=True, capture_output=True, text=True)

            for line in result.stdout.splitlines():
                if line.startswith(f"user:{username}:"):
                    permissions = line.split(":")[-1]
                    has_read = "r" in permissions
                    has_execute = "x" in permissions

                    if has_read and has_execute:
                        logger.info("NFS access verified", path=path, username=username)
                        return True

            error_msg = f"User {username} does not have read/execute access to {path}"
            logger.warning(error_msg)
            raise ValidationError(error_msg)

        except subprocess.CalledProcessError as e:
            error_msg = f"Failed to check NFS permissions: {e.stderr}"
            logger.error(error_msg, error=str(e))
            raise ValidationError(error_msg)


class NFS4DirectoryAccessVerifier(DirectoryAccessVerifier):
    """Verifies directory access using nfs4_getfacl for NFSv4 ACLs."""

    def verify_directory_access(self, path: str, username: str) -> bool:
        """Verify NFSv4 directory access using NFSv4 ACLs.

        Args:
            path: Directory path to check
            username: Username to verify access for

        Returns:
            True if user has read and execute access

        Raises:
            NotADirectoryError: If path is not a directory
            ValidationError: If user lacks required permissions
        """
        logger.info("Verifying NFSv4 directory access", path=path, username=username)

        if not os.path.isdir(path):
            raise NotADirectoryError(
                f"Path {path} does not exist or is not a directory"
            )

        try:
            command = ["nfs4_getfacl", path]
            result = subprocess.run(command, check=True, capture_output=True, text=True)

            for line in result.stdout.splitlines():
                if line.startswith(f"A::{username}@") or line.startswith(
                    f"A::{username}:"
                ):
                    parts = line.split(":")
                    if len(parts) >= 4:
                        permissions = parts[3]
                        has_read = "r" in permissions
                        has_execute = "x" in permissions

                        if has_read and has_execute:
                            logger.info(
                                "NFSv4 access verified", path=path, username=username
                            )
                            return True

            error_msg = f"User {username} does not have read/execute access to {path}"
            logger.warning(error_msg)
            raise ValidationError(error_msg)

        except subprocess.CalledProcessError as e:
            error_msg = f"Failed to check NFSv4 permissions: {e.stderr}"
            logger.error(error_msg, error=str(e))
            raise ValidationError(error_msg)


def verify_directory_access(
    filesystem_type: FilesystemType, path: str, username: str
) -> bool:
    """Verify directory access for a user on different filesystem types.

    Args:
        filesystem_type: Type of filesystem ('nfs', 'nfs4')
        path: Directory path to check
        username: Username to verify access for

    Returns:
        True if user has directory access

    Raises:
        ValueError: If filesystem type is unsupported
    """
    # Simple conditional instead of unnecessary factory pattern
    if filesystem_type == FilesystemType.nfs:
        verifier = NFSDirectoryAccessVerifier()
    elif filesystem_type == FilesystemType.nfs4:
        verifier = NFS4DirectoryAccessVerifier()
    else:
        available_types = ", ".join([e.value for e in FilesystemType])
        raise ValueError(
            f"Unsupported filesystem type: {filesystem_type}. "
            f"Available types: {available_types}"
        )

    return verifier.verify_directory_access(path, username)
