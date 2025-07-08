"""Filesystem operations for local and remote directories.

This module provides directory analysis, listing, and execution operations
for both local filesystems and remote systems accessed via SSH.
"""

import os
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass

from ds_tools.core import get_logger
from ds_tools.core.exceptions import CommandExecutionError, ValidationError

logger = get_logger(__name__)


@dataclass(frozen=True)
class DirectoryMetrics:
    """Metrics about a directory's contents.

    Attributes:
        file_count: Total number of files in the directory (recursive)
        total_bytes: Total size in bytes of all files
    """

    file_count: int
    total_bytes: int


class FileSystemCommandExecutor(ABC):
    """Base class for executing filesystem commands locally or remotely."""

    @abstractmethod
    def execute_command(
        self, path: str, timeout: int
    ) -> subprocess.CompletedProcess[str]:
        """Execute a filesystem command and return the result.

        Args:
            path: The filesystem path to operate on
            timeout: Command timeout in seconds

        Returns:
            CompletedProcess result from subprocess.run
        """
        pass


class RemoteFileSystemExecutor(FileSystemCommandExecutor):
    """Base class for remote filesystem command executors via SSH."""

    def __init__(self, hostname: str, username: str, ssh_key: str):
        """Initialize remote executor with SSH connection details.

        Args:
            hostname: Remote host to connect to
            username: SSH username
            ssh_key: Path to SSH private key file

        Raises:
            ValidationError: If SSH key file is invalid
        """
        self.hostname = hostname
        self.username = username
        self.ssh_key = ssh_key

        # Validate SSH key file exists and is readable
        if not os.path.isfile(ssh_key) or not os.access(ssh_key, os.R_OK):
            logger.error("SSH key validation failed", ssh_key=ssh_key)
            raise ValidationError(f"SSH key file {ssh_key} is missing or unreadable")

        logger.info(
            "Remote filesystem executor initialized",
            hostname=hostname,
            username=username,
        )


class LocalDirectoryAnalyzer(FileSystemCommandExecutor):
    """Analyzes directories on the local filesystem."""

    def execute_command(
        self, path: str, timeout: int
    ) -> subprocess.CompletedProcess[str]:
        """Execute file counting command locally.

        Uses find and awk to count files and sum their sizes recursively.

        Args:
            path: Local directory path to analyze
            timeout: Command timeout in seconds

        Returns:
            CompletedProcess with stdout containing "file_count,total_bytes"
        """
        # Construct efficient shell pipeline for file analysis:
        # 1. find: Recursively find all files (-type f) and print their sizes
        # 2. awk: Sum sizes and count files in a single pass
        #   - sum += $1 + 0.0: Add file size (+ 0.0 handles empty lines)
        #   - count++: Increment file counter
        #   - END: Print results as "count,total_bytes" when done
        command = (
            f"find '{path}' -type f -printf '%s\\n' | "
            f"awk '{{sum += $1 + 0.0; count++}} "
            f'END {{printf "%d,%.0f\\n", count, sum}}\''
        )

        return subprocess.run(
            command, shell=True, capture_output=True, text=True, timeout=timeout
        )


class RemoteDirectoryAnalyzer(RemoteFileSystemExecutor):
    """Analyzes directories on remote filesystems via SSH."""

    def execute_command(
        self, path: str, timeout: int
    ) -> subprocess.CompletedProcess[str]:
        """Execute file counting command on remote host via SSH.

        Uses find and awk to count files and sum their sizes recursively.

        Args:
            path: Remote directory path to analyze
            timeout: Command timeout in seconds

        Returns:
            CompletedProcess with stdout containing "file_count,total_bytes"
        """
        # SSH command construction:
        # - Use specified private key (-i)
        # - Set connection timeout to prevent hanging
        # - Use BatchMode to prevent interactive prompts
        # - Execute the same find+awk pipeline as local version
        ssh_cmd = [
            "ssh",
            "-i", self.ssh_key,                    # Use specific private key
            "-o", "ConnectTimeout=30",             # Prevent hanging connections
            "-o", "BatchMode=yes",                 # No interactive prompts
            f"{self.username}@{self.hostname}",    # Connection target
            # Same efficient find+awk pipeline as local version
            f"find '{path}' -type f -printf '%s\\n' | "
            f"awk '{{sum += $1 + 0.0; count++}} "
            f'END {{printf "%d,%.0f\\n", count, sum}}\'',
        ]

        return subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=timeout)


class LocalSubdirectoryLister(FileSystemCommandExecutor):
    """Lists subdirectories on the local filesystem."""

    def execute_command(
        self, path: str, timeout: int
    ) -> subprocess.CompletedProcess[str]:
        """Execute subdirectory listing command locally.

        Uses find to list immediate subdirectories (depth 1) only.

        Args:
            path: Local directory path to list subdirectories for
            timeout: Command timeout in seconds

        Returns:
            CompletedProcess with stdout containing subdirectory paths
        """
        command = f"find '{path}' -mindepth 1 -maxdepth 1 -type d"

        return subprocess.run(
            command, shell=True, capture_output=True, text=True, timeout=timeout
        )


class RemoteSubdirectoryLister(RemoteFileSystemExecutor):
    """Lists subdirectories on remote filesystems via SSH."""

    def execute_command(
        self, path: str, timeout: int
    ) -> subprocess.CompletedProcess[str]:
        """Execute subdirectory listing command on remote host via SSH.

        Uses find to list immediate subdirectories (depth 1) only.

        Args:
            path: Remote directory path to list subdirectories for
            timeout: Command timeout in seconds

        Returns:
            CompletedProcess with stdout containing subdirectory paths
        """
        ssh_cmd = [
            "ssh",
            "-i", self.ssh_key,
            "-o", "ConnectTimeout=30",
            "-o", "BatchMode=yes",
            f"{self.username}@{self.hostname}",
            f"find '{path}' -mindepth 1 -maxdepth 1 -type d",
        ]

        return subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=timeout)


def calculate_directory_metrics(
    executor: FileSystemCommandExecutor, path: str, timeout: int = 300
) -> DirectoryMetrics:
    """Calculate metrics for a directory using the provided executor.

    Args:
        executor: Command executor (local or remote)
        path: Directory path to analyze
        timeout: Command timeout in seconds

    Returns:
        DirectoryMetrics containing file count and total size

    Raises:
        CommandExecutionError: If command execution fails
    """
    logger.info(
        "Calculating directory metrics", path=path, executor=type(executor).__name__
    )

    try:
        result = executor.execute_command(path, timeout)

        if result.returncode != 0:
            error_msg = f"Command failed: {result.stderr.strip()}"
            logger.error(error_msg, returncode=result.returncode)
            raise CommandExecutionError(error_msg)

        output = result.stdout.strip()
        if "," not in output:
            error_msg = f"Unexpected output format: {output}"
            logger.error(error_msg)
            raise CommandExecutionError(error_msg)

        file_count, total_bytes = map(int, output.split(","))
        metrics = DirectoryMetrics(file_count=file_count, total_bytes=total_bytes)

        logger.info(
            "Directory metrics calculated",
            file_count=metrics.file_count,
            total_bytes=metrics.total_bytes,
        )
        return metrics

    except subprocess.TimeoutExpired:
        error_msg = f"Command timed out after {timeout} seconds"
        logger.error(error_msg, timeout=timeout)
        raise CommandExecutionError(error_msg)
    except (ValueError, TypeError) as e:
        error_msg = f"Failed to parse command output: {e}"
        logger.error(error_msg, error=str(e))
        raise CommandExecutionError(error_msg)
    except CommandExecutionError:
        raise
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        logger.error(error_msg, error=str(e))
        raise CommandExecutionError(error_msg)


def list_subdirectories(
    executor: FileSystemCommandExecutor, path: str, timeout: int = 300
) -> list[str]:
    """List immediate subdirectories using the provided executor.

    Args:
        executor: Command executor (local or remote)
        path: Directory path to list subdirectories for
        timeout: Command timeout in seconds

    Returns:
        List of subdirectory paths

    Raises:
        CommandExecutionError: If command execution fails
    """
    logger.info("Listing subdirectories", path=path, executor=type(executor).__name__)

    try:
        result = executor.execute_command(path, timeout)

        if result.returncode != 0:
            error_msg = f"Command failed: {result.stderr.strip()}"
            logger.error(error_msg, returncode=result.returncode)
            raise CommandExecutionError(error_msg)

        # Split output into lines and filter out empty lines
        subdirectories = [
            line.strip() for line in result.stdout.strip().splitlines() if line.strip()
        ]

        logger.info(
            "Subdirectories listed",
            subdirectory_count=len(subdirectories),
        )
        return subdirectories

    except subprocess.TimeoutExpired:
        error_msg = f"Command timed out after {timeout} seconds"
        logger.error(error_msg, timeout=timeout)
        raise CommandExecutionError(error_msg)
    except CommandExecutionError:
        raise
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        logger.error(error_msg, error=str(e))
        raise CommandExecutionError(error_msg)

