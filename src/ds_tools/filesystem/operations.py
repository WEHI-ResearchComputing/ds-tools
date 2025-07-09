"""Filesystem operations for local and remote directories.

This module provides directory analysis and listing operations
for both local filesystems and remote systems accessed via SSH.
"""

import os
import subprocess
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


def _validate_ssh_key(ssh_key: str) -> None:
    """Validate SSH key file exists and is readable.

    Args:
        ssh_key: Path to SSH private key file

    Raises:
        ValidationError: If SSH key file is invalid
    """
    if not os.path.isfile(ssh_key) or not os.access(ssh_key, os.R_OK):
        logger.error("SSH key validation failed", ssh_key=ssh_key)
        raise ValidationError(f"SSH key file {ssh_key} is missing or unreadable")


def _execute_local_command(
    command: str, timeout: int
) -> subprocess.CompletedProcess[str]:
    """Execute a command locally.

    Args:
        command: Shell command to execute
        timeout: Command timeout in seconds

    Returns:
        CompletedProcess result from subprocess.run
    """
    return subprocess.run(
        command, shell=True, capture_output=True, text=True, timeout=timeout
    )


def _execute_ssh_command(
    hostname: str, username: str, ssh_key: str, remote_command: str, timeout: int
) -> subprocess.CompletedProcess[str]:
    """Execute a command on remote host via SSH.

    Args:
        hostname: Remote host to connect to
        username: SSH username
        ssh_key: Path to SSH private key file
        remote_command: Command to execute on remote host
        timeout: Command timeout in seconds

    Returns:
        CompletedProcess result from subprocess.run
    """
    _validate_ssh_key(ssh_key)

    ssh_cmd = [
        "ssh",
        "-i",
        ssh_key,
        "-o",
        "ConnectTimeout=30",
        "-o",
        "BatchMode=yes",
        f"{username}@{hostname}",
        remote_command,
    ]

    return subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=timeout)


def analyze_local_directory(path: str, timeout: int = 300) -> DirectoryMetrics:
    """Analyze a local directory to get file count and total size.

    Args:
        path: Local directory path to analyze
        timeout: Command timeout in seconds

    Returns:
        DirectoryMetrics containing file count and total size

    Raises:
        CommandExecutionError: If command execution fails
    """
    logger.info("Analyzing local directory", path=path)

    command = (
        f"find '{path}' -type f -printf '%s\\n' | "
        f"awk '{{sum += $1 + 0.0; count++}} "
        f'END {{printf "%d,%.0f\\n", count, sum}}'
    )

    try:
        result = _execute_local_command(command, timeout)
        return _parse_metrics_output(result)
    except Exception as e:
        error_msg = f"Failed to analyze local directory '{path}': {e}"
        logger.error(error_msg, error=str(e))
        raise CommandExecutionError(error_msg)


def analyze_remote_directory(
    hostname: str, username: str, ssh_key: str, path: str, timeout: int = 300
) -> DirectoryMetrics:
    """Analyze a remote directory to get file count and total size.

    Args:
        hostname: Remote host to connect to
        username: SSH username
        ssh_key: Path to SSH private key file
        path: Remote directory path to analyze
        timeout: Command timeout in seconds

    Returns:
        DirectoryMetrics containing file count and total size

    Raises:
        CommandExecutionError: If command execution fails
    """
    logger.info(
        "Analyzing remote directory", hostname=hostname, username=username, path=path
    )

    remote_command = (
        f"find '{path}' -type f -printf '%s\\n' | "
        f"awk '{{sum += $1 + 0.0; count++}} "
        f'END {{printf "%d,%.0f\\n", count, sum}}'
    )

    try:
        result = _execute_ssh_command(
            hostname, username, ssh_key, remote_command, timeout
        )
        return _parse_metrics_output(result)
    except Exception as e:
        error_msg = f"Failed to analyze remote directory '{hostname}:{path}': {e}"
        logger.error(error_msg, error=str(e))
        raise CommandExecutionError(error_msg)


def list_local_subdirectories(path: str, timeout: int = 300) -> list[str]:
    """List immediate subdirectories in a local directory.

    Args:
        path: Local directory path to list subdirectories for
        timeout: Command timeout in seconds

    Returns:
        List of subdirectory paths

    Raises:
        CommandExecutionError: If command execution fails
    """
    logger.info("Listing local subdirectories", path=path)

    command = f"find '{path}' -mindepth 1 -maxdepth 1 -type d"

    try:
        result = _execute_local_command(command, timeout)
        return _parse_listing_output(result)
    except Exception as e:
        error_msg = f"Failed to list local subdirectories '{path}': {e}"
        logger.error(error_msg, error=str(e))
        raise CommandExecutionError(error_msg)


def list_remote_subdirectories(
    hostname: str, username: str, ssh_key: str, path: str, timeout: int = 300
) -> list[str]:
    """List immediate subdirectories in a remote directory.

    Args:
        hostname: Remote host to connect to
        username: SSH username
        ssh_key: Path to SSH private key file
        path: Remote directory path to list subdirectories for
        timeout: Command timeout in seconds

    Returns:
        List of subdirectory paths

    Raises:
        CommandExecutionError: If command execution fails
    """
    logger.info(
        "Listing remote subdirectories", hostname=hostname, username=username, path=path
    )

    remote_command = f"find '{path}' -mindepth 1 -maxdepth 1 -type d"

    try:
        result = _execute_ssh_command(
            hostname, username, ssh_key, remote_command, timeout
        )
        return _parse_listing_output(result)
    except Exception as e:
        error_msg = f"Failed to list remote subdirectories '{hostname}:{path}': {e}"
        logger.error(error_msg, error=str(e))
        raise CommandExecutionError(error_msg)


def _parse_metrics_output(result: subprocess.CompletedProcess[str]) -> DirectoryMetrics:
    """Parse command output to extract directory metrics.

    Args:
        result: CompletedProcess from subprocess.run

    Returns:
        DirectoryMetrics containing file count and total size

    Raises:
        CommandExecutionError: If command failed or output is invalid
    """
    if result.returncode != 0:
        error_msg = f"Command failed: {result.stderr.strip()}"
        logger.error(error_msg, returncode=result.returncode)
        raise CommandExecutionError(error_msg)

    output = result.stdout.strip()
    if "," not in output:
        error_msg = f"Unexpected output format: {output}"
        logger.error(error_msg)
        raise CommandExecutionError(error_msg)

    try:
        file_count, total_bytes = map(int, output.split(","))
        metrics = DirectoryMetrics(file_count=file_count, total_bytes=total_bytes)

        logger.info(
            "Directory metrics parsed",
            file_count=metrics.file_count,
            total_bytes=metrics.total_bytes,
        )
        return metrics
    except (ValueError, TypeError) as e:
        error_msg = f"Failed to parse command output: {e}"
        logger.error(error_msg, error=str(e))
        raise CommandExecutionError(error_msg)


def _parse_listing_output(result: subprocess.CompletedProcess[str]) -> list[str]:
    """Parse command output to extract directory listing.

    Args:
        result: CompletedProcess from subprocess.run

    Returns:
        List of subdirectory paths

    Raises:
        CommandExecutionError: If command failed
    """
    if result.returncode != 0:
        error_msg = f"Command failed: {result.stderr.strip()}"
        logger.error(error_msg, returncode=result.returncode)
        raise CommandExecutionError(error_msg)

    subdirectories = [
        line.strip() for line in result.stdout.strip().splitlines() if line.strip()
    ]

    logger.info(
        "Subdirectories parsed",
        subdirectory_count=len(subdirectories),
    )
    return subdirectories


# Legacy compatibility - these classes and functions maintain the old API
class LocalDirectoryAnalyzer:
    """Legacy compatibility class for LocalDirectoryAnalyzer."""

    def execute_command(
        self, path: str, timeout: int
    ) -> subprocess.CompletedProcess[str]:
        """Execute file counting command locally."""
        command = (
            f"find '{path}' -type f -printf '%s\\n' | "
            f"awk '{{sum += $1 + 0.0; count++}} "
            f'END {{printf "%d,%.0f\\n", count, sum}}'
        )
        return _execute_local_command(command, timeout)


class RemoteDirectoryAnalyzer:
    """Legacy compatibility class for RemoteDirectoryAnalyzer."""

    def __init__(self, hostname: str, username: str, ssh_key: str):
        self.hostname = hostname
        self.username = username
        self.ssh_key = ssh_key
        _validate_ssh_key(ssh_key)

    def execute_command(
        self, path: str, timeout: int
    ) -> subprocess.CompletedProcess[str]:
        """Execute file counting command on remote host via SSH."""
        remote_command = (
            f"find '{path}' -type f -printf '%s\\n' | "
            f"awk '{{sum += $1 + 0.0; count++}} "
            f'END {{printf "%d,%.0f\\n", count, sum}}'
        )
        return _execute_ssh_command(
            self.hostname, self.username, self.ssh_key, remote_command, timeout
        )


class LocalSubdirectoryLister:
    """Legacy compatibility class for LocalSubdirectoryLister."""

    def execute_command(
        self, path: str, timeout: int
    ) -> subprocess.CompletedProcess[str]:
        """Execute subdirectory listing command locally."""
        command = f"find '{path}' -mindepth 1 -maxdepth 1 -type d"
        return _execute_local_command(command, timeout)


class RemoteSubdirectoryLister:
    """Legacy compatibility class for RemoteSubdirectoryLister."""

    def __init__(self, hostname: str, username: str, ssh_key: str):
        self.hostname = hostname
        self.username = username
        self.ssh_key = ssh_key
        _validate_ssh_key(ssh_key)

    def execute_command(
        self, path: str, timeout: int
    ) -> subprocess.CompletedProcess[str]:
        """Execute subdirectory listing command on remote host via SSH."""
        remote_command = f"find '{path}' -mindepth 1 -maxdepth 1 -type d"
        return _execute_ssh_command(
            self.hostname, self.username, self.ssh_key, remote_command, timeout
        )


def calculate_directory_metrics(
    executor, path: str, timeout: int = 300
) -> DirectoryMetrics:
    """Legacy compatibility function for calculate_directory_metrics."""
    try:
        result = executor.execute_command(path, timeout)
        return _parse_metrics_output(result)
    except subprocess.TimeoutExpired:
        error_msg = f"Command timed out after {timeout} seconds"
        logger.error(error_msg, timeout=timeout)
        raise CommandExecutionError(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        logger.error(error_msg, error=str(e))
        raise CommandExecutionError(error_msg)


def list_subdirectories(executor, path: str, timeout: int = 300) -> list[str]:
    """Legacy compatibility function for list_subdirectories."""
    try:
        result = executor.execute_command(path, timeout)
        return _parse_listing_output(result)
    except subprocess.TimeoutExpired:
        error_msg = f"Command timed out after {timeout} seconds"
        logger.error(error_msg, timeout=timeout)
        raise CommandExecutionError(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        logger.error(error_msg, error=str(e))
        raise CommandExecutionError(error_msg)
