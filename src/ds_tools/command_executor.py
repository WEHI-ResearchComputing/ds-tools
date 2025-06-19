import subprocess
from typing import Protocol
import os


class CommandExecutor(Protocol):
    """Protocol for executing commands that count files."""

    def execute_command(
        self, path: str, timeout: int
    ) -> subprocess.CompletedProcess[str]:
        """Execute the file counting command and return the result."""
        ...


class RemoteCommandExecutor(CommandExecutor):
    """Base class for remote command executors that require SSH key checking."""

    def __init__(self, hostname: str, username: str, ssh_key: str):
        self.hostname = hostname
        self.username = username
        self.ssh_key = ssh_key

        # Check if the SSH key file exists and is readable
        if not os.path.isfile(ssh_key) or not os.access(ssh_key, os.R_OK):
            raise FileNotFoundError(f"Key file {ssh_key} is missing or unreadable")
