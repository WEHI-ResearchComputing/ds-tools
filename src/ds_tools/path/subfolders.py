import subprocess
from ds_tools.command_executor import RemoteCommandExecutor


class RemotePathSubfolders(RemoteCommandExecutor):
    """Executes path stats commands on remote machines via SSH."""

    def execute_command(
        self, path: str, timeout: int
    ) -> subprocess.CompletedProcess[str]:
        ssh_cmd = [
            "ssh",
            "-i",
            self.ssh_key,
            "-o",
            "ConnectTimeout=30",
            "-o",
            "BatchMode=yes",
            f"{self.username}@{self.hostname}",
            f"find '{path}' -mindepth 1 -maxdepth 1 -type d",
        ]

        return subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=timeout)


def list_subfolders(
    path: str, host: str, user: str, ssh_key: str, timeout: int = 300
) -> list[str]:
    """List subfolders in a remote directory via SSH."""
    executor = RemotePathSubfolders(host, user, ssh_key)

    try:
        result = executor.execute_command(path, timeout)

        if result.returncode != 0:
            raise Exception(f"Command failed: {result.stderr.strip()}")

        return result.stdout.strip().splitlines()

    except subprocess.TimeoutExpired:
        raise Exception(f"Command timed out after {timeout} seconds")
    except Exception as e:
        raise Exception(f"Error: {e}")
