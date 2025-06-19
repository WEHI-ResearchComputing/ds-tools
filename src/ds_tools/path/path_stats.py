import subprocess

from ds_tools.command_executor import RemoteCommandExecutor, CommandExecutor


class LocalPathStats(CommandExecutor):
    """Executes path stats commands locally."""

    def execute_command(
        self, path: str, timeout: int
    ) -> subprocess.CompletedProcess[str]:
        command = (
            f"find '{path}' -type f -printf '%s\\n' | "
            f"awk '{{sum += $1 + 0.0; count++}} END {{printf \"%d,%.0f\\n\", count, sum}}'"
        )

        return subprocess.run(
            command, shell=True, capture_output=True, text=True, timeout=timeout
        )


class RemotePathStats(RemoteCommandExecutor):
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
            f"find '{path}' -type f -printf '%s\\n' | "
            f"awk '{{sum += $1 + 0.0; count++}} END {{printf \"%d,%.0f\\n\", count, sum}}'",
        ]

        return subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=timeout)


def path_stats(executor: CommandExecutor, path: str, timeout: int = 300) -> tuple[int, int]:
    """Count files and sum bytes using the provided command executor."""

    try:
        result = executor.execute_command(path, timeout)

        if result.returncode != 0:
            raise Exception(f"Command failed: {result.stderr.strip()}")

        output = result.stdout.strip()
        if "," not in output:
            raise Exception(f"Unexpected output: {output}")

        file_count, byte_count = map(int, output.split(","))
        return file_count, byte_count

    except subprocess.TimeoutExpired:
        raise Exception(f"Command timed out after {timeout} seconds")
    except Exception as e:
        raise Exception(f"Error: {e}")
