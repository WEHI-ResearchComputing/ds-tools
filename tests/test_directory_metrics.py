"""Tests for directory metrics functionality."""

import subprocess
from unittest.mock import Mock, patch

import pytest

from ds_tools.core.exceptions import CommandExecutionError
from ds_tools.filesystem.operations import (
    DirectoryMetrics,
    LocalDirectoryAnalyzer,
    RemoteDirectoryAnalyzer,
    calculate_directory_metrics,
)


class TestDirectoryMetrics:
    """Test DirectoryMetrics dataclass."""

    def test_directory_metrics_creation(self):
        """Test DirectoryMetrics creation and immutability."""
        metrics = DirectoryMetrics(file_count=42, total_bytes=1048576)

        assert metrics.file_count == 42
        assert metrics.total_bytes == 1048576

        # Test immutability (frozen dataclass)
        with pytest.raises(AttributeError):
            metrics.file_count = 100


class TestLocalDirectoryAnalyzer:
    """Test local directory analysis functionality."""

    def test_execute_command_success(self):
        """Test successful command execution."""
        analyzer = LocalDirectoryAnalyzer()

        with patch("subprocess.run") as mock_run:
            mock_result = Mock()
            mock_result.returncode = 0
            mock_result.stdout = "10,1024"
            mock_result.stderr = ""
            mock_run.return_value = mock_result

            result = analyzer.execute_command("/test/path", timeout=30)

            assert result.returncode == 0
            assert result.stdout == "10,1024"

            # Verify the command was constructed correctly
            call_args = mock_run.call_args[0][0]
            assert "find '/test/path'" in call_args
            assert "awk" in call_args


class TestRemoteDirectoryAnalyzer:
    """Test remote directory analysis functionality."""

    def test_execute_command_success(self, mock_ssh_key):
        """Test successful remote command execution."""
        analyzer = RemoteDirectoryAnalyzer(
            hostname="test.host", username="testuser", ssh_key=mock_ssh_key
        )

        with patch("subprocess.run") as mock_run:
            mock_result = Mock()
            mock_result.returncode = 0
            mock_result.stdout = "20,2048"
            mock_result.stderr = ""
            mock_run.return_value = mock_result

            result = analyzer.execute_command("/remote/path", timeout=30)

            assert result.returncode == 0
            assert result.stdout == "20,2048"

            # Verify SSH command construction
            call_args = mock_run.call_args[0][0]
            assert call_args[0] == "ssh"
            assert "-i" in call_args
            assert mock_ssh_key in call_args
            assert "testuser@test.host" in call_args


class TestCalculateDirectoryMetrics:
    """Test the calculate_directory_metrics function."""

    def test_calculate_metrics_success(self):
        """Test successful metrics calculation."""
        mock_analyzer = Mock()
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "42,1048576"
        mock_result.stderr = ""
        mock_analyzer.execute_command.return_value = mock_result

        metrics = calculate_directory_metrics(mock_analyzer, "/test/path")

        assert isinstance(metrics, DirectoryMetrics)
        assert metrics.file_count == 42
        assert metrics.total_bytes == 1048576
        mock_analyzer.execute_command.assert_called_once_with("/test/path", 300)

    def test_calculate_metrics_command_failure(self):
        """Test handling of command failure."""
        mock_analyzer = Mock()
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "Permission denied"
        mock_analyzer.execute_command.return_value = mock_result

        with pytest.raises(CommandExecutionError) as exc_info:
            calculate_directory_metrics(mock_analyzer, "/test/path")

        assert "Command failed: Permission denied" in str(exc_info.value)

    def test_calculate_metrics_invalid_output(self):
        """Test handling of invalid command output."""
        mock_analyzer = Mock()
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "invalid output"
        mock_result.stderr = ""
        mock_analyzer.execute_command.return_value = mock_result

        with pytest.raises(CommandExecutionError) as exc_info:
            calculate_directory_metrics(mock_analyzer, "/test/path")

        assert "Unexpected output format" in str(exc_info.value)

    def test_calculate_metrics_timeout(self):
        """Test handling of command timeout."""
        mock_analyzer = Mock()
        mock_analyzer.execute_command.side_effect = subprocess.TimeoutExpired(
            cmd="test", timeout=30
        )

        with pytest.raises(CommandExecutionError) as exc_info:
            calculate_directory_metrics(mock_analyzer, "/test/path", timeout=30)

        assert "Command timed out after 30 seconds" in str(exc_info.value)

    def test_calculate_metrics_parse_error(self):
        """Test handling of output parsing errors."""
        mock_analyzer = Mock()
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "abc,def"  # Non-numeric values
        mock_result.stderr = ""
        mock_analyzer.execute_command.return_value = mock_result

        with pytest.raises(CommandExecutionError) as exc_info:
            calculate_directory_metrics(mock_analyzer, "/test/path")

        assert "Failed to parse command output" in str(exc_info.value)
