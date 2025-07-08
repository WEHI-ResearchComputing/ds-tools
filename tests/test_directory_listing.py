"""Tests for directory listing functionality."""

import subprocess
from unittest.mock import Mock, patch

import pytest

from ds_tools.core.exceptions import CommandExecutionError
from ds_tools.filesystem.operations import (
    LocalSubdirectoryLister,
    RemoteSubdirectoryLister,
    list_subdirectories,
)


class TestLocalSubdirectoryLister:
    """Test local subdirectory listing functionality."""

    def test_execute_command_success(self):
        """Test successful command execution."""
        lister = LocalSubdirectoryLister()

        with patch("subprocess.run") as mock_run:
            mock_result = Mock()
            mock_result.returncode = 0
            mock_result.stdout = "/test/path/dir1\n/test/path/dir2\n"
            mock_result.stderr = ""
            mock_run.return_value = mock_result

            result = lister.execute_command("/test/path", timeout=30)

            assert result.returncode == 0
            assert "/test/path/dir1" in result.stdout
            assert "/test/path/dir2" in result.stdout

            # Verify the command was constructed correctly
            call_args = mock_run.call_args[0][0]
            assert "find '/test/path'" in call_args
            assert "-mindepth 1 -maxdepth 1 -type d" in call_args


class TestRemoteSubdirectoryLister:
    """Test remote subdirectory listing functionality."""

    def test_execute_command_success(self, mock_ssh_key):
        """Test successful remote command execution."""
        lister = RemoteSubdirectoryLister(
            hostname="test.host", username="testuser", ssh_key=mock_ssh_key
        )

        with patch("subprocess.run") as mock_run:
            mock_result = Mock()
            mock_result.returncode = 0
            mock_result.stdout = "/remote/path/dir1\n/remote/path/dir2\n"
            mock_result.stderr = ""
            mock_run.return_value = mock_result

            result = lister.execute_command("/remote/path", timeout=30)

            assert result.returncode == 0
            assert "/remote/path/dir1" in result.stdout
            assert "/remote/path/dir2" in result.stdout

            # Verify SSH command construction
            call_args = mock_run.call_args[0][0]
            assert call_args[0] == "ssh"
            assert "-i" in call_args
            assert mock_ssh_key in call_args
            assert "testuser@test.host" in call_args


class TestListSubdirectories:
    """Test the list_subdirectories function."""

    def test_list_subdirectories_success(self):
        """Test successful subdirectory listing."""
        mock_lister = Mock()
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "/test/path/dir1\n/test/path/dir2\n/test/path/dir3\n"
        mock_result.stderr = ""
        mock_lister.execute_command.return_value = mock_result

        subdirs = list_subdirectories(mock_lister, "/test/path")

        assert len(subdirs) == 3
        assert "/test/path/dir1" in subdirs
        assert "/test/path/dir2" in subdirs
        assert "/test/path/dir3" in subdirs
        mock_lister.execute_command.assert_called_once_with("/test/path", 300)

    def test_list_subdirectories_empty_result(self):
        """Test handling of empty subdirectory listing."""
        mock_lister = Mock()
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_result.stderr = ""
        mock_lister.execute_command.return_value = mock_result

        subdirs = list_subdirectories(mock_lister, "/test/path")

        assert subdirs == []

    def test_list_subdirectories_with_empty_lines(self):
        """Test handling of output with empty lines."""
        mock_lister = Mock()
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "/test/path/dir1\n\n/test/path/dir2\n\n"
        mock_result.stderr = ""
        mock_lister.execute_command.return_value = mock_result

        subdirs = list_subdirectories(mock_lister, "/test/path")

        assert len(subdirs) == 2
        assert "/test/path/dir1" in subdirs
        assert "/test/path/dir2" in subdirs
        assert "" not in subdirs

    def test_list_subdirectories_command_failure(self):
        """Test handling of command failure."""
        mock_lister = Mock()
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "Permission denied"
        mock_lister.execute_command.return_value = mock_result

        with pytest.raises(CommandExecutionError) as exc_info:
            list_subdirectories(mock_lister, "/test/path")

        assert "Command failed: Permission denied" in str(exc_info.value)

    def test_list_subdirectories_timeout(self):
        """Test handling of command timeout."""
        mock_lister = Mock()
        mock_lister.execute_command.side_effect = subprocess.TimeoutExpired(
            cmd="test", timeout=30
        )

        with pytest.raises(CommandExecutionError) as exc_info:
            list_subdirectories(mock_lister, "/test/path", timeout=30)

        assert "Command timed out after 30 seconds" in str(exc_info.value)
