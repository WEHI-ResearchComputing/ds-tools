"""Tests for filesystem permissions and access verification."""

import subprocess
from unittest.mock import Mock, patch

import pytest

from ds_tools.core.exceptions import ValidationError
from ds_tools.filesystem.permissions.access_verification import (
    FilesystemType,
    NFS4DirectoryAccessVerifier,
    NFSDirectoryAccessVerifier,
    verify_directory_access,
)


class TestFilesystemType:
    """Test FilesystemType enum."""

    def test_filesystem_type_values(self):
        """Test filesystem type enum values."""
        assert FilesystemType.nfs == "nfs"
        assert FilesystemType.nfs4 == "nfs4"


class TestNFSDirectoryAccessVerifier:
    """Test NFS directory access verification."""

    def setup_method(self):
        """Set up test environment."""
        self.verifier = NFSDirectoryAccessVerifier()

    @patch("os.path.isdir")
    @patch("subprocess.run")
    def test_verify_access_success(self, mock_run, mock_isdir):
        """Test successful access verification."""
        mock_isdir.return_value = True
        mock_result = Mock()
        mock_result.stdout = "user:testuser:rx\nother::---"
        mock_run.return_value = mock_result

        result = self.verifier.verify_directory_access("/test/path", "testuser")

        assert result is True
        mock_run.assert_called_once_with(
            ["getfacl", "/test/path"], check=True, capture_output=True, text=True
        )

    @patch("os.path.isdir")
    def test_verify_access_not_directory(self, mock_isdir):
        """Test error when path is not a directory."""
        mock_isdir.return_value = False

        with pytest.raises(NotADirectoryError):
            self.verifier.verify_directory_access("/bad/path", "testuser")

    @patch("os.path.isdir")
    @patch("subprocess.run")
    def test_verify_access_insufficient_permissions(self, mock_run, mock_isdir):
        """Test access verification with insufficient permissions."""
        mock_isdir.return_value = True
        mock_result = Mock()
        mock_result.stdout = "user:testuser:r--\nother::---"  # No execute permission
        mock_run.return_value = mock_result

        with pytest.raises(ValidationError) as exc_info:
            self.verifier.verify_directory_access("/test/path", "testuser")

        assert "does not have read/execute access" in str(exc_info.value)

    @patch("os.path.isdir")
    @patch("subprocess.run")
    def test_verify_access_command_failure(self, mock_run, mock_isdir):
        """Test handling of getfacl command failure."""
        mock_isdir.return_value = True
        mock_run.side_effect = subprocess.CalledProcessError(
            1, ["getfacl"], stderr="Permission denied"
        )

        with pytest.raises(ValidationError) as exc_info:
            self.verifier.verify_directory_access("/test/path", "testuser")

        assert "Failed to check NFS permissions" in str(exc_info.value)

    @patch("os.path.isdir")
    @patch("subprocess.run")
    def test_verify_access_user_not_found(self, mock_run, mock_isdir):
        """Test when user is not found in ACL."""
        mock_isdir.return_value = True
        mock_result = Mock()
        mock_result.stdout = "user:otheruser:rwx\nother::---"  # Different user
        mock_run.return_value = mock_result

        with pytest.raises(ValidationError) as exc_info:
            self.verifier.verify_directory_access("/test/path", "testuser")

        assert "does not have read/execute access" in str(exc_info.value)


class TestNFS4DirectoryAccessVerifier:
    """Test NFS4 directory access verification."""

    def setup_method(self):
        """Set up test environment."""
        self.verifier = NFS4DirectoryAccessVerifier()

    @patch("os.path.isdir")
    @patch("subprocess.run")
    def test_verify_access_success_domain_format(self, mock_run, mock_isdir):
        """Test successful access verification with domain format."""
        mock_isdir.return_value = True
        mock_result = Mock()
        mock_result.stdout = "A::testuser@domain.com:rxD"
        mock_run.return_value = mock_result

        result = self.verifier.verify_directory_access("/test/path", "testuser")

        assert result is True
        mock_run.assert_called_once_with(
            ["nfs4_getfacl", "/test/path"], check=True, capture_output=True, text=True
        )

    @patch("os.path.isdir")
    @patch("subprocess.run")
    def test_verify_access_success_simple_format(self, mock_run, mock_isdir):
        """Test successful access verification with simple format."""
        mock_isdir.return_value = True
        mock_result = Mock()
        mock_result.stdout = "A::testuser:rxD"
        mock_run.return_value = mock_result

        result = self.verifier.verify_directory_access("/test/path", "testuser")

        assert result is True

    @patch("os.path.isdir")
    @patch("subprocess.run")
    def test_verify_access_insufficient_permissions(self, mock_run, mock_isdir):
        """Test access verification with insufficient permissions."""
        mock_isdir.return_value = True
        mock_result = Mock()
        mock_result.stdout = "A::testuser@domain.com:rD"  # No execute permission
        mock_run.return_value = mock_result

        with pytest.raises(ValidationError) as exc_info:
            self.verifier.verify_directory_access("/test/path", "testuser")

        assert "does not have read/execute access" in str(exc_info.value)

    @patch("os.path.isdir")
    @patch("subprocess.run")
    def test_verify_access_command_failure(self, mock_run, mock_isdir):
        """Test handling of nfs4_getfacl command failure."""
        mock_isdir.return_value = True
        mock_run.side_effect = subprocess.CalledProcessError(
            1, ["nfs4_getfacl"], stderr="Command not found"
        )

        with pytest.raises(ValidationError) as exc_info:
            self.verifier.verify_directory_access("/test/path", "testuser")

        assert "Failed to check NFSv4 permissions" in str(exc_info.value)


class TestVerifyDirectoryAccess:
    """Test the main verify_directory_access function."""

    @patch("ds_tools.filesystem.permissions.access_verification.NFSDirectoryAccessVerifier")
    def test_verify_nfs_access(self, mock_verifier_class):
        """Test NFS access verification through main function."""
        mock_verifier = Mock()
        mock_verifier.verify_directory_access.return_value = True
        mock_verifier_class.return_value = mock_verifier

        result = verify_directory_access(FilesystemType.nfs, "/test/path", "testuser")

        assert result is True
        mock_verifier.verify_directory_access.assert_called_once_with(
            "/test/path", "testuser"
        )

    @patch("ds_tools.filesystem.permissions.access_verification.NFS4DirectoryAccessVerifier")
    def test_verify_nfs4_access(self, mock_verifier_class):
        """Test NFS4 access verification through main function."""
        mock_verifier = Mock()
        mock_verifier.verify_directory_access.return_value = True
        mock_verifier_class.return_value = mock_verifier

        result = verify_directory_access(FilesystemType.nfs4, "/test/path", "testuser")

        assert result is True
        mock_verifier.verify_directory_access.assert_called_once_with(
            "/test/path", "testuser"
        )

    def test_verify_unsupported_filesystem(self):
        """Test error with unsupported filesystem type."""
        with pytest.raises(ValueError) as exc_info:
            verify_directory_access("unsupported", "/test/path", "testuser")

        assert "Unsupported filesystem type" in str(exc_info.value)
        assert "Available types" in str(exc_info.value)
