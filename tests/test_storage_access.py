"""Tests for unified storage access verification."""

from unittest.mock import patch

import pytest

from ds_tools.core.exceptions import ValidationError
from ds_tools.schemas import NFSStorageConfig, S3StorageConfig, SSHStorageConfig
from ds_tools.unified.storage_operations import verify_storage_access


class TestVerifyStorageAccess:
    """Test unified storage access verification."""

    @patch("ds_tools.unified.storage_operations.verify_s3_access")
    def test_verify_s3_access(self, mock_verify_s3):
        """Test S3 access verification."""
        mock_verify_s3.return_value = True

        config = S3StorageConfig(region_name="us-west-2")
        result = verify_storage_access(
            "s3://bucket/prefix", config, operation="read"
        )

        assert result is True
        mock_verify_s3.assert_called_once()

    @patch("ds_tools.unified.storage_operations.verify_s3_access")
    def test_verify_s3_access_with_credentials(self, mock_verify_s3):
        """Test S3 access verification with credentials."""
        mock_verify_s3.return_value = True

        config = S3StorageConfig(
            access_key_id="key123",
            secret_access_key="secret456",
            session_token="token789",
        )
        result = verify_storage_access(
            "s3://bucket/prefix", config, operation="write"
        )

        assert result is True
        mock_verify_s3.assert_called_once_with(
            s3_path="s3://bucket/prefix",
            operation="write",
            access_key_id="key123",
            secret_access_key="secret456",
            session_token="token789",
            region_name="us-east-1",
            endpoint_url=None,
            aws_profile=None,
        )

    @patch("ds_tools.unified.storage_operations.list_storage_contents")
    def test_verify_ssh_read_access(self, mock_list):
        """Test SSH read access verification."""
        mock_list.return_value = ["/remote/dir1", "/remote/dir2"]

        config = SSHStorageConfig(
            hostname="test.host", username="user", ssh_key_path="/key"
        )
        result = verify_storage_access("/remote/path", config, operation="read")

        assert result is True
        mock_list.assert_called_once_with(
            path="/remote/path",
            config=config,
            content_type="subdirectories",
            max_items=1,
            timeout=300,
        )

    @patch("ds_tools.unified.storage_operations.list_storage_contents")
    def test_verify_ssh_list_access(self, mock_list):
        """Test SSH list access verification."""
        mock_list.return_value = []

        config = SSHStorageConfig(
            hostname="test.host", username="user", ssh_key_path="/key"
        )
        result = verify_storage_access("/remote/path", config, operation="list")

        assert result is True

    @patch("ds_tools.unified.storage_operations.list_storage_contents")
    def test_verify_ssh_write_access_warning(self, mock_list):
        """Test SSH write access verification returns True with warning."""
        mock_list.return_value = []

        config = SSHStorageConfig(
            hostname="test.host", username="user", ssh_key_path="/key"
        )
        result = verify_storage_access("/remote/path", config, operation="write")

        assert result is True

    @patch("ds_tools.unified.storage_operations.list_storage_contents")
    def test_verify_ssh_access_denied(self, mock_list):
        """Test SSH access verification when access is denied."""
        mock_list.side_effect = PermissionError("Access denied")

        config = SSHStorageConfig(
            hostname="test.host", username="user", ssh_key_path="/key"
        )
        result = verify_storage_access("/remote/path", config)

        assert result is False

    @patch("ds_tools.unified.storage_operations.list_storage_contents")
    def test_verify_ssh_file_not_found(self, mock_list):
        """Test SSH access verification when path not found."""
        mock_list.side_effect = FileNotFoundError("Path not found")

        config = SSHStorageConfig(
            hostname="test.host", username="user", ssh_key_path="/key"
        )
        result = verify_storage_access("/remote/path", config)

        assert result is False

    @patch("ds_tools.unified.storage_operations.list_storage_contents")
    def test_verify_ssh_validation_error_propagated(self, mock_list):
        """Test SSH access verification propagates ValidationError."""
        mock_list.side_effect = ValidationError("Invalid path")

        config = SSHStorageConfig(
            hostname="test.host", username="user", ssh_key_path="/key"
        )

        with pytest.raises(ValidationError):
            verify_storage_access("/remote/path", config)

    @patch("ds_tools.unified.storage_operations.list_storage_contents")
    def test_verify_ssh_invalid_operation(self, mock_list):
        """Test SSH access verification with invalid operation."""
        mock_list.return_value = []  # Mock successful listing first

        config = SSHStorageConfig(
            hostname="test.host", username="user", ssh_key_path="/key"
        )

        with pytest.raises(ValidationError) as exc_info:
            verify_storage_access("/remote/path", config, operation="invalid")

        assert "Failed to verify storage access" in str(exc_info.value)
        assert "Unknown operation" in str(exc_info.value)

    @patch("ds_tools.unified.storage_operations.verify_directory_access")
    def test_verify_nfs_read_access(self, mock_verify_directory):
        """Test NFS read access verification."""
        mock_verify_directory.return_value = True

        config = NFSStorageConfig(base_path="/mnt/nfs")
        result = verify_storage_access(
            "/data/path", config, operation="read", username="testuser"
        )

        assert result is True
        mock_verify_directory.assert_called_once()

    @patch("ds_tools.unified.storage_operations.verify_directory_access")
    def test_verify_nfs4_list_access(self, mock_verify_directory):
        """Test NFS4 list access verification."""
        mock_verify_directory.return_value = True

        config = NFSStorageConfig(base_path="/mnt/nfs4")
        result = verify_storage_access(
            "/data/path", config, operation="list", username="testuser"
        )

        assert result is True

    def test_verify_nfs_missing_username(self):
        """Test NFS access verification without username."""
        config = NFSStorageConfig()

        with pytest.raises(ValidationError) as exc_info:
            verify_storage_access("/data/path", config)

        assert "Username required for NFS filesystem" in str(exc_info.value)

    def test_verify_nfs_write_not_implemented(self):
        """Test NFS write access verification not implemented."""
        config = NFSStorageConfig()

        with pytest.raises(ValidationError) as exc_info:
            verify_storage_access(
                "/data/path", config, operation="write", username="testuser"
            )

        assert "Write permission verification is not implemented" in str(exc_info.value)

    def test_verify_nfs_invalid_operation(self):
        """Test NFS access verification with invalid operation."""
        config = NFSStorageConfig()

        with pytest.raises(ValidationError) as exc_info:
            verify_storage_access(
                "/data/path", config, operation="invalid", username="testuser"
            )

        assert "Unknown operation" in str(exc_info.value)

    @patch("ds_tools.unified.storage_operations.verify_directory_access")
    def test_verify_storage_access_error_handling(self, mock_verify_directory):
        """Test error handling in storage access verification."""
        mock_verify_directory.side_effect = Exception("Access check failed")

        config = NFSStorageConfig()

        with pytest.raises(ValidationError) as exc_info:
            verify_storage_access("/bad/path", config, username="testuser")

        assert "Failed to verify storage access" in str(exc_info.value)
