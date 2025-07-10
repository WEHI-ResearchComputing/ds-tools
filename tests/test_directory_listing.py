"""Tests for unified storage listing operations."""

from unittest.mock import patch

import pytest

from ds_tools.core.exceptions import ValidationError
from ds_tools.schemas import NFSStorageConfig, S3StorageConfig, SSHStorageConfig
from ds_tools.unified.storage_operations import list_storage_contents


class TestListStorageContents:
    """Test unified storage listing functionality."""

    @patch("ds_tools.unified.storage_operations.list_local_subdirectories")
    def test_list_nfs_subdirectories(self, mock_list_local):
        """Test NFS subdirectory listing."""
        mock_list_local.return_value = ["/data/dir1", "/data/dir2"]

        config = NFSStorageConfig(base_path="/mnt/nfs")
        result = list_storage_contents("/data", config, content_type="subdirectories")

        assert result == ["/data/dir1", "/data/dir2"]
        mock_list_local.assert_called_once_with("/data", 300)

    @patch("ds_tools.unified.storage_operations.list_remote_subdirectories")
    def test_list_ssh_subdirectories(self, mock_list_remote):
        """Test SSH subdirectory listing."""
        mock_list_remote.return_value = ["/remote/dir1", "/remote/dir2"]

        config = SSHStorageConfig(
            hostname="test.host", username="user", ssh_key_path="/key"
        )
        result = list_storage_contents("/remote", config)

        assert result == ["/remote/dir1", "/remote/dir2"]
        mock_list_remote.assert_called_once_with(
            hostname="test.host",
            username="user",
            ssh_key="/key",
            path="/remote",
            timeout=300,
        )

    @patch("ds_tools.unified.storage_operations.list_objects_by_prefix")
    def test_list_s3_prefixes(self, mock_list_objects):
        """Test S3 prefix listing."""
        mock_list_objects.return_value = [
            "s3://bucket/prefix1/",
            "s3://bucket/prefix2/",
        ]

        config = S3StorageConfig(region_name="us-east-1")
        result = list_storage_contents(
            "s3://bucket/", config, content_type="subdirectories"
        )

        assert "s3://bucket/prefix1/" in result
        mock_list_objects.assert_called_once()

    @patch("ds_tools.unified.storage_operations.list_objects_by_prefix")
    def test_list_s3_objects(self, mock_list_objects):
        """Test S3 object listing."""
        mock_list_objects.return_value = [
            "s3://bucket/file1.txt",
            "s3://bucket/file2.txt",
        ]

        config = S3StorageConfig()
        result = list_storage_contents("s3://bucket/", config, content_type="files")

        assert "s3://bucket/file1.txt" in result
        mock_list_objects.assert_called_once()

    def test_list_invalid_content_type(self):
        """Test error with invalid content type."""
        config = NFSStorageConfig()
        with pytest.raises(ValidationError) as exc_info:
            list_storage_contents("/data", config, content_type="invalid")

        assert "content_type must be 'subdirectories' or 'files'" in str(exc_info.value)

    def test_list_ssh_files_not_implemented(self):
        """Test that SSH file listing raises error."""
        config = SSHStorageConfig(
            hostname="test.host", username="user", ssh_key_path="/key"
        )
        with pytest.raises(ValidationError) as exc_info:
            list_storage_contents("/remote", config, content_type="files")

        assert "File listing not implemented for SSH storage" in str(exc_info.value)

    @patch("ds_tools.unified.storage_operations.list_local_subdirectories")
    def test_list_storage_error_handling(self, mock_list_local):
        """Test error handling in storage listing."""
        mock_list_local.side_effect = Exception("Listing failed")

        config = NFSStorageConfig()
        with pytest.raises(ValidationError) as exc_info:
            list_storage_contents("/bad/path", config)

        assert "Failed to list storage contents" in str(exc_info.value)
