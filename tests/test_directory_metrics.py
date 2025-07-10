"""Tests for unified storage analysis operations."""

from unittest.mock import Mock, patch

import pytest

from ds_tools.core.exceptions import ValidationError
from ds_tools.schemas import NFSStorageConfig, S3StorageConfig, SSHStorageConfig
from ds_tools.unified.storage_operations import StorageMetrics, analyze_storage


class TestStorageMetrics:
    """Test StorageMetrics dataclass."""

    def test_storage_metrics_creation(self):
        """Test StorageMetrics creation and immutability."""
        metrics = StorageMetrics(
            item_count=42,
            total_bytes=1048576,
            storage_type="nfs",
            location="/test/path",
        )

        assert metrics.item_count == 42
        assert metrics.total_bytes == 1048576
        assert metrics.storage_type == "nfs"
        assert metrics.location == "/test/path"

        with pytest.raises(AttributeError):
            metrics.item_count = 100


class TestAnalyzeStorage:
    """Test unified storage analysis functionality."""

    @patch("ds_tools.unified.storage_operations.analyze_local_directory")
    def test_analyze_nfs_storage(self, mock_analyze_local):
        """Test NFS storage analysis."""
        mock_metrics = Mock()
        mock_metrics.file_count = 10
        mock_metrics.total_bytes = 2048
        mock_analyze_local.return_value = mock_metrics

        config = NFSStorageConfig(base_path="/mnt/nfs")
        result = analyze_storage("/data/test", config, timeout=60)

        assert isinstance(result, StorageMetrics)
        assert result.item_count == 10
        assert result.total_bytes == 2048
        assert result.storage_type == "nfs"
        assert result.location == "/data/test"
        mock_analyze_local.assert_called_once_with("/data/test", 60)

    @patch("ds_tools.unified.storage_operations.analyze_remote_directory")
    def test_analyze_ssh_storage(self, mock_analyze_remote):
        """Test SSH storage analysis."""
        mock_metrics = Mock()
        mock_metrics.file_count = 20
        mock_metrics.total_bytes = 4096
        mock_analyze_remote.return_value = mock_metrics

        config = SSHStorageConfig(
            hostname="test.host", username="user", ssh_key_path="/key"
        )
        result = analyze_storage("/remote/path", config)

        assert result.item_count == 20
        assert result.total_bytes == 4096
        assert result.storage_type == "ssh"
        mock_analyze_remote.assert_called_once_with(
            hostname="test.host",
            username="user",
            ssh_key="/key",
            path="/remote/path",
            timeout=300,
        )

    @patch("ds_tools.unified.storage_operations.analyze_prefix")
    def test_analyze_s3_storage(self, mock_analyze_prefix):
        """Test S3 storage analysis."""
        mock_metrics = Mock()
        mock_metrics.object_count = 15
        mock_metrics.total_bytes = 8192
        mock_analyze_prefix.return_value = mock_metrics

        config = S3StorageConfig(region_name="us-west-2")
        result = analyze_storage("s3://bucket/prefix", config)

        assert result.item_count == 15
        assert result.total_bytes == 8192
        assert result.storage_type == "s3"
        mock_analyze_prefix.assert_called_once()

    @patch("ds_tools.unified.storage_operations.analyze_local_directory")
    def test_analyze_storage_error_handling(self, mock_analyze_local):
        """Test error handling in storage analysis."""
        mock_analyze_local.side_effect = Exception("Analysis failed")

        config = NFSStorageConfig()
        with pytest.raises(ValidationError) as exc_info:
            analyze_storage("/bad/path", config)

        assert "Failed to analyze storage" in str(exc_info.value)
