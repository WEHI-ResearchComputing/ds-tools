"""Tests for S3 object storage operations."""

import boto3
import pytest
from moto import mock_aws

from ds_tools.core.exceptions import ValidationError
from ds_tools.objectstorage import (
    PrefixMetrics,
    S3AccessVerifier,
    S3ClientConfig,
    S3ClientManager,
    S3PrefixAnalyzer,
    S3PrefixLister,
    analyze_prefix,
    list_objects_by_prefix,
    verify_s3_access,
)


class TestS3ClientManager:
    """Test S3 client management."""

    def test_parse_s3_path_valid(self):
        """Test parsing valid S3 paths."""
        bucket, prefix = S3ClientManager.parse_s3_path("s3://mybucket/myprefix")
        assert bucket == "mybucket"
        assert prefix == "myprefix"

        bucket, prefix = S3ClientManager.parse_s3_path("s3://mybucket/folder/subfolder")
        assert bucket == "mybucket"
        assert prefix == "folder/subfolder"

        bucket, prefix = S3ClientManager.parse_s3_path("s3://mybucket")
        assert bucket == "mybucket"
        assert prefix == ""

    def test_parse_s3_path_invalid(self):
        """Test parsing invalid S3 paths."""
        with pytest.raises(ValidationError):
            S3ClientManager.parse_s3_path("http://mybucket/myprefix")

        with pytest.raises(ValidationError):
            S3ClientManager.parse_s3_path("s3://")


@mock_aws
class TestS3PrefixAnalyzer:
    """Test S3 prefix analysis with mocked S3."""

    def setup_method(self, _method):
        """Set up test environment."""
        self.config = S3ClientConfig(
            access_key_id="test_key",
            secret_access_key="test_secret",
            region_name="us-east-1",
        )

        # Create mock S3 bucket and objects
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region_name="us-east-1",
        )
        self.s3_client.create_bucket(Bucket="test-bucket")

        # Add some test objects
        self.s3_client.put_object(
            Bucket="test-bucket", Key="data/file1.txt", Body=b"content1"
        )
        self.s3_client.put_object(
            Bucket="test-bucket", Key="data/file2.txt", Body=b"content2content2"
        )  # 16 bytes
        self.s3_client.put_object(
            Bucket="test-bucket", Key="data/subdir/file3.txt", Body=b"content3"
        )

    def test_analyze_prefix_success(self):
        """Test successful prefix analysis."""
        analyzer = S3PrefixAnalyzer(self.config)
        metrics = analyzer.analyze_prefix("s3://test-bucket/data")

        assert isinstance(metrics, PrefixMetrics)
        assert metrics.bucket == "test-bucket"
        assert metrics.prefix == "data"
        assert metrics.object_count == 3
        assert metrics.total_bytes == 32  # 8 + 16 + 8 bytes

    def test_analyze_prefix_empty(self):
        """Test analysis of empty prefix."""
        analyzer = S3PrefixAnalyzer(self.config)
        metrics = analyzer.analyze_prefix("s3://test-bucket/nonexistent")

        assert metrics.object_count == 0
        assert metrics.total_bytes == 0

    def test_analyze_prefix_convenience_function(self):
        """Test the convenience function."""
        metrics = analyze_prefix(
            "s3://test-bucket/data",
            access_key_id="test_key",
            secret_access_key="test_secret",
            region_name="us-east-1",
        )

        assert metrics.object_count == 3
        assert metrics.total_bytes == 32


@mock_aws
class TestS3PrefixLister:
    """Test S3 prefix listing with mocked S3."""

    def setup_method(self, _method):
        """Set up test environment."""
        self.config = S3ClientConfig(
            access_key_id="test_key",
            secret_access_key="test_secret",
            region_name="us-east-1",
        )

        # Create mock S3 bucket and objects
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region_name="us-east-1",
        )
        self.s3_client.create_bucket(Bucket="test-bucket")

        # Add objects to create prefixes
        self.s3_client.put_object(
            Bucket="test-bucket", Key="data/2023/file1.txt", Body=b"content1"
        )
        self.s3_client.put_object(
            Bucket="test-bucket", Key="data/2024/file2.txt", Body=b"content2"
        )
        self.s3_client.put_object(
            Bucket="test-bucket", Key="data/archive/file3.txt", Body=b"content3"
        )
        self.s3_client.put_object(
            Bucket="test-bucket", Key="data/file4.txt", Body=b"content4"
        )

    def test_list_common_prefixes(self):
        """Test listing common prefixes."""
        lister = S3PrefixLister(self.config)
        prefixes = lister.list_common_prefixes("s3://test-bucket/data/")

        expected_prefixes = [
            "s3://test-bucket/data/2023/",
            "s3://test-bucket/data/2024/",
            "s3://test-bucket/data/archive/",
        ]
        assert sorted(prefixes) == sorted(expected_prefixes)

    def test_list_objects(self):
        """Test listing objects."""
        lister = S3PrefixLister(self.config)
        objects = lister.list_objects("s3://test-bucket/data/", max_keys=10)

        assert len(objects) == 4
        assert "s3://test-bucket/data/file4.txt" in objects
        assert "s3://test-bucket/data/2023/file1.txt" in objects

    def test_list_objects_by_prefix_convenience(self):
        """Test the convenience function."""
        prefixes = list_objects_by_prefix(
            "s3://test-bucket/data/",
            list_type="prefixes",
            access_key_id="test_key",
            secret_access_key="test_secret",
            region_name="us-east-1",
        )

        assert len(prefixes) == 3
        assert all(p.startswith("s3://test-bucket/data/") for p in prefixes)


@mock_aws
class TestS3AccessVerifier:
    """Test S3 access verification with mocked S3."""

    def setup_method(self, _method):
        """Set up test environment."""
        self.config = S3ClientConfig(
            access_key_id="test_key",
            secret_access_key="test_secret",
            region_name="us-east-1",
        )

        # Create mock S3 bucket and objects
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region_name="us-east-1",
        )
        self.s3_client.create_bucket(Bucket="test-bucket")
        self.s3_client.put_object(
            Bucket="test-bucket", Key="data/file1.txt", Body=b"content1"
        )

    def test_verify_bucket_access(self):
        """Test bucket access verification."""
        verifier = S3AccessVerifier(self.config)
        assert verifier.verify_bucket_access("test-bucket") is True

    def test_verify_prefix_list_access(self):
        """Test prefix list access verification."""
        verifier = S3AccessVerifier(self.config)
        assert verifier.verify_prefix_access("s3://test-bucket/data", "list") is True

    def test_verify_prefix_read_access(self):
        """Test prefix read access verification."""
        verifier = S3AccessVerifier(self.config)
        assert verifier.verify_prefix_access("s3://test-bucket/data", "read") is True

    def test_verify_prefix_write_access(self):
        """Test prefix write access verification."""
        verifier = S3AccessVerifier(self.config)
        # Write test creates and aborts multipart upload
        assert verifier.verify_prefix_access("s3://test-bucket/data", "write") is True

    def test_get_accessible_operations(self):
        """Test getting all accessible operations."""
        verifier = S3AccessVerifier(self.config)
        operations = verifier.get_accessible_operations("s3://test-bucket/data")

        assert "list" in operations
        assert "read" in operations
        assert "write" in operations

    def test_verify_s3_access_convenience(self):
        """Test the convenience function."""
        result = verify_s3_access(
            "s3://test-bucket/data",
            operation="list",
            access_key_id="test_key",
            secret_access_key="test_secret",
            region_name="us-east-1",
        )
        assert result is True

    def test_invalid_operation(self):
        """Test invalid operation parameter."""
        verifier = S3AccessVerifier(self.config)
        with pytest.raises(ValidationError, match="Invalid operation"):
            verifier.verify_prefix_access("s3://test-bucket/data", "invalid")


class TestPrefixMetrics:
    """Test PrefixMetrics dataclass."""

    def test_prefix_metrics_creation(self):
        """Test PrefixMetrics creation and immutability."""
        metrics = PrefixMetrics(
            object_count=42, total_bytes=1048576, bucket="mybucket", prefix="myprefix"
        )

        assert metrics.object_count == 42
        assert metrics.total_bytes == 1048576
        assert metrics.bucket == "mybucket"
        assert metrics.prefix == "myprefix"

        # Test immutability (frozen dataclass)
        with pytest.raises(AttributeError):
            metrics.object_count = 100
