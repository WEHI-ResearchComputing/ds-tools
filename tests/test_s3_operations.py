"""Tests for S3 object storage operations."""

import boto3
import pytest
from moto import mock_aws

from ds_tools.core.exceptions import ValidationError
from ds_tools.objectstorage import (
    PrefixMetrics,
    S3ClientConfig,
    S3ClientManager,
    analyze_prefix,
    list_objects_by_prefix,
    verify_s3_access,
)
from ds_tools.objectstorage.s3_operations import (
    analyze_s3_prefix,
    list_s3_objects,
    list_s3_prefixes,
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

    def setup_method(self, method):
        """Set up test environment."""
        self.config = S3ClientConfig(
            access_key_id="test_key",
            secret_access_key="test_secret",
            region_name="us-east-1",
            session_token=None,
            endpoint_url=None,
            aws_profile=None,
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
        metrics = analyze_s3_prefix(
            "s3://test-bucket/data",
            access_key_id="test_key",
            secret_access_key="test_secret",
            region_name="us-east-1",
        )

        assert isinstance(metrics, PrefixMetrics)
        assert metrics.bucket == "test-bucket"
        assert metrics.prefix == "data"
        assert metrics.object_count == 3
        assert metrics.total_bytes == 32  # 8 + 16 + 8 bytes

    def test_analyze_prefix_empty(self):
        """Test analysis of empty prefix."""
        metrics = analyze_s3_prefix(
            "s3://test-bucket/nonexistent",
            access_key_id="test_key",
            secret_access_key="test_secret",
            region_name="us-east-1",
        )

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

    def setup_method(self, method):
        """Set up test environment."""
        self.config = S3ClientConfig(
            access_key_id="test_key",
            secret_access_key="test_secret",
            region_name="us-east-1",
            session_token=None,
            endpoint_url=None,
            aws_profile=None,
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
        prefixes = list_s3_prefixes(
            "s3://test-bucket/data/",
            access_key_id="test-key",
            secret_access_key="test-secret",
            region_name="us-east-1",
        )

        expected_prefixes = [
            "s3://test-bucket/data/2023/",
            "s3://test-bucket/data/2024/",
            "s3://test-bucket/data/archive/",
        ]
        assert sorted(prefixes) == sorted(expected_prefixes)

    def test_list_objects(self):
        """Test listing objects."""
        objects = list_s3_objects(
            "s3://test-bucket/data/",
            access_key_id="test-key",
            secret_access_key="test-secret",
            region_name="us-east-1",
            max_keys=10,
        )

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

    def setup_method(self, method):
        """Set up test environment."""
        self.config = S3ClientConfig(
            access_key_id="test_key",
            secret_access_key="test_secret",
            region_name="us-east-1",
            session_token=None,
            endpoint_url=None,
            aws_profile=None,
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
        # Test basic list access on bucket root
        result = verify_s3_access(
            "s3://test-bucket/",
            operation="list",
            access_key_id="test_key",
            secret_access_key="test_secret",
            region_name="us-east-1",
        )
        assert result is True

    def test_verify_prefix_list_access(self):
        """Test prefix list access verification."""
        result = verify_s3_access(
            "s3://test-bucket/data",
            operation="list",
            access_key_id="test_key",
            secret_access_key="test_secret",
            region_name="us-east-1",
        )
        assert result is True

    def test_verify_prefix_read_access(self):
        """Test prefix read access verification."""
        result = verify_s3_access(
            "s3://test-bucket/data",
            operation="read",
            access_key_id="test_key",
            secret_access_key="test_secret",
            region_name="us-east-1",
        )
        assert result is True

    def test_verify_prefix_write_access(self):
        """Test prefix write access verification."""
        result = verify_s3_access(
            "s3://test-bucket/data",
            operation="write",
            access_key_id="test_key",
            secret_access_key="test_secret",
            region_name="us-east-1",
        )
        # Write test creates and aborts multipart upload
        assert result is True

    def test_get_accessible_operations(self):
        """Test getting all accessible operations."""
        # Test each operation individually since we don't have
        # get_accessible_operations function
        operations = []

        for op in ["list", "read", "write"]:
            try:
                result = verify_s3_access(
                    "s3://test-bucket/data",
                    operation=op,
                    access_key_id="test_key",
                    secret_access_key="test_secret",
                    region_name="us-east-1",
                )
                if result:
                    operations.append(op)
            except ValidationError:
                pass

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
        with pytest.raises(ValidationError, match="Invalid operation"):
            verify_s3_access(
                "s3://test-bucket/data",
                operation="invalid",
                access_key_id="test_key",
                secret_access_key="test_secret",
                region_name="us-east-1",
            )


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
        with pytest.raises((AttributeError, TypeError)):
            metrics.object_count = 100  # type: ignore
