"""S3 access verification for bucket and prefix permissions."""

from typing import Optional

from ds_tools.core import get_logger
from ds_tools.core.exceptions import ValidationError
from ds_tools.objectstorage.clients import S3ClientConfig, S3ClientManager

logger = get_logger(__name__)


class S3AccessVerifier:
    """Verifies S3 access permissions for buckets and prefixes."""

    def __init__(self, config: S3ClientConfig):
        """Initialize S3 access verifier.

        Args:
            config: S3 client configuration
        """
        self.client_manager = S3ClientManager(config)
        logger.info("S3 access verifier initialized")

    def verify_bucket_access(self, bucket: str) -> bool:
        """Verify access to an S3 bucket.

        Tests basic bucket access by attempting to list objects (without actually
        retrieving any objects to minimize cost/latency).

        Args:
            bucket: S3 bucket name

        Returns:
            True if bucket is accessible

        Raises:
            ValidationError: If access verification fails
        """
        logger.info("Verifying S3 bucket access", bucket=bucket)

        try:
            client = self.client_manager.client

            # Try to list objects with MaxKeys=0 to test access without downloading data
            client.list_objects_v2(Bucket=bucket, MaxKeys=0)

            logger.info("S3 bucket access verified", bucket=bucket)
            return True

        except Exception as e:
            error_msg = f"S3 bucket access verification failed for '{bucket}': {e}"
            logger.warning(error_msg)
            raise ValidationError(error_msg)

    def verify_prefix_access(self, s3_path: str, operation: str = "read") -> bool:
        """Verify access to an S3 prefix for a specific operation.

        Args:
            s3_path: S3 path in format s3://bucket/prefix
            operation: Operation to test ("read", "write", "list")

        Returns:
            True if operation is permitted on the prefix

        Raises:
            ValidationError: If access verification fails or operation is invalid
        """
        if operation not in ("read", "write", "list"):
            raise ValidationError(
                f"Invalid operation: {operation}. Must be 'read', 'write', or 'list'"
            )

        logger.info("Verifying S3 prefix access", s3_path=s3_path, operation=operation)

        try:
            bucket, prefix = S3ClientManager.parse_s3_path(s3_path)
            client = self.client_manager.client

            if operation == "list":
                # Test list operation
                client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)

            elif operation == "read":
                # Test by listing objects and attempting to get metadata
                response = client.list_objects_v2(
                    Bucket=bucket, Prefix=prefix, MaxKeys=1
                )
                if "Contents" in response and response["Contents"]:
                    # Try to get object metadata (head_object)
                    first_object = response["Contents"][0]["Key"]
                    client.head_object(Bucket=bucket, Key=first_object)
                # If no objects exist, list permission is sufficient proof

            elif operation == "write":
                # Test write permission using a safe, non-destructive approach:
                # 1. Create a multipart upload (requires write permission)
                # 2. Immediately abort it (cleans up without writing data)
                # This approach verifies write access without actually creating objects
                test_key = f"{prefix}/.ds-tools-access-test"
                try:
                    # Initiate multipart upload - this requires s3:PutObject permission
                    response = client.create_multipart_upload(
                        Bucket=bucket, Key=test_key
                    )
                    upload_id = response["UploadId"]

                    # Immediately abort to clean up - requires s3:AbortMultipartUpload
                    # This ensures no storage is consumed and no objects are created
                    client.abort_multipart_upload(
                        Bucket=bucket, Key=test_key, UploadId=upload_id
                    )
                except Exception as e:
                    # Any failure indicates insufficient write permissions
                    raise ValidationError(f"Write access test failed: {e}")

            logger.info(
                "S3 prefix access verified", s3_path=s3_path, operation=operation
            )
            return True

        except ValidationError:
            raise
        except Exception as e:
            error_msg = (
                f"S3 prefix access verification failed for '{s3_path}' "
                f"operation '{operation}': {e}"
            )
            logger.warning(error_msg)
            raise ValidationError(error_msg)

    def get_accessible_operations(self, s3_path: str) -> list[str]:
        """Get list of operations that are accessible for an S3 path.

        Args:
            s3_path: S3 path in format s3://bucket/prefix

        Returns:
            List of accessible operations: ["read", "write", "list"]
        """
        logger.info("Checking accessible operations", s3_path=s3_path)

        accessible_ops = []

        for operation in ["list", "read", "write"]:
            try:
                if self.verify_prefix_access(s3_path, operation):
                    accessible_ops.append(operation)
            except ValidationError:
                # Operation not accessible, continue checking others
                pass

        logger.info(
            "Accessible operations determined",
            s3_path=s3_path,
            operations=accessible_ops,
        )
        return accessible_ops


def verify_s3_access(
    s3_path: str,
    operation: str = "read",
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    session_token: Optional[str] = None,
    region_name: str = "us-east-1",
    endpoint_url: Optional[str] = None,
    aws_profile: Optional[str] = None,
) -> bool:
    """Convenience function to verify S3 access.

    Args:
        s3_path: S3 path in format s3://bucket/prefix
        operation: Operation to test ("read", "write", "list")
        access_key_id: AWS access key ID
        secret_access_key: AWS secret access key
        session_token: AWS session token for temporary credentials
        region_name: AWS region name
        endpoint_url: Custom S3 endpoint URL
        aws_profile: AWS CLI profile name

    Returns:
        True if operation is permitted
    """
    config = S3ClientConfig(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        region_name=region_name,
        endpoint_url=endpoint_url,
        aws_profile=aws_profile,
    )

    verifier = S3AccessVerifier(config)
    return verifier.verify_prefix_access(s3_path, operation)
