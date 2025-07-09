"""S3 operations for analysis, listing, and access verification.

This module consolidates all S3 operations into a single, maintainable interface
while preserving the same functionality as the original modular structure.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import boto3
from pydantic import BaseModel, ConfigDict, Field

from ds_tools.core import get_logger
from ds_tools.core.exceptions import CommandExecutionError, ValidationError

logger = get_logger(__name__)


# Configuration
class S3ClientConfig(BaseModel):
    """Configuration for S3 client connections."""

    model_config = ConfigDict(extra="forbid")

    access_key_id: Optional[str] = Field(None, description="AWS access key ID")
    secret_access_key: Optional[str] = Field(None, description="AWS secret access key")
    session_token: Optional[str] = Field(
        None, description="AWS session token for temporary credentials"
    )
    region_name: str = Field("us-east-1", description="AWS region name")
    endpoint_url: Optional[str] = Field(
        None, description="Custom S3 endpoint URL for S3-compatible services"
    )
    aws_profile: Optional[str] = Field(
        None, description="AWS CLI profile name to use for credentials"
    )


@dataclass(frozen=True)
class PrefixMetrics:
    """Metrics about objects under an S3 prefix."""

    object_count: int
    total_bytes: int
    bucket: str
    prefix: str


# Core S3 client management
class S3ClientManager:
    """Manages S3 client connections and provides utility methods."""

    def __init__(self, config: S3ClientConfig):
        self.config = config
        self._client = None
        logger.info("S3 client manager initialized", region=config.region_name)

    @property
    def client(self):
        """Get or create S3 client instance."""
        if self._client is None:
            self._client = self._create_client()
        return self._client

    def _create_client(self):
        """Create boto3 S3 client with the configured settings."""
        kwargs: Dict[str, Any] = {
            "region_name": self.config.region_name,
        }

        if self.config.endpoint_url:
            kwargs["endpoint_url"] = self.config.endpoint_url

        if self.config.aws_profile:
            session = boto3.Session(profile_name=self.config.aws_profile)
            client = session.client("s3", **kwargs)  # type: ignore
            logger.info(
                "S3 client created with profile", profile=self.config.aws_profile
            )
        else:
            if self.config.access_key_id and self.config.secret_access_key:
                kwargs.update(
                    {
                        "aws_access_key_id": self.config.access_key_id,
                        "aws_secret_access_key": self.config.secret_access_key,
                    }
                )
                if self.config.session_token:
                    kwargs["aws_session_token"] = self.config.session_token
                logger.info("S3 client created with explicit credentials")
            else:
                logger.info("S3 client created with default credential chain")

            client = boto3.client("s3", **kwargs)  # type: ignore

        return client

    @staticmethod
    def parse_s3_path(s3_path: str) -> tuple[str, str]:
        """Parse S3 path into bucket and prefix components."""
        if not s3_path.startswith("s3://"):
            raise ValidationError(f"S3 path must start with 's3://': {s3_path}")

        try:
            parsed = urlparse(s3_path)
            bucket = parsed.netloc
            prefix = parsed.path.lstrip("/")

            if not bucket:
                raise ValidationError(f"Invalid S3 path, missing bucket: {s3_path}")

            logger.debug("S3 path parsed", bucket=bucket, prefix=prefix)
            return bucket, prefix

        except Exception as e:
            raise ValidationError(f"Failed to parse S3 path '{s3_path}': {e}")


# Analysis operations
def analyze_s3_prefix(
    s3_path: str,
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    session_token: Optional[str] = None,
    region_name: str = "us-east-1",
    endpoint_url: Optional[str] = None,
    aws_profile: Optional[str] = None,
) -> PrefixMetrics:
    """Analyze S3 prefix to calculate object count and total size."""
    logger.info("Analyzing S3 prefix", s3_path=s3_path)

    config = S3ClientConfig(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        region_name=region_name,
        endpoint_url=endpoint_url,
        aws_profile=aws_profile,
    )

    try:
        client_manager = S3ClientManager(config)
        bucket, prefix = S3ClientManager.parse_s3_path(s3_path)
        client = client_manager.client

        object_count = 0
        total_bytes = 0

        # Use paginator to handle large numbers of objects
        paginator = client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    object_count += 1
                    total_bytes += obj.get("Size", 0)

        metrics = PrefixMetrics(
            object_count=object_count,
            total_bytes=total_bytes,
            bucket=bucket,
            prefix=prefix,
        )

        logger.info(
            "S3 prefix analysis completed",
            bucket=bucket,
            prefix=prefix,
            object_count=metrics.object_count,
            total_bytes=metrics.total_bytes,
        )
        return metrics

    except ValidationError:
        raise
    except Exception as e:
        error_msg = f"Failed to analyze S3 prefix '{s3_path}': {e}"
        logger.error(error_msg, error=str(e))
        raise CommandExecutionError(error_msg)


# Listing operations
def list_s3_prefixes(
    s3_path: str,
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    session_token: Optional[str] = None,
    region_name: str = "us-east-1",
    endpoint_url: Optional[str] = None,
    aws_profile: Optional[str] = None,
    delimiter: str = "/",
) -> list[str]:
    """List common prefixes (subdirectory equivalents) under an S3 prefix."""
    logger.info("Listing S3 common prefixes", s3_path=s3_path)

    config = S3ClientConfig(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        region_name=region_name,
        endpoint_url=endpoint_url,
        aws_profile=aws_profile,
    )

    try:
        client_manager = S3ClientManager(config)
        bucket, prefix = S3ClientManager.parse_s3_path(s3_path)
        client = client_manager.client

        # Ensure prefix ends with delimiter if it's not empty
        if prefix and not prefix.endswith(delimiter):
            prefix += delimiter

        common_prefixes = []

        # Use paginator to handle large numbers of prefixes
        paginator = client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=bucket, Prefix=prefix, Delimiter=delimiter
        )

        for page in page_iterator:
            if "CommonPrefixes" in page:
                for prefix_info in page["CommonPrefixes"]:
                    prefix_name = prefix_info["Prefix"]
                    # Convert back to full S3 path
                    full_s3_path = f"s3://{bucket}/{prefix_name}"
                    common_prefixes.append(full_s3_path)

        logger.info(
            "S3 common prefixes listed",
            bucket=bucket,
            prefix=prefix,
            prefix_count=len(common_prefixes),
        )
        return common_prefixes

    except ValidationError:
        raise
    except Exception as e:
        error_msg = f"Failed to list S3 common prefixes for '{s3_path}': {e}"
        logger.error(error_msg, error=str(e))
        raise CommandExecutionError(error_msg)


def list_s3_objects(
    s3_path: str,
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    session_token: Optional[str] = None,
    region_name: str = "us-east-1",
    endpoint_url: Optional[str] = None,
    aws_profile: Optional[str] = None,
    max_keys: int = 1000,
) -> list[str]:
    """List objects (files) directly under an S3 prefix."""
    logger.info("Listing S3 objects", s3_path=s3_path, max_keys=max_keys)

    config = S3ClientConfig(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        region_name=region_name,
        endpoint_url=endpoint_url,
        aws_profile=aws_profile,
    )

    try:
        client_manager = S3ClientManager(config)
        bucket, prefix = S3ClientManager.parse_s3_path(s3_path)
        client = client_manager.client

        objects = []

        # List objects with pagination
        paginator = client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=bucket, Prefix=prefix, PaginationConfig={"MaxItems": max_keys}
        )

        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    object_key = obj["Key"]
                    # Convert back to full S3 path
                    full_s3_path = f"s3://{bucket}/{object_key}"
                    objects.append(full_s3_path)

        logger.info(
            "S3 objects listed",
            bucket=bucket,
            prefix=prefix,
            object_count=len(objects),
        )
        return objects

    except ValidationError:
        raise
    except Exception as e:
        error_msg = f"Failed to list S3 objects for '{s3_path}': {e}"
        logger.error(error_msg, error=str(e))
        raise CommandExecutionError(error_msg)


# Access verification operations
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
    """Verify access to an S3 prefix for a specific operation."""
    if operation not in ("read", "write", "list"):
        raise ValidationError(
            f"Invalid operation: {operation}. Must be 'read', 'write', or 'list'"
        )

    logger.info("Verifying S3 prefix access", s3_path=s3_path, operation=operation)

    config = S3ClientConfig(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        region_name=region_name,
        endpoint_url=endpoint_url,
        aws_profile=aws_profile,
    )

    try:
        client_manager = S3ClientManager(config)
        bucket, prefix = S3ClientManager.parse_s3_path(s3_path)
        client = client_manager.client

        if operation == "list":
            # Test list operation
            client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)

        elif operation == "read":
            # Test by listing objects and attempting to get metadata
            response = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            if "Contents" in response and response["Contents"]:
                # Try to get object metadata (head_object)
                first_object = response["Contents"][0]["Key"]
                client.head_object(Bucket=bucket, Key=first_object)

        elif operation == "write":
            # Test write permission using multipart upload test
            test_key = f"{prefix}/.ds-tools-access-test"
            try:
                # Initiate multipart upload - this requires s3:PutObject permission
                response = client.create_multipart_upload(Bucket=bucket, Key=test_key)
                upload_id = response["UploadId"]

                # Immediately abort to clean up
                client.abort_multipart_upload(
                    Bucket=bucket, Key=test_key, UploadId=upload_id
                )
            except Exception as e:
                raise ValidationError(f"Write access test failed: {e}")

        logger.info("S3 prefix access verified", s3_path=s3_path, operation=operation)
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


# Convenience function for unified interface compatibility
def list_objects_by_prefix(
    s3_path: str,
    list_type: str = "prefixes",
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    session_token: Optional[str] = None,
    region_name: str = "us-east-1",
    endpoint_url: Optional[str] = None,
    aws_profile: Optional[str] = None,
    max_keys: int = 1000,
) -> list[str]:
    """List S3 prefixes or objects (compatibility function)."""
    if list_type not in ("prefixes", "objects"):
        raise ValidationError(
            f"list_type must be 'prefixes' or 'objects', got: {list_type}"
        )

    if list_type == "prefixes":
        return list_s3_prefixes(
            s3_path=s3_path,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_profile=aws_profile,
        )
    else:
        return list_s3_objects(
            s3_path=s3_path,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_profile=aws_profile,
            max_keys=max_keys,
        )


# Legacy compatibility - maintain old function names
def analyze_prefix(
    s3_path: str,
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    session_token: Optional[str] = None,
    region_name: str = "us-east-1",
    endpoint_url: Optional[str] = None,
    aws_profile: Optional[str] = None,
) -> PrefixMetrics:
    """Legacy compatibility function for analyze_prefix."""
    return analyze_s3_prefix(
        s3_path=s3_path,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        region_name=region_name,
        endpoint_url=endpoint_url,
        aws_profile=aws_profile,
    )
