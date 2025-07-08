"""S3 client configuration and management.

This module provides S3 client configuration and management functionality
with support for multiple authentication methods and S3-compatible services.

The S3ClientManager handles the complexity of boto3 client creation with
different credential sources and provides utilities for S3 path parsing.

Authentication Methods Supported:
    1. Explicit credentials (access_key_id, secret_access_key)
    2. AWS CLI profiles (aws_profile)
    3. IAM roles / environment variables (no explicit credentials)
    4. Temporary credentials (session_token)

S3-Compatible Services:
    Supports custom endpoints for services like MinIO, DigitalOcean Spaces,
    and other S3-compatible object storage providers via endpoint_url.
"""

from typing import Optional
from urllib.parse import urlparse

import boto3
from pydantic import BaseModel, ConfigDict, Field

from ds_tools.core import get_logger
from ds_tools.core.exceptions import ValidationError

logger = get_logger(__name__)


class S3ClientConfig(BaseModel):
    """Configuration for S3 client connections.

    This class encapsulates all configuration needed to create an S3 client,
    supporting multiple authentication methods and S3-compatible services.

    The configuration follows AWS SDK conventions and provides validation
    for required parameters based on the authentication method chosen.

    Authentication Priority:
        1. If aws_profile is provided, use profile-based authentication
        2. If explicit credentials are provided, use them
        3. Otherwise, fall back to default AWS credential chain

    Example:
        # Explicit credentials
        config = S3ClientConfig(
            access_key_id="AKIAIOSFODNN7EXAMPLE",
            secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        )

        # AWS profile
        config = S3ClientConfig(aws_profile="my-profile")

        # MinIO endpoint
        config = S3ClientConfig(
            endpoint_url="http://localhost:9000",
            access_key_id="minioadmin",
            secret_access_key="minioadmin"
        )
    """

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


class S3ClientManager:
    """Manages S3 client connections and provides utility methods."""

    def __init__(self, config: S3ClientConfig):
        """Initialize S3 client manager.

        Args:
            config: S3 client configuration
        """
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
        kwargs = {
            "service_name": "s3",
            "region_name": self.config.region_name,
        }

        # Add endpoint URL for S3-compatible services
        if self.config.endpoint_url:
            kwargs["endpoint_url"] = self.config.endpoint_url

        # Use profile if specified, otherwise use explicit credentials
        if self.config.aws_profile:
            session = boto3.Session(profile_name=self.config.aws_profile)
            client = session.client(**kwargs)
            logger.info(
                "S3 client created with profile", profile=self.config.aws_profile
            )
        else:
            # Use explicit credentials if provided
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

            client = boto3.client(**kwargs)

        return client

    @staticmethod
    def parse_s3_path(s3_path: str) -> tuple[str, str]:
        """Parse S3 path into bucket and prefix components.

        Args:
            s3_path: S3 path in format s3://bucket/prefix or s3://bucket

        Returns:
            Tuple of (bucket_name, prefix)

        Raises:
            ValidationError: If path format is invalid
        """
        if not s3_path.startswith("s3://"):
            raise ValidationError(f"S3 path must start with 's3://': {s3_path}")

        try:
            parsed = urlparse(s3_path)
            bucket = parsed.netloc
            prefix = parsed.path.lstrip("/")  # Remove leading slash

            if not bucket:
                raise ValidationError(f"Invalid S3 path, missing bucket: {s3_path}")

            logger.debug("S3 path parsed", bucket=bucket, prefix=prefix)
            return bucket, prefix

        except Exception as e:
            raise ValidationError(f"Failed to parse S3 path '{s3_path}': {e}")

    def test_connection(self) -> bool:
        """Test S3 connection by listing buckets.

        Returns:
            True if connection successful

        Raises:
            ValidationError: If connection fails
        """
        try:
            self.client.list_buckets()
            logger.info("S3 connection test successful")
            return True
        except Exception as e:
            error_msg = f"S3 connection test failed: {e}"
            logger.error(error_msg)
            raise ValidationError(error_msg)
