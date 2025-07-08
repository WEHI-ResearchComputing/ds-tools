"""S3 prefix analysis for calculating object counts and total sizes."""

from dataclasses import dataclass
from typing import Optional

from ds_tools.core import get_logger
from ds_tools.core.exceptions import CommandExecutionError, ValidationError
from ds_tools.objectstorage.clients import S3ClientConfig, S3ClientManager

logger = get_logger(__name__)


@dataclass(frozen=True)
class PrefixMetrics:
    """Metrics about objects under an S3 prefix.

    Attributes:
        object_count: Total number of objects under the prefix
        total_bytes: Total size in bytes of all objects
        bucket: S3 bucket name
        prefix: S3 prefix/path (empty string for bucket root)
    """

    object_count: int
    total_bytes: int
    bucket: str
    prefix: str


class S3PrefixAnalyzer:
    """Analyzes S3 prefixes for object counts and total sizes."""

    def __init__(self, config: S3ClientConfig):
        """Initialize S3 prefix analyzer.

        Args:
            config: S3 client configuration
        """
        self.client_manager = S3ClientManager(config)
        logger.info("S3 prefix analyzer initialized")

    def analyze_prefix(self, s3_path: str) -> PrefixMetrics:
        """Analyze S3 prefix to calculate object count and total size.

        Args:
            s3_path: S3 path in format s3://bucket/prefix

        Returns:
            PrefixMetrics containing object count and total size

        Raises:
            CommandExecutionError: If S3 operations fail
            ValidationError: If path format is invalid
        """
        logger.info("Analyzing S3 prefix", s3_path=s3_path)

        try:
            bucket, prefix = S3ClientManager.parse_s3_path(s3_path)
            client = self.client_manager.client

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


def analyze_prefix(
    s3_path: str,
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    session_token: Optional[str] = None,
    region_name: str = "us-east-1",
    endpoint_url: Optional[str] = None,
    aws_profile: Optional[str] = None,
) -> PrefixMetrics:
    """Convenience function to analyze S3 prefix metrics.

    Args:
        s3_path: S3 path in format s3://bucket/prefix
        access_key_id: AWS access key ID
        secret_access_key: AWS secret access key
        session_token: AWS session token for temporary credentials
        region_name: AWS region name
        endpoint_url: Custom S3 endpoint URL
        aws_profile: AWS CLI profile name

    Returns:
        PrefixMetrics containing object count and total size
    """
    config = S3ClientConfig(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        region_name=region_name,
        endpoint_url=endpoint_url,
        aws_profile=aws_profile,
    )

    analyzer = S3PrefixAnalyzer(config)
    return analyzer.analyze_prefix(s3_path)
