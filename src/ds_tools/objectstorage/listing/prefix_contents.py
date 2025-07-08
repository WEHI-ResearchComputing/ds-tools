"""S3 prefix listing operations for finding common prefixes (subdirectories)."""

from typing import Optional

from ds_tools.core import get_logger
from ds_tools.core.exceptions import CommandExecutionError, ValidationError
from ds_tools.objectstorage.clients import S3ClientConfig, S3ClientManager

logger = get_logger(__name__)


class S3PrefixLister:
    """Lists common prefixes (subdirectory equivalents) under S3 prefixes."""

    def __init__(self, config: S3ClientConfig):
        """Initialize S3 prefix lister.

        Args:
            config: S3 client configuration
        """
        self.client_manager = S3ClientManager(config)
        logger.info("S3 prefix lister initialized")

    def list_common_prefixes(self, s3_path: str, delimiter: str = "/") -> list[str]:
        """List common prefixes (subdirectory equivalents) under an S3 prefix.

        This is equivalent to listing subdirectories in a filesystem.
        For example, if you have objects:
        - s3://bucket/data/2023/file1.txt
        - s3://bucket/data/2024/file2.txt

        Listing common prefixes for s3://bucket/data/ would return:
        - s3://bucket/data/2023/
        - s3://bucket/data/2024/

        Args:
            s3_path: S3 path in format s3://bucket/prefix
            delimiter: Delimiter for prefix grouping (usually "/")

        Returns:
            List of full S3 paths to common prefixes

        Raises:
            CommandExecutionError: If S3 operations fail
            ValidationError: If path format is invalid
        """
        logger.info("Listing S3 common prefixes", s3_path=s3_path)

        try:
            bucket, prefix = S3ClientManager.parse_s3_path(s3_path)
            client = self.client_manager.client

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

    def list_objects(self, s3_path: str, max_keys: int = 1000) -> list[str]:
        """List objects (files) directly under an S3 prefix.

        Args:
            s3_path: S3 path in format s3://bucket/prefix
            max_keys: Maximum number of objects to return

        Returns:
            List of full S3 paths to objects

        Raises:
            CommandExecutionError: If S3 operations fail
            ValidationError: If path format is invalid
        """
        logger.info("Listing S3 objects", s3_path=s3_path, max_keys=max_keys)

        try:
            bucket, prefix = S3ClientManager.parse_s3_path(s3_path)
            client = self.client_manager.client

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
    """Convenience function to list S3 prefixes or objects.

    Args:
        s3_path: S3 path in format s3://bucket/prefix
        list_type: Type of listing - "prefixes" for common prefixes or "objects"
        access_key_id: AWS access key ID
        secret_access_key: AWS secret access key
        session_token: AWS session token for temporary credentials
        region_name: AWS region name
        endpoint_url: Custom S3 endpoint URL
        aws_profile: AWS CLI profile name
        max_keys: Maximum number of items to return (for objects only)

    Returns:
        List of S3 paths (either prefixes or objects)

    Raises:
        ValidationError: If list_type is invalid
    """
    if list_type not in ("prefixes", "objects"):
        raise ValidationError(
            f"list_type must be 'prefixes' or 'objects', got: {list_type}"
        )

    config = S3ClientConfig(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        region_name=region_name,
        endpoint_url=endpoint_url,
        aws_profile=aws_profile,
    )

    lister = S3PrefixLister(config)

    if list_type == "prefixes":
        return lister.list_common_prefixes(s3_path)
    else:
        return lister.list_objects(s3_path, max_keys=max_keys)
