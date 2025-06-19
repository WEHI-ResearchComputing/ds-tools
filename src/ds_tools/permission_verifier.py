from abc import ABC, abstractmethod
from typing import Optional
from enum import Enum
import os
import subprocess
from pydantic import BaseModel, Field, ConfigDict


class FilesystemType(str, Enum):
    nfs = "nfs"
    nfs4 = "nfs4"
    s3 = "s3"


class PermissionVerifier(ABC):
    """Abstract base class for verifying folder permissions."""

    @abstractmethod
    def verify_permissions(self, path: str, user: str) -> bool:
        """
        Verify that the user has read and execute permissions.

        Args:
            path: The directory path to check
            user: The user to verify permissions for

        Returns:
            bool: True if user has read and execute permissions

        Raises:
            NotADirectoryError: If path doesn't exist or isn't a directory
            PermissionError: If user doesn't have required permissions
        """
        pass


class NFSPermissionVerifier(PermissionVerifier):
    """Verify permissions using getfacl for NFS (POSIX ACLs)."""

    def verify_permissions(self, path: str, user: str) -> bool:
        if not os.path.isdir(path):
            raise NotADirectoryError(
                f"Path {path} does not exist or is not a directory"
            )

        command = ["getfacl", path]
        output = subprocess.run(command, check=True, capture_output=True, text=True)

        for line in output.stdout.splitlines():
            if line.startswith(f"user:{user}:"):
                permissions = line.split(":")[-1]
                has_read = "r" in permissions
                has_execute = "x" in permissions
                return has_read and has_execute

        raise PermissionError(
            f"{user} does not have read/execute permissions on path {path}"
        )


class NFS4PermissionVerifier(PermissionVerifier):
    """Verify permissions using nfs4_getfacl for NFSv4 ACLs."""

    def verify_permissions(self, path: str, user: str) -> bool:
        if not os.path.isdir(path):
            raise NotADirectoryError(
                f"Path {path} does not exist or is not a directory"
            )

        command = ["nfs4_getfacl", path]
        output = subprocess.run(command, check=True, capture_output=True, text=True)

        for line in output.stdout.splitlines():
            if line.startswith(f"A::{user}@") or line.startswith(f"A::{user}:"):
                parts = line.split(":")
                if len(parts) >= 4:
                    permissions = parts[3]
                    has_read = "r" in permissions
                    has_execute = "x" in permissions
                    return has_read and has_execute

        raise PermissionError(
            f"{user} does not have read/execute permissions on path {path}"
        )


class S3Config(BaseModel):
    """Configuration for S3-compatible object storage."""

    model_config = ConfigDict(extra="forbid")

    access_key_id: str = Field(description="S3 access key ID")
    secret_access_key: str = Field(description="S3 secret access key")
    endpoint_url: Optional[str] = Field(
        None, description="S3-compatible endpoint URL (leave None for AWS)"
    )
    region_name: str = Field("us-east-1", description="Region name")
    session_token: Optional[str] = Field(
        None, description="Session token for temporary credentials"
    )


class S3PermissionVerifier(PermissionVerifier):
    """Verify permissions for S3-compatible object storage using boto3."""

    def __init__(self, config: S3Config):
        self.config = config

    def verify_permissions(self, path: str, user: str = "") -> bool:
        if not path.startswith("s3://"):
            raise ValueError(f"S3 path must start with 's3://': {path}")

        # TODO: Implement S3 permission checking
        # import boto3
        #
        # s3 = boto3.client(
        #     's3',
        #     aws_access_key_id=self.config.access_key_id,
        #     aws_secret_access_key=self.config.secret_access_key,
        #     aws_session_token=self.config.session_token,
        #     endpoint_url=self.config.endpoint_url,
        #     region_name=self.config.region_name
        # )
        #
        # bucket_name, prefix = parse_s3_path(path)
        # Check bucket ACLs/policies for user permissions

        raise NotImplementedError("S3 permission verification not yet implemented")


class PermissionVerifierFactory:
    """Factory for creating permission verifier instances."""

    @staticmethod
    def create_verifier(
        filesystem_type: FilesystemType, **config
    ) -> PermissionVerifier:
        """
        Create a permission verifier instance.

        Args:
            filesystem_type: Type of filesystem ('nfs', 'nfs4', 's3')
            **config: Configuration for specific filesystem types:
                     - For 's3': access_key_id, secret_access_key, endpoint_url, region_name, session_token
                     - For 'nfs', 'nfs4': no config needed

        Returns:
            PermissionVerifier: Instance of the requested verifier
        """
        fs_type = filesystem_type.lower()

        if fs_type == FilesystemType.nfs:
            return NFSPermissionVerifier()
        elif fs_type == FilesystemType.nfs4:
            return NFS4PermissionVerifier()
        elif fs_type == FilesystemType.s3:
            s3_config = S3Config(**config)
            return S3PermissionVerifier(s3_config)
        else:
            raise ValueError(
                f"Unsupported filesystem type: {filesystem_type}. "
                f"Available types: {', '.join([e.value for e in FilesystemType])}"
            )
