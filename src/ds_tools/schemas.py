"""Storage configuration schemas for ds-tools."""

from typing import Literal, Union

from pydantic import BaseModel, Field


class LocalStorageConfig(BaseModel):
    """Configuration for local filesystem storage."""
    type: Literal["local"] = "local"


class SSHStorageConfig(BaseModel):
    """Configuration for SSH remote storage."""
    type: Literal["ssh"] = "ssh"
    hostname: str = Field(..., description="SSH server hostname")
    username: str = Field(..., description="SSH username")
    ssh_key_path: str = Field(..., description="Path to SSH private key file")
    port: int = Field(default=22, description="SSH port")


class S3StorageConfig(BaseModel):
    """Configuration for S3 object storage."""
    type: Literal["s3"] = "s3"
    access_key_id: str | None = Field(default=None, description="AWS access key ID")
    secret_access_key: str | None = Field(
        default=None, description="AWS secret access key"
    )
    session_token: str | None = Field(default=None, description="AWS session token")
    region_name: str | None = Field(default=None, description="AWS region")
    endpoint_url: str | None = Field(default=None, description="Custom S3 endpoint URL")
    aws_profile: str | None = Field(default=None, description="AWS profile name")


# Discriminated union for storage configurations
StorageConfig = Union[LocalStorageConfig, SSHStorageConfig, S3StorageConfig]
