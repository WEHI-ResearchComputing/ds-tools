"""Unified storage configuration to reduce parameter explosion."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class SSHConfig:
    """SSH connection configuration."""

    hostname: str
    username: str
    ssh_key: str


@dataclass
class S3Config:
    """S3 client configuration."""

    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    session_token: Optional[str] = None
    region_name: str = "us-east-1"
    endpoint_url: Optional[str] = None
    aws_profile: Optional[str] = None


@dataclass
class StorageConfig:
    """Unified storage configuration."""

    ssh: Optional[SSHConfig] = None
    s3: Optional[S3Config] = None
    timeout: int = 300

    @classmethod
    def from_ssh(
        cls, hostname: str, username: str, ssh_key: str, timeout: int = 300
    ) -> "StorageConfig":
        """Create config for SSH operations."""
        return cls(ssh=SSHConfig(hostname, username, ssh_key), timeout=timeout)

    @classmethod
    def from_s3(
        cls,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        session_token: Optional[str] = None,
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        aws_profile: Optional[str] = None,
        timeout: int = 300
    ) -> "StorageConfig":
        """Create config for S3 operations."""
        return cls(
            s3=S3Config(
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                session_token=session_token,
                region_name=region_name,
                endpoint_url=endpoint_url,
                aws_profile=aws_profile,
            ),
            timeout=timeout
        )

