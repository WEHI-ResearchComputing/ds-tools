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


