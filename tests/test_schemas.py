"""Tests for storage configuration schemas."""

import pytest
from pydantic import ValidationError

from ds_tools.schemas import (
    NFS4StorageConfig,
    NFSStorageConfig,
    S3StorageConfig,
    SSHStorageConfig,
)


class TestNFSStorageConfig:
    """Test NFS storage configuration."""

    def test_nfs_config_creation(self):
        """Test NFS configuration creation."""
        config = NFSStorageConfig(base_path="/mnt/nfs")
        assert config.type == "nfs"
        assert config.base_path == "/mnt/nfs"

    def test_nfs_config_defaults(self):
        """Test NFS configuration with defaults."""
        config = NFSStorageConfig()
        assert config.type == "nfs"
        assert config.base_path is None


class TestNFS4StorageConfig:
    """Test NFS4 storage configuration."""

    def test_nfs4_config_creation(self):
        """Test NFS4 configuration creation."""
        config = NFS4StorageConfig(base_path="/mnt/nfs4")
        assert config.type == "nfs4"
        assert config.base_path == "/mnt/nfs4"

    def test_nfs4_config_defaults(self):
        """Test NFS4 configuration with defaults."""
        config = NFS4StorageConfig()
        assert config.type == "nfs4"
        assert config.base_path is None


class TestSSHStorageConfig:
    """Test SSH storage configuration."""

    def test_ssh_config_creation(self):
        """Test SSH configuration creation."""
        config = SSHStorageConfig(
            hostname="test.host", username="user", ssh_key_path="/key"
        )
        assert config.type == "ssh"
        assert config.hostname == "test.host"
        assert config.username == "user"
        assert config.ssh_key_path == "/key"
        assert config.port == 22  # default

    def test_ssh_config_custom_port(self):
        """Test SSH configuration with custom port."""
        config = SSHStorageConfig(
            hostname="test.host", username="user", ssh_key_path="/key", port=2222
        )
        assert config.port == 2222

    def test_ssh_config_missing_required_fields(self):
        """Test SSH configuration validation with missing fields."""
        with pytest.raises(ValidationError):
            SSHStorageConfig(hostname="test.host")  # Missing username, ssh_key_path


class TestS3StorageConfig:
    """Test S3 storage configuration."""

    def test_s3_config_creation(self):
        """Test S3 configuration creation."""
        config = S3StorageConfig(
            access_key_id="key123",
            secret_access_key="secret456",
            region_name="us-west-2",
        )
        assert config.type == "s3"
        assert config.access_key_id == "key123"
        assert config.secret_access_key == "secret456"
        assert config.region_name == "us-west-2"

    def test_s3_config_defaults(self):
        """Test S3 configuration with defaults."""
        config = S3StorageConfig()
        assert config.type == "s3"
        assert config.access_key_id is None
        assert config.secret_access_key is None
        assert config.session_token is None
        assert config.region_name is None
        assert config.endpoint_url is None
        assert config.aws_profile is None

    def test_s3_config_with_profile(self):
        """Test S3 configuration with AWS profile."""
        config = S3StorageConfig(aws_profile="myprofile")
        assert config.aws_profile == "myprofile"

    def test_s3_config_with_session_token(self):
        """Test S3 configuration with session token."""
        config = S3StorageConfig(
            access_key_id="key",
            secret_access_key="secret",
            session_token="token123",
        )
        assert config.session_token == "token123"
