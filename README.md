# ds-tools

Library of tools and integrations to help build dynamic user interfaces and complex Airflow workflows.

## Overview

`ds-tools` provides a unified interface for storage operations across NFS/NFS4 filesystems, remote SSH connections, and S3-compatible object storage. It's designed to be integrated with the WEHI Datasets Service backend to expose RPC-like endpoints for enhanced UI and workflow capabilities.

## Features

- **Unified Storage Operations**: Single API for NFS, NFS4, SSH, and S3 storage
- **Storage Analysis**: Calculate file counts and total sizes across different storage types
- **Directory Listing**: List subdirectories/prefixes and files/objects
- **Access Verification**: Verify read/write permissions for different storage backends
- **CLI Interface**: Command-line tools for all storage operations
- **Type Safety**: Full TypeScript-style type annotations and runtime validation

## Architecture

The library follows a layered architecture similar to `ds-backend`:

- **Core module**: Configuration, exceptions, and observability setup
- **Unified module**: Single interface for all storage operations
- **Domain modules**: Specific functionality (filesystem, object storage)
- **CLI**: Command-line interface using Typer with shared parameter utilities
- **Testing**: Comprehensive test coverage with pytest and mocked services

## Installation

Using uv (recommended):

```bash
uv add git+ssh://git@github.com/WEHI-ResearchComputing/ds-tools.git
```

## Usage

### CLI Commands

The CLI provides three main commands that work across all storage types. All commands require explicit `--storage-type` specification:

#### Analyze Storage

Calculate file/object count and total size:

```bash
# NFS filesystem
ds-tools analyze /data/path --storage-type nfs --base-path /mnt/nfs

# NFS4 filesystem
ds-tools analyze /data/path --storage-type nfs4 --base-path /mnt/nfs4

# Remote directory via SSH
ds-tools analyze /remote/path --storage-type ssh --hostname server.com --username user --ssh-key ~/.ssh/id_rsa

# S3 bucket/prefix
ds-tools analyze s3://bucket/prefix --storage-type s3 --aws-profile myprofile

# S3 with explicit credentials
ds-tools analyze s3://bucket/prefix --storage-type s3 --access-key-id KEY --secret-access-key SECRET --region us-east-1
```

#### List Storage Contents

List subdirectories/prefixes or files/objects:

```bash
# List subdirectories (default)
ds-tools list /data/path --storage-type nfs --base-path /mnt/nfs
ds-tools list /data/path --storage-type nfs4 --base-path /mnt/nfs4
ds-tools list /remote/path --storage-type ssh --hostname server.com --username user --ssh-key ~/.ssh/id_rsa
ds-tools list s3://bucket/prefix --storage-type s3 --aws-profile myprofile

# List files/objects (S3 only)
ds-tools list s3://bucket/prefix --storage-type s3 --content-type files --max-items 100 --aws-profile myprofile
```

#### Verify Access

Check storage access permissions:

```bash
# NFS filesystem (uses getfacl)
ds-tools verify-access /data/path --storage-type nfs --base-path /mnt/nfs --fs-username myuser --operation read

# NFS4 filesystem (uses nfs4_getfacl)
ds-tools verify-access /data/path --storage-type nfs4 --base-path /mnt/nfs4 --fs-username myuser --operation read

# SSH access
ds-tools verify-access /remote/path --storage-type ssh --hostname server.com --username user --ssh-key ~/.ssh/id_rsa --operation read

# S3 access
ds-tools verify-access s3://bucket/prefix --storage-type s3 --operation write --aws-profile myprofile
```

### Python API

#### Unified Interface (Recommended)

```python
from ds_tools import (
    StorageMetrics,
    analyze_storage,
    list_storage_contents,
    verify_storage_access,
    NFSStorageConfig,
    NFS4StorageConfig,
    SSHStorageConfig,
    S3StorageConfig,
)

# Create storage configurations
nfs_config = NFSStorageConfig(base_path="/mnt/nfs")
nfs4_config = NFS4StorageConfig(base_path="/mnt/nfs4")
ssh_config = SSHStorageConfig(
    hostname="server.com",
    username="user",
    ssh_key_path="/path/to/key"
)
s3_config = S3StorageConfig(aws_profile="myprofile")

# Analyze storage across different backends
metrics = analyze_storage("/data/path", nfs_config)
print(f"Files: {metrics.item_count}, Size: {metrics.total_bytes} bytes")

# List subdirectories/prefixes
subdirs = list_storage_contents("/data/path", nfs4_config, content_type="subdirectories")
objects = list_storage_contents("s3://bucket/", s3_config, content_type="files", max_items=1000)

# Verify access permissions (NFS uses getfacl, NFS4 uses nfs4_getfacl)
has_nfs_access = verify_storage_access(
    "/data/path",
    nfs_config,
    operation="read",
    username="myuser"
)

has_s3_access = verify_storage_access(
    "s3://bucket/sensitive/",
    s3_config,
    operation="read"
)
```

#### Direct Module Usage

```python
# Filesystem operations
from ds_tools.filesystem import (
    DirectoryMetrics,
    LocalDirectoryAnalyzer,
    RemoteDirectoryAnalyzer,
    calculate_directory_metrics,
)

analyzer = LocalDirectoryAnalyzer()
metrics = calculate_directory_metrics(analyzer, "/some/path")

# S3 operations
from ds_tools.objectstorage import (
    S3ClientConfig,
    S3PrefixAnalyzer,
    analyze_prefix,
)

config = S3ClientConfig(access_key_id="KEY", secret_access_key="SECRET")
analyzer = S3PrefixAnalyzer(config)
metrics = analyzer.analyze_prefix("s3://bucket/prefix")

# Or use convenience functions
metrics = analyze_prefix("s3://bucket/prefix", access_key_id="KEY", secret_access_key="SECRET")

# Configuration objects for type-safe parameter management
from ds_tools.schemas import NFSStorageConfig, NFS4StorageConfig, SSHStorageConfig, S3StorageConfig

# NFS configuration
nfs_config = NFSStorageConfig(base_path="/mnt/nfs")

# NFS4 configuration
nfs4_config = NFS4StorageConfig(base_path="/mnt/nfs4")

# SSH configuration
ssh_config = SSHStorageConfig(
    hostname="server.com",
    username="user", 
    ssh_key_path="/path/to/key"
)

# S3 configuration  
s3_config = S3StorageConfig(
    access_key_id="KEY",
    secret_access_key="SECRET",
    region_name="us-east-1"
)
```

## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/WEHI-ResearchComputing/ds-tools.git
cd ds-tools

# Install with development dependencies
uv sync --dev
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src/ds_tools --cov-report=term-missing

# Run specific test file
uv run pytest tests/test_s3_operations.py -v
```

### Code Quality

```bash
# Format code
uv run ruff format .

# Lint code  
uv run ruff check .

# Fix linting issues
uv run ruff check --fix .
```

### Configuration

The library uses environment variables for configuration. All settings are prefixed with `DS_TOOLS_`:

- `DS_TOOLS_LOG_LEVEL`: Logging level (default: INFO)
- `DS_TOOLS_OTEL_ENABLED`: Enable OpenTelemetry tracing (default: false)
- `DS_TOOLS_OTEL_SERVICE_NAME`: Service name for tracing (default: ds-tools)
- `DS_TOOLS_OTEL_EXPORTER_ENDPOINT`: OTLP exporter endpoint (default: http://localhost:4317)

### Project Structure

```
ds-tools/
├── src/
│   └── ds_tools/
│       ├── __init__.py           # Main exports and unified interface
│       ├── cli.py                # CLI interface with Typer
│       ├── cli_params.py         # Shared CLI parameter utilities
│       ├── schemas.py            # Storage configuration schemas
│       ├── core/                 # Core utilities
│       │   ├── __init__.py
│       │   ├── config.py         # Configuration management
│       │   ├── exceptions.py     # Exception hierarchy
│       │   └── observability.py # Logging and tracing
│       ├── filesystem/           # Local and remote filesystem operations
│       │   ├── __init__.py
│       │   ├── operations.py     # Consolidated filesystem operations
│       │   └── permissions/      # Access verification
│       │       ├── __init__.py
│       │       └── access_verification.py
│       ├── objectstorage/        # S3-compatible object storage
│       │   ├── __init__.py
│       │   ├── analysis/         # Prefix metrics calculation
│       │   ├── clients/          # S3 client management
│       │   ├── listing/          # Object/prefix listing
│       │   └── permissions/      # S3 access verification
│       └── unified/              # Unified storage interface
│           ├── __init__.py
│           └── storage_operations.py # Cross-storage operations
├── tests/                        # Test suite
│   ├── conftest.py              # Pytest fixtures
│   └── test_*.py                # Test modules
├── pyproject.toml               # Project configuration
└── README.md                    # This file
```

## Error Handling

The library provides clear error messages and proper exception handling:

- `ValidationError`: Input validation failures
- `CommandExecutionError`: Command execution failures
- `NotImplementedError`: For unimplemented features (SSH write verification, NFS/NFS4 write verification)

## Security Considerations

- **NFS/NFS4**: Access verification uses `getfacl` and `nfs4_getfacl` commands respectively for ACL-based permission checking
- **SSH**: Operations require key-based authentication
- **S3**: Credentials support multiple authentication methods (explicit keys, profiles, IAM roles)
- Access verification operations are non-destructive where possible
- Write access tests use safe operations (multipart upload creation/abortion for S3)

## Contributing

1. Follow the existing code style and patterns
2. Add tests for new functionality
3. Update documentation for API changes
4. Ensure all tests pass and linting is clean

## License

MIT