# ds-tools

Library of tools and integrations to help build dynamic user interfaces and complex Airflow workflows.

## Overview

`ds-tools` provides a unified interface for storage operations across local filesystems, remote SSH connections, and S3-compatible object storage. It's designed to be integrated with the WEHI Datasets Service backend to expose RPC-like endpoints for enhanced UI and workflow capabilities.

## Features

- **Unified Storage Operations**: Single API for local, SSH, and S3 storage
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

The CLI provides three main commands that work across all storage types:

#### Analyze Storage

Calculate file/object count and total size:

```bash
# Local directory
ds-tools analyze /local/path

# Remote directory via SSH
ds-tools analyze ssh://user@host/path --ssh-key /path/to/key

# S3 bucket/prefix
ds-tools analyze s3://bucket/prefix --access-key-id KEY --secret-access-key SECRET

# With custom endpoint (e.g., MinIO)
ds-tools analyze s3://bucket/prefix --endpoint-url http://localhost:9000
```

#### List Storage Contents

List subdirectories/prefixes or files/objects:

```bash
# List subdirectories (default)
ds-tools list /local/path
ds-tools list ssh://user@host/path --ssh-key /path/to/key
ds-tools list s3://bucket/prefix --access-key-id KEY --secret-access-key SECRET

# List files/objects
ds-tools list s3://bucket/prefix --type files --max-items 100
```

#### Verify Access

Check storage access permissions:

```bash
# Local filesystem
ds-tools verify-access /local/path --username myuser --operation read

# S3 access
ds-tools verify-access s3://bucket/prefix --operation write --access-key-id KEY
```

### Python API

#### Unified Interface (Recommended)

```python
from ds_tools import (
    StorageMetrics,
    analyze_storage,
    list_storage_contents,
    verify_storage_access,
)

# Analyze storage across different backends
metrics = analyze_storage("s3://my-bucket/data/")
print(f"Objects: {metrics.item_count}, Size: {metrics.total_bytes} bytes")

# List subdirectories/prefixes
subdirs = list_storage_contents("/local/path", content_type="subdirectories")
objects = list_storage_contents("s3://bucket/", content_type="files", max_items=1000)

# Verify access permissions
has_access = verify_storage_access(
    "s3://bucket/sensitive/",
    operation="read",
    access_key_id="KEY",
    secret_access_key="SECRET"
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

# New configuration objects for cleaner parameter management
from ds_tools.storage_config import StorageConfig, SSHConfig, S3Config

# SSH configuration
ssh_config = StorageConfig.from_ssh("hostname", "username", "/path/to/key")

# S3 configuration  
s3_config = StorageConfig.from_s3(access_key_id="KEY", secret_access_key="SECRET")
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
│       ├── storage_config.py     # Unified storage configuration objects
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
- `NotImplementedError`: For unimplemented features (SSH access verification, local write verification)

## Security Considerations

- SSH operations require key-based authentication
- S3 credentials support multiple authentication methods (explicit keys, profiles, IAM roles)
- Access verification operations are non-destructive where possible
- Write access tests use safe operations (multipart upload creation/abortion for S3)

## Contributing

1. Follow the existing code style and patterns
2. Add tests for new functionality
3. Update documentation for API changes
4. Ensure all tests pass and linting is clean

## License

MIT