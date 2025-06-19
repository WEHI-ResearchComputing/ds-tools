# ds-tools

Datasets operations library and CLI tools.

## Installation

Using uv (recommended):

```bash
uv add git+ssh://git@github.com/WEHI-ResearchComputing/ds-tools.git
```

## Usage

### CLI Commands

- **Verify permissions for a user on a path:**

  ```bash
  # For NFS:
  ds-tools verify-permissions /some/path username --filesystem-type nfs

  # For NFS4:
  ds-tools verify-permissions /some/path username --filesystem-type nfs4

  # For S3:
  ds-tools verify-permissions s3://bucket/path username --filesystem-type s3 \
    --access-key-id <KEY> --secret-access-key <SECRET>
  ```

- **Get file and byte count for a local directory:**

  ```bash
  ds-tools local-path-stats /some/local/path
  ```

- **Get file and byte count for a remote directory via SSH:**

  ```bash
  ds-tools remote-path-stats /some/remote/path --hostname host --username user --ssh-key /path/to/key
  ```

- **Get subfolders for a remote directory via SSH:**
  ```bash
  ds-tools list-remote-subfolders /some/remote/path --hostname host --username user --ssh-key /path/to/key
  ```

### Python API

```python
from ds_tools import (
    path_stats, LocalCommandExecutor, RemoteCommandExecutor,
    PermissionVerifierFactory, FilesystemType, list_subfolders
)

# Permission verification (NFS)
nfs_verifier = PermissionVerifierFactory.create_verifier(FilesystemType.nfs)
nfs_verifier.verify_permissions("/some/path", "username")

# Permission verification (NFS4)
nfs4_verifier = PermissionVerifierFactory.create_verifier(FilesystemType.nfs4)
nfs4_verifier.verify_permissions("/some/path", "username")

# Permission verification (S3)
s3_verifier = PermissionVerifierFactory.create_verifier(
    FilesystemType.s3,
    access_key_id="<KEY>",
    secret_access_key="<SECRET>"
)
s3_verifier.verify_permissions("s3://bucket/path", "username")

# Local file stats
local_executor = LocalPathStats()
file_count, byte_count = path_stats(executor, "/some/path")

# Remote file stats
remote_executor = RemotePathStats("host", "user", "/path/to/key")
file_count, byte_count = path_stats(remote_executor, "/remote/path")

# Remote subfolders
list_subfolders("/some/path", "host", "user", "/path/to/key")
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
uv run pytest
```

### Code Quality

```bash
# Format code
uv run ruff format

# Lint code
uv run ruff check

# Fix linting issues
uv run ruff check --fix
```

## License

MIT
