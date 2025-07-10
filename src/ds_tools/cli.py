"""Command-line interface for ds-tools.

This module provides a unified CLI for storage operations across different backends.

Commands:
    - analyze: Calculate storage metrics (file count, total size)
    - list: List storage contents (subdirectories/prefixes or files/objects)
    - verify-access: Verify storage access permissions

Storage type must be explicitly specified using --storage-type flag.
Only relevant parameters for each storage type are used.
"""

from typing import Annotated, Literal, Optional

import typer

from . import __version__
from .schemas import (
    NFS4StorageConfig,
    NFSStorageConfig,
    S3StorageConfig,
    SSHStorageConfig,
)
from .unified import (
    analyze_storage,
    list_storage_contents,
    verify_storage_access,
)

app = typer.Typer(
    name="ds-tools",
    help="Tools and integrations for dynamic UIs and complex workflows.",
    no_args_is_help=True,
)


def version_callback(value: bool) -> None:
    """Display version information."""
    if value:
        typer.echo(f"ds-tools {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        Optional[bool],
        typer.Option("--version", callback=version_callback, help="Show version."),
    ] = None,
) -> None:
    """
    DS-Tools: Unified storage operations for local, SSH, and S3.

    Storage type must be explicitly specified using --storage-type flag.
    """
    pass


StorageTypeOption = Annotated[
    Literal["nfs", "nfs4", "ssh", "s3"],
    typer.Option(
        "--storage-type",
        "-t",
        help="Storage type: nfs, nfs4, ssh, or s3",
        case_sensitive=False,
    ),
]


def _create_storage_config(
    storage_type: Literal["nfs", "nfs4", "ssh", "s3"],
    hostname: Optional[str] = None,
    username: Optional[str] = None,
    ssh_key: Optional[str] = None,
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    session_token: Optional[str] = None,
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    aws_profile: Optional[str] = None,
    base_path: Optional[str] = None,
):
    """Create appropriate storage configuration based on storage type."""
    if storage_type == "ssh":
        if not all([hostname, username, ssh_key]):
            raise ValueError(
                "SSH storage requires --hostname, --username, and --ssh-key"
            )

        assert hostname is not None
        assert username is not None
        assert ssh_key is not None

        return SSHStorageConfig(
            hostname=hostname,
            username=username,
            ssh_key_path=ssh_key,
        )

    elif storage_type == "s3":
        return S3StorageConfig(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_profile=aws_profile,
        )

    elif storage_type == "nfs":
        return NFSStorageConfig(base_path=base_path)

    elif storage_type == "nfs4":
        return NFS4StorageConfig(base_path=base_path)

    else:
        raise ValueError(
            f"Invalid storage type: {storage_type}. Must be 'nfs', 'nfs4', 'ssh', "
            f"or 's3'"
        )


@app.command("analyze")
def analyze_cmd(
    path: Annotated[str, typer.Argument(help="Storage path to analyze")],
    storage_type: StorageTypeOption,
    # SSH options
    hostname: Annotated[
        Optional[str],
        typer.Option("--hostname", help="SSH hostname (for remote paths)"),
    ] = None,
    username: Annotated[
        Optional[str],
        typer.Option("--username", help="SSH username (for remote paths)"),
    ] = None,
    ssh_key: Annotated[
        Optional[str],
        typer.Option("--ssh-key", help="Path to SSH private key (for remote paths)"),
    ] = None,
    # S3 options
    access_key_id: Annotated[
        Optional[str],
        typer.Option("--access-key-id", help="AWS access key ID (for S3 paths)"),
    ] = None,
    secret_access_key: Annotated[
        Optional[str],
        typer.Option(
            "--secret-access-key", help="AWS secret access key (for S3 paths)"
        ),
    ] = None,
    session_token: Annotated[
        Optional[str],
        typer.Option("--session-token", help="AWS session token (for S3 paths)"),
    ] = None,
    region_name: Annotated[
        str, typer.Option("--region", help="AWS region name (for S3 paths)")
    ] = "us-east-1",
    endpoint_url: Annotated[
        Optional[str], typer.Option("--endpoint-url", help="Custom S3 endpoint URL")
    ] = None,
    aws_profile: Annotated[
        Optional[str],
        typer.Option("--aws-profile", help="AWS CLI profile name (for S3 paths)"),
    ] = None,
    # NFS options
    base_path: Annotated[
        Optional[str], typer.Option("--base-path", help="Base path for NFS storage")
    ] = None,
    # Common options
    timeout: Annotated[
        int, typer.Option("--timeout", help="Command timeout in seconds")
    ] = 300,
) -> None:
    """
    Analyze storage to get item count and total size.

    Examples:
        NFS: ds-tools analyze /data/path --storage-type nfs --base-path /mnt/nfs
        NFS4: ds-tools analyze /data/path --storage-type nfs4 --base-path /mnt/nfs4
        SSH: ds-tools analyze /remote/path --storage-type ssh --hostname server.com \
             --username user --ssh-key ~/.ssh/id_rsa
        S3: ds-tools analyze s3://bucket/prefix --storage-type s3 \
            --aws-profile myprofile
    """
    try:
        config = _create_storage_config(
            storage_type=storage_type,
            hostname=hostname,
            username=username,
            ssh_key=ssh_key,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_profile=aws_profile,
            base_path=base_path,
        )

        metrics = analyze_storage(
            path=path,
            config=config,
            timeout=timeout,
        )

        typer.echo(f"Storage: {metrics.location}")
        typer.echo(f"Type: {metrics.storage_type}")
        typer.echo(f"Items: {metrics.item_count:,}")
        typer.echo(f"Total size: {metrics.total_bytes:,} bytes")

        # Human-readable size
        if metrics.total_bytes >= 1024**3:
            size_str = f"{metrics.total_bytes / (1024**3):.2f} GB"
        elif metrics.total_bytes >= 1024**2:
            size_str = f"{metrics.total_bytes / (1024**2):.2f} MB"
        elif metrics.total_bytes >= 1024:
            size_str = f"{metrics.total_bytes / 1024:.2f} KB"
        else:
            size_str = f"{metrics.total_bytes} bytes"
        typer.echo(f"Human readable: {size_str}")

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command("list")
def list_cmd(
    path: Annotated[str, typer.Argument(help="Storage path to list")],
    storage_type: StorageTypeOption,
    content_type: Annotated[
        str,
        typer.Option(
            "--type", help="Content type to list: 'subdirectories' or 'files'"
        ),
    ] = "subdirectories",
    # SSH options
    hostname: Annotated[
        Optional[str],
        typer.Option("--hostname", help="SSH hostname (for remote paths)"),
    ] = None,
    username: Annotated[
        Optional[str],
        typer.Option("--username", help="SSH username (for remote paths)"),
    ] = None,
    ssh_key: Annotated[
        Optional[str],
        typer.Option("--ssh-key", help="Path to SSH private key (for remote paths)"),
    ] = None,
    # S3 options
    access_key_id: Annotated[
        Optional[str],
        typer.Option("--access-key-id", help="AWS access key ID (for S3 paths)"),
    ] = None,
    secret_access_key: Annotated[
        Optional[str],
        typer.Option(
            "--secret-access-key", help="AWS secret access key (for S3 paths)"
        ),
    ] = None,
    session_token: Annotated[
        Optional[str],
        typer.Option("--session-token", help="AWS session token (for S3 paths)"),
    ] = None,
    region_name: Annotated[
        str, typer.Option("--region", help="AWS region name (for S3 paths)")
    ] = "us-east-1",
    endpoint_url: Annotated[
        Optional[str], typer.Option("--endpoint-url", help="Custom S3 endpoint URL")
    ] = None,
    aws_profile: Annotated[
        Optional[str],
        typer.Option("--aws-profile", help="AWS CLI profile name (for S3 paths)"),
    ] = None,
    # NFS options
    base_path: Annotated[
        Optional[str], typer.Option("--base-path", help="Base path for NFS storage")
    ] = None,
    # Common options
    timeout: Annotated[
        int, typer.Option("--timeout", help="Command timeout in seconds")
    ] = 300,
    max_items: Annotated[
        int, typer.Option("--max-items", help="Maximum number of items to return")
    ] = 1000,
) -> None:
    """
    List storage contents (subdirectories/prefixes or files/objects).

    Examples:
        NFS: ds-tools list /data/path --storage-type nfs --base-path /mnt/nfs
        NFS4: ds-tools list /data/path --storage-type nfs4 --base-path /mnt/nfs4
        SSH: ds-tools list /remote/path --storage-type ssh --hostname server.com \
             --username user --ssh-key ~/.ssh/id_rsa
        S3: ds-tools list s3://bucket/prefix --storage-type s3 --aws-profile myprofile
    """
    try:
        config = _create_storage_config(
            storage_type=storage_type,
            hostname=hostname,
            username=username,
            ssh_key=ssh_key,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_profile=aws_profile,
            base_path=base_path,
        )

        items = list_storage_contents(
            path=path,
            config=config,
            content_type=content_type,
            max_items=max_items,
            timeout=timeout,
        )

        if items:
            typer.echo(f"Found {len(items)} {content_type}:")
            for item in items:
                typer.echo(f"  {item}")
        else:
            typer.echo(f"No {content_type} found.")

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command("verify-access")
def verify_access_cmd(
    path: Annotated[str, typer.Argument(help="Storage path to verify access for")],
    storage_type: StorageTypeOption,
    operation: Annotated[
        str,
        typer.Option(
            "--operation", help="Operation to test: 'read', 'write', or 'list'"
        ),
    ] = "read",
    # SSH options
    hostname: Annotated[
        Optional[str],
        typer.Option("--hostname", help="SSH hostname (for remote paths)"),
    ] = None,
    username: Annotated[
        Optional[str],
        typer.Option("--username", help="SSH username (for remote paths)"),
    ] = None,
    ssh_key: Annotated[
        Optional[str],
        typer.Option("--ssh-key", help="Path to SSH private key (for remote paths)"),
    ] = None,
    # S3 options
    access_key_id: Annotated[
        Optional[str],
        typer.Option("--access-key-id", help="AWS access key ID (for S3 paths)"),
    ] = None,
    secret_access_key: Annotated[
        Optional[str],
        typer.Option(
            "--secret-access-key", help="AWS secret access key (for S3 paths)"
        ),
    ] = None,
    session_token: Annotated[
        Optional[str],
        typer.Option("--session-token", help="AWS session token (for S3 paths)"),
    ] = None,
    region_name: Annotated[
        str, typer.Option("--region", help="AWS region name (for S3 paths)")
    ] = "us-east-1",
    endpoint_url: Annotated[
        Optional[str], typer.Option("--endpoint-url", help="Custom S3 endpoint URL")
    ] = None,
    aws_profile: Annotated[
        Optional[str],
        typer.Option("--aws-profile", help="AWS CLI profile name (for S3 paths)"),
    ] = None,
    # Filesystem options
    fs_username: Annotated[
        Optional[str],
        typer.Option(
            "--fs-username", help="Username to check access for (filesystem only)"
        ),
    ] = None,
    # NFS options
    base_path: Annotated[
        Optional[str], typer.Option("--base-path", help="Base path for NFS storage")
    ] = None,
    # Common options
    timeout: Annotated[
        int, typer.Option("--timeout", help="Command timeout in seconds")
    ] = 300,
) -> None:
    """
    Verify access to storage location.

    Tests read, write, or list permissions for the specified path.
    Examples:
        NFS: ds-tools verify-access /data/path --storage-type nfs \
             --base-path /mnt/nfs --fs-username user
        NFS4: ds-tools verify-access /data/path --storage-type nfs4 \
              --base-path /mnt/nfs4 --fs-username user
        SSH: ds-tools verify-access /remote/path --storage-type ssh \
             --hostname server.com --username user --ssh-key ~/.ssh/id_rsa
        S3: ds-tools verify-access s3://bucket/prefix --storage-type s3 \
            --aws-profile myprofile
    """
    try:
        config = _create_storage_config(
            storage_type=storage_type,
            hostname=hostname,
            username=username,
            ssh_key=ssh_key,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_profile=aws_profile,
            base_path=base_path,
        )

        has_access = verify_storage_access(
            path=path,
            config=config,
            operation=operation,
            timeout=timeout,
            username=fs_username,  # For local filesystem
        )

        if has_access:
            typer.echo(f"✓ Access verified: {operation} permission granted for {path}")
        else:
            typer.echo(
                f"✗ Access denied: {operation} permission denied for {path}", err=True
            )
            raise typer.Exit(1)

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
