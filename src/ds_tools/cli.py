"""Command-line interface for ds-tools.

This module provides a unified CLI for storage operations across different backends.
The CLI automatically detects storage type based on path format and routes operations
to the appropriate backend implementation.

Storage Type Detection:
    - Local paths: /path/to/directory
    - SSH paths: ssh://user@host/path or user@host:/path
    - S3 paths: s3://bucket/prefix

Commands:
    - analyze: Calculate storage metrics (file count, total size)
    - list: List storage contents (subdirectories/prefixes or files/objects)
    - verify-access: Verify storage access permissions

All commands support the same parameter interface but only use relevant parameters
for each storage type (e.g., SSH parameters are ignored for S3 operations).
"""

from typing import Annotated, Optional

import typer

from . import __version__
from .cli_params import (
    aws_access_key_option,
    aws_endpoint_url_option,
    aws_profile_option,
    aws_region_option,
    aws_secret_key_option,
    aws_session_token_option,
    content_type_option,
    fs_username_option,
    max_items_option,
    operation_option,
    ssh_hostname_option,
    ssh_key_option,
    ssh_username_option,
    ssh_username_verify_option,
    timeout_option,
)
from .unified import (
    analyze_storage,
    list_storage_contents,
    verify_storage_access,
)

app = typer.Typer(
    name="ds-tools",
    help="CLI for unified storage operations (local, SSH, S3)",
    add_completion=False,
)


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        typer.echo(f"ds-tools {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        Optional[bool],
        typer.Option(
            "--version",
            "-v",
            callback=version_callback,
            is_eager=True,
            help="Show version and exit",
        ),
    ] = None,
) -> None:
    """Main callback for global options."""
    pass


@app.command("analyze")
def analyze_cmd(
    path: Annotated[
        str,
        typer.Argument(
            help="Storage path (local, ssh://user@host/path, or s3://bucket/prefix)"
        ),
    ],
    # SSH options
    hostname: ssh_hostname_option() = None,
    username: ssh_username_option() = None,
    ssh_key: ssh_key_option() = None,
    # S3 options
    access_key_id: aws_access_key_option() = None,
    secret_access_key: aws_secret_key_option() = None,
    session_token: aws_session_token_option() = None,
    region_name: aws_region_option() = "us-east-1",
    endpoint_url: aws_endpoint_url_option() = None,
    aws_profile: aws_profile_option() = None,
    # Common options
    timeout: timeout_option() = 300,
) -> None:
    """
    Analyze storage to get item count and total size.

    Supports local paths, SSH (ssh://user@host/path), and S3 (s3://bucket/prefix).
    Storage type is auto-detected from path format.
    """
    try:
        metrics = analyze_storage(
            path=path,
            hostname=hostname,
            username=username,
            ssh_key=ssh_key,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_profile=aws_profile,
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
    content_type: content_type_option() = "subdirectories",
    # SSH options
    hostname: ssh_hostname_option() = None,
    username: ssh_username_option() = None,
    ssh_key: ssh_key_option() = None,
    # S3 options
    access_key_id: aws_access_key_option() = None,
    secret_access_key: aws_secret_key_option() = None,
    session_token: aws_session_token_option() = None,
    region_name: aws_region_option() = "us-east-1",
    endpoint_url: aws_endpoint_url_option() = None,
    aws_profile: aws_profile_option() = None,
    # Common options
    timeout: timeout_option() = 300,
    max_items: max_items_option() = 1000,
) -> None:
    """
    List storage contents (subdirectories/prefixes or files/objects).

    Supports local paths, SSH (ssh://user@host/path), and S3 (s3://bucket/prefix).
    """
    try:
        items = list_storage_contents(
            path=path,
            content_type=content_type,
            hostname=hostname,
            username=username,
            ssh_key=ssh_key,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_profile=aws_profile,
            timeout=timeout,
            max_items=max_items,
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
    operation: operation_option() = "read",
    username: fs_username_option() = None,
    # SSH options
    hostname: ssh_hostname_option() = None,
    ssh_username: ssh_username_verify_option() = None,
    ssh_key: ssh_key_option() = None,
    # S3 options
    access_key_id: aws_access_key_option() = None,
    secret_access_key: aws_secret_key_option() = None,
    session_token: aws_session_token_option() = None,
    region_name: aws_region_option() = "us-east-1",
    endpoint_url: aws_endpoint_url_option() = None,
    aws_profile: aws_profile_option() = None,
) -> None:
    """
    Verify access to storage location.

    Supports local paths, SSH (ssh://user@host/path), and S3 (s3://bucket/prefix).
    """
    try:
        has_access = verify_storage_access(
            path=path,
            username=username,
            operation=operation,
            hostname=hostname,
            ssh_username=ssh_username,
            ssh_key=ssh_key,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_profile=aws_profile,
        )

        if has_access:
            typer.echo(f"✓ {operation.title()} access verified for: {path}")
        else:
            typer.echo(f"✗ {operation.title()} access denied for: {path}")
            raise typer.Exit(1)

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
