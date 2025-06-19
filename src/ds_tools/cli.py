"""Command-line interface for ds-tools."""

from typing import Annotated, Optional

import typer

from . import __version__
from .permission_verifier import (
    PermissionVerifierFactory,
    FilesystemType,
)
from .path import (
    path_stats,
    LocalPathStats,
    RemotePathStats,
    list_subfolders,
)

app = typer.Typer(
    name="ds-tools",
    help="CLI for datasets tools",
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


@app.command()
def verify_permissions(
    path: Annotated[str, typer.Argument(help="Path to verify permissions for")],
    user: Annotated[str, typer.Argument(help="User to check permissions for")],
    filesystem_type: Annotated[
        FilesystemType,
        typer.Option(
            "--filesystem-type",
            "-f",
            help="Filesystem type: nfs, nfs4, or s3",
            case_sensitive=False,
        ),
    ] = FilesystemType.nfs,
    access_key_id: Annotated[
        Optional[str],
        typer.Option(help="S3 access key ID (for s3 only)"),
    ] = None,
    secret_access_key: Annotated[
        Optional[str],
        typer.Option(help="S3 secret access key (for s3 only)"),
    ] = None,
    endpoint_url: Annotated[
        Optional[str],
        typer.Option(help="S3 endpoint URL (for s3 only)"),
    ] = None,
    region_name: Annotated[
        Optional[str],
        typer.Option(help="S3 region name (for s3 only)"),
    ] = None,
    session_token: Annotated[
        Optional[str],
        typer.Option(help="S3 session token (for s3 only)"),
    ] = None,
) -> None:
    """
    Verify that a user has read and execute permissions on a given path.

    For S3, provide credentials via options.
    """
    try:
        config = {}
        if filesystem_type.lower() == "s3":
            if not (access_key_id and secret_access_key):
                typer.echo(
                    "S3 requires --access-key-id and --secret-access-key", err=True
                )
                raise typer.Exit(1)
            config = {
                "access_key_id": access_key_id,
                "secret_access_key": secret_access_key,
                "endpoint_url": endpoint_url,
                "region_name": region_name or "us-east-1",
                "session_token": session_token,
            }
        verifier = PermissionVerifierFactory.create_verifier(filesystem_type, **config)
        if verifier.verify_permissions(path, user):
            typer.echo("✓ User has read and execute permissions")

    except NotImplementedError as e:
        typer.echo(f"Not implemented: {e}", err=True)
        raise typer.Exit(2)

    except (PermissionError, NotADirectoryError, ValueError) as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def local_path_stats(
    path: Annotated[str, typer.Argument(help="Local path to analyze")],
    timeout: Annotated[
        int,
        typer.Option(help="Command timeout in seconds"),
    ] = 300,
) -> None:
    """
    Count files and sum bytes in a local directory.

    Uses `find` and `awk`.
    """
    try:
        executor = LocalPathStats()
        file_count, byte_count = path_stats(executor, path, timeout)
        typer.echo(f"File count: {file_count}, Byte count: {byte_count}")

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def remote_path_stats(
    path: Annotated[str, typer.Argument(help="Remote path to analyze")],
    hostname: Annotated[str, typer.Option(help="Remote hostname")],
    username: Annotated[str, typer.Option(help="SSH username")],
    ssh_key: Annotated[str, typer.Option(help="Path to SSH private key")],
    timeout: Annotated[
        int,
        typer.Option(help="Command timeout in seconds"),
    ] = 300,
) -> None:
    """
    Count files and sum bytes in a remote directory via SSH.

    Uses `find` and `awk`.
    """
    try:
        executor = RemotePathStats(hostname, username, ssh_key)
        file_count, byte_count = path_stats(executor, path, timeout)
        typer.echo(f"File count: {file_count}, Byte count: {byte_count}")

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def list_remote_subfolders(
    path: Annotated[str, typer.Argument(help="Remote path to analyze")],
    hostname: Annotated[str, typer.Option(help="Remote hostname")],
    username: Annotated[str, typer.Option(help="SSH username")],
    ssh_key: Annotated[str, typer.Option(help="Path to SSH private key")],
    timeout: Annotated[
        int,
        typer.Option(help="Command timeout in seconds"),
    ] = 300,
) -> None:
    """
    List subfolders in a remote directory via SSH.

    Uses `find`.
    """
    try:
        subfolders = list_subfolders(path, hostname, username, ssh_key, timeout)
        typer.echo(f"Subfolders: {subfolders}")

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
