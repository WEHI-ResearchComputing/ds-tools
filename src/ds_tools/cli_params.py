"""Shared CLI parameter utilities to reduce duplication.

This module provides reusable parameter definitions for the CLI interface,
eliminating duplication across commands and ensuring consistent parameter
names, types, and help text throughout the application.

The parameter functions return properly typed Typer annotations that can
be used directly in command function signatures. This approach provides
several benefits:

1. Single source of truth for parameter definitions
2. Consistent help text and validation across commands
3. Easy maintenance when parameter details need to change
4. Type safety through proper annotation

Usage:
    Import parameter functions and use them in command definitions:

    @app.command()
    def my_command(
        hostname: ssh_hostname_option() = None,
        region: aws_region_option() = "us-east-1"
    ):
        pass

Parameter Categories:
    - SSH parameters: For remote filesystem operations
    - AWS parameters: For S3 operations
    - Common parameters: Shared across multiple storage types
    - Operation parameters: For specific command behaviors
"""

from typing import Annotated, Optional

import typer


def ssh_hostname_option() -> Annotated[Optional[str], typer.Option]:
    """SSH hostname option."""
    return typer.Option("--hostname", help="SSH hostname (for remote paths)")


def ssh_username_option() -> Annotated[Optional[str], typer.Option]:
    """SSH username option."""
    return typer.Option("--username", help="SSH username (for remote paths)")


def ssh_key_option() -> Annotated[Optional[str], typer.Option]:
    """SSH key option."""
    return typer.Option("--ssh-key", help="Path to SSH private key (for remote paths)")


def aws_access_key_option() -> Annotated[Optional[str], typer.Option]:
    """AWS access key ID option."""
    return typer.Option("--access-key-id", help="AWS access key ID (for S3 paths)")


def aws_secret_key_option() -> Annotated[Optional[str], typer.Option]:
    """AWS secret access key option."""
    return typer.Option(
        "--secret-access-key", help="AWS secret access key (for S3 paths)"
    )


def aws_session_token_option() -> Annotated[Optional[str], typer.Option]:
    """AWS session token option."""
    return typer.Option("--session-token", help="AWS session token (for S3 paths)")


def aws_region_option() -> Annotated[str, typer.Option]:
    """AWS region option."""
    return typer.Option("--region", help="AWS region name (for S3 paths)")


def aws_endpoint_url_option() -> Annotated[Optional[str], typer.Option]:
    """AWS endpoint URL option."""
    return typer.Option("--endpoint-url", help="Custom S3 endpoint URL")


def aws_profile_option() -> Annotated[Optional[str], typer.Option]:
    """AWS profile option."""
    return typer.Option("--aws-profile", help="AWS CLI profile name (for S3 paths)")


def timeout_option() -> Annotated[int, typer.Option]:
    """Timeout option."""
    return typer.Option("--timeout", help="Command timeout in seconds")


def max_items_option() -> Annotated[int, typer.Option]:
    """Max items option."""
    return typer.Option("--max-items", help="Maximum number of items to return")


def content_type_option() -> Annotated[str, typer.Option]:
    """Content type option."""
    return typer.Option(
        "--type", help="Content type to list: 'subdirectories' or 'files'"
    )


def operation_option() -> Annotated[str, typer.Option]:
    """Operation option."""
    return typer.Option(
        "--operation", help="Operation to test: 'read', 'write', or 'list'"
    )


def fs_username_option() -> Annotated[Optional[str], typer.Option]:
    """Username for filesystem access verification."""
    return typer.Option(
        "--username", help="Username to check access for (filesystem only)"
    )


def ssh_username_verify_option() -> Annotated[Optional[str], typer.Option]:
    """SSH username for access verification."""
    return typer.Option("--ssh-username", help="SSH username (for remote paths)")

