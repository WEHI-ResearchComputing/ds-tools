"""Exception hierarchy for ds-tools."""


class DSToolsError(Exception):
    """Base exception for all ds-tools errors."""

    pass


class ValidationError(DSToolsError):
    """Raised when validation fails."""

    pass


class CommandExecutionError(DSToolsError):
    """Raised when command execution fails."""

    pass


class PermissionError(DSToolsError):
    """Raised when permission verification fails."""

    pass


class PathNotFoundError(DSToolsError):
    """Raised when a path is not found."""

    pass
