class StateSyncError(Exception):
    """Base exception for StateSync operations."""

class ProviderValidationError(StateSyncError):
    """Raised when a snapshot provider returns invalid or unsafe data."""

class VersionMismatchError(StateSyncError):
    """Raised when client version is ahead of server (should not happen)."""

class SchemaMismatchError(StateSyncError):
    """Raised when schema versions differ."""