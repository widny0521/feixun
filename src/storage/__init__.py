"""Persistent task storage: SQLite schema, task records, events, and backups."""

from storage.database import (
    SCHEMA_VERSION,
    Database,
    DatabaseClosedError,
    SchemaVersionError,
)
from storage.task_store import (
    RECOVERY_ERROR_CODE,
    EventLogError,
    StorageError,
    TaskConflictError,
    TaskNotFoundError,
    TaskRecord,
    TaskStateError,
    TaskStore,
    UnauthorizedProjectPathError,
)

__all__ = [
    "RECOVERY_ERROR_CODE",
    "SCHEMA_VERSION",
    "Database",
    "DatabaseClosedError",
    "EventLogError",
    "SchemaVersionError",
    "StorageError",
    "TaskConflictError",
    "TaskNotFoundError",
    "TaskRecord",
    "TaskStateError",
    "TaskStore",
    "UnauthorizedProjectPathError",
]
