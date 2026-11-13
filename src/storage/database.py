"""SQLite bootstrap, schema versioning, transactional access, and backups."""

from __future__ import annotations

import os
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path

SCHEMA_VERSION = 1

_BUSY_TIMEOUT_MS = 5_000

# Migration scripts are append-only. Each script must be safe to re-run:
# a crash between the script and the version bump re-applies it on next open.
_MIGRATIONS: dict[int, str] = {
    1: """
    CREATE TABLE IF NOT EXISTS tasks (
        task_id TEXT PRIMARY KEY,
        idempotency_key TEXT NOT NULL UNIQUE,
        project_path TEXT NOT NULL,
        instruction TEXT NOT NULL,
        timeout_seconds REAL NOT NULL,
        role TEXT NOT NULL,
        status TEXT NOT NULL,
        owner TEXT,
        agent TEXT,
        provider TEXT,
        tool TEXT,
        plugin TEXT,
        result_json TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT
    );

    CREATE INDEX IF NOT EXISTS tasks_status_idx ON tasks(status);
    CREATE INDEX IF NOT EXISTS tasks_project_path_idx ON tasks(project_path);

    CREATE TABLE IF NOT EXISTS task_events (
        task_id TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        status TEXT NOT NULL,
        message TEXT NOT NULL DEFAULT '',
        data_json TEXT NOT NULL DEFAULT '{}',
        timestamp TEXT NOT NULL,
        PRIMARY KEY (task_id, sequence),
        FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
    );
    """,
}


class SchemaVersionError(RuntimeError):
    """Raised when a database schema is newer than this code supports."""


class DatabaseClosedError(RuntimeError):
    """Raised when a closed database is used."""


class Database:
    """Owns one SQLite connection, its schema version, and backup operations.

    The connection is shared across threads, so every access is serialized with
    an internal re-entrant lock. Writes use ``BEGIN IMMEDIATE`` so competing
    writers observe a consistent snapshot before mutating task state.
    """

    def __init__(self, path: str | Path) -> None:
        raw_path = os.fspath(path)
        self._memory = raw_path == ":memory:"
        if self._memory:
            self.path: Path | None = None
            target = ":memory:"
        else:
            self.path = Path(raw_path).expanduser().resolve()
            if self.path.exists() and self.path.is_dir():
                raise IsADirectoryError(f"database path is a directory: {self.path}")
            self.path.parent.mkdir(parents=True, exist_ok=True)
            target = str(self.path)

        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            target,
            timeout=_BUSY_TIMEOUT_MS / 1000,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
        self._connection.execute("PRAGMA journal_mode = WAL")
        try:
            self._migrate()
        except BaseException:
            self._connection.close()
            raise

    @property
    def schema_version(self) -> int:
        """Current schema version recorded in the SQLite header."""

        with self._lock:
            self._ensure_open()
            return int(self._connection.execute("PRAGMA user_version").fetchone()[0])

    @property
    def lock(self) -> threading.RLock:
        """Lock guarding the raw connection for multi-statement reads."""

        return self._lock

    @property
    def connection(self) -> sqlite3.Connection:
        """Raw connection; callers must hold :attr:`lock` while using it."""

        return self._connection

    @contextmanager
    def reader(self) -> Iterator[sqlite3.Connection]:
        """Serialize reads against local writers without starting a transaction."""

        with self._lock:
            self._ensure_open()
            yield self._connection

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Run a write transaction, rolling back on any exception."""

        with self._lock:
            self._ensure_open()
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise
            try:
                self._connection.execute("COMMIT")
            except BaseException:
                with suppress(sqlite3.Error):
                    self._connection.execute("ROLLBACK")
                raise

    def backup(self, destination: str | Path) -> Path:
        """Copy the live database to a new file without overwriting anything.

        The destination must not exist. A partially written backup is removed
        if the copy or integrity check fails.
        """

        target_path = Path(destination).expanduser().resolve()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            self._ensure_open()
            descriptor = os.open(
                target_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            os.close(descriptor)
            target: sqlite3.Connection | None = None
            try:
                target = sqlite3.connect(str(target_path), isolation_level=None)
                self._connection.backup(target)
                check = target.execute("PRAGMA integrity_check").fetchone()[0]
                if check != "ok":
                    raise RuntimeError(f"backup integrity check failed: {check}")
            except BaseException:
                if target is not None:
                    target.close()
                target_path.unlink(missing_ok=True)
                raise
            else:
                target.close()
        return target_path

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._connection.close()
            self._closed = True

    def __enter__(self) -> Database:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise DatabaseClosedError("database is closed")

    def _migrate(self) -> None:
        with self._lock:
            self._ensure_open()
            version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
            if version > SCHEMA_VERSION:
                raise SchemaVersionError(
                    f"database schema version {version} is newer than supported version "
                    f"{SCHEMA_VERSION}: {self.path or ':memory:'}"
                )
            while version < SCHEMA_VERSION:
                next_version = version + 1
                script = _MIGRATIONS.get(next_version)
                if script is None:
                    raise SchemaVersionError(f"missing migration for schema version {next_version}")
                self._connection.executescript(script)
                self._connection.execute(f"PRAGMA user_version = {next_version}")
                version = next_version
