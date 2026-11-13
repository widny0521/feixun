"""Idempotent SQLite task persistence with event log and project boundaries.

The store is deliberately independent of the runtime so API and agent layers
share one durable state machine. Project roots are enforced here, before any
request is written, and every mutation runs in a single SQLite transaction.
"""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from agent_protocol import TaskEvent, TaskRequest, TaskResult, TaskStatus, ensure_transition
from storage.database import Database, DatabaseClosedError

RECOVERY_ERROR_CODE = "interrupted"

_MAX_EXTENSION_CHARS = 128


class StorageError(Exception):
    """Base class for storage failures."""


class TaskNotFoundError(StorageError):
    """Raised when a task id is unknown to the store."""


class TaskConflictError(StorageError):
    """Raised when a request or result contradicts stored task state."""


class TaskStateError(StorageError):
    """Raised when a mutation is illegal for the task's current state."""


class EventLogError(StorageError):
    """Raised when an event breaks sequence or identity invariants."""


class UnauthorizedProjectPathError(StorageError):
    """Raised when a project path lies outside every authorized root."""


@dataclass(frozen=True, slots=True)
class TaskRecord:
    """Durable task state loaded from the database."""

    task_id: str
    idempotency_key: str
    project_path: Path
    instruction: str
    timeout_seconds: float
    role: str
    status: TaskStatus
    owner: str | None = None
    agent: str | None = None
    provider: str | None = None
    tool: str | None = None
    plugin: str | None = None
    result: TaskResult | None = None
    created_at: datetime = datetime.min.replace(tzinfo=UTC)
    updated_at: datetime = datetime.min.replace(tzinfo=UTC)
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @property
    def is_terminal(self) -> bool:
        return self.status.is_terminal


class TaskStore:
    """Persist tasks, events, terminal results, and backups.

    ``allowed_roots`` is mandatory: a request whose resolved project path is not
    inside one of the roots is rejected before any row is written.
    """

    def __init__(
        self,
        database: Database | str | Path,
        allowed_roots: Iterable[Path | str],
    ) -> None:
        roots = tuple(Path(root).expanduser().resolve() for root in allowed_roots)
        if not roots:
            raise ValueError("allowed_roots must not be empty")
        self._allowed_roots = roots
        self._owns_database = not isinstance(database, Database)
        self._database = Database(database) if self._owns_database else database
        self._closed = False

    @property
    def database(self) -> Database:
        return self._database

    @property
    def allowed_roots(self) -> tuple[Path, ...]:
        return self._allowed_roots

    @property
    def schema_version(self) -> int:
        return self._database.schema_version

    def create_or_get(
        self,
        request: TaskRequest | Mapping[str, Any],
        *,
        owner: str | None = None,
        agent: str | None = None,
        provider: str | None = None,
        tool: str | None = None,
        plugin: str | None = None,
    ) -> TaskRecord:
        """Insert a new queued task or return the existing equivalent one.

        An existing idempotency key wins over the request's task id, matching the
        runtime's duplicate handling. A reused task id with a different key also
        returns the stored record; neither path writes a second row. Extension
        fields are stored as-is and never updated by later duplicates.
        """

        self._ensure_open()
        req = self._coerce_request(request)
        project_path = req.project_path.expanduser().resolve()
        if not self._is_authorized(project_path):
            raise UnauthorizedProjectPathError(
                f"project path is outside the allowed roots: {project_path}"
            )

        extensions = {
            "owner": owner,
            "agent": agent,
            "provider": provider,
            "tool": tool,
            "plugin": plugin,
        }
        for name, value in extensions.items():
            self._validate_extension(name, value)

        now = _dump_datetime(_utc_now())
        with self._database.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM tasks WHERE idempotency_key = ?",
                (req.idempotency_key,),
            ).fetchone()
            if row is None:
                row = conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?",
                    (req.task_id,),
                ).fetchone()
            if row is not None:
                return _to_record(row)

            conn.execute(
                """
                INSERT INTO tasks (
                    task_id, idempotency_key, project_path, instruction,
                    timeout_seconds, role, status, owner, agent, provider, tool,
                    plugin, result_json, created_at, updated_at, started_at, finished_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, NULL, NULL)
                """,
                (
                    req.task_id,
                    req.idempotency_key,
                    str(project_path),
                    req.instruction,
                    req.timeout_seconds,
                    req.role,
                    TaskStatus.QUEUED.value,
                    owner,
                    agent,
                    provider,
                    tool,
                    plugin,
                    now,
                    now,
                ),
            )
            inserted = conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?",
                (req.task_id,),
            ).fetchone()
            if inserted is None:  # pragma: no cover - database invariant
                raise StorageError(f"task insert vanished: {req.task_id}")
            return _to_record(inserted)

    def append_event(
        self,
        task_id: str,
        event: TaskEvent | Mapping[str, Any],
    ) -> None:
        """Append a lifecycle event and advance the task's status.

        Sequences must be contiguous from zero, events must name the same task,
        and the event status must be a legal transition from the stored status.
        """

        self._ensure_open()
        if not isinstance(event, TaskEvent):
            event = TaskEvent.model_validate(event)
        if event.task_id != task_id:
            raise EventLogError(
                f"event task id {event.task_id!r} does not match {task_id!r}"
            )

        with self._database.transaction() as conn:
            row = self._require_row(conn, task_id)
            expected = self._next_sequence(conn, task_id)
            if event.sequence != expected:
                raise EventLogError(
                    f"event sequence {event.sequence} is not contiguous; expected {expected}"
                )
            self._apply_event_status(conn, row, event)

    def complete(self, task_id: str, result: TaskResult) -> None:
        """Attach a terminal result, appending a terminal event when missing.

        Repeating the identical completion is a no-op; conflicting results or
        statuses raise :class:`TaskConflictError` / :class:`TaskStateError`.
        """

        self._ensure_open()
        if not isinstance(result, TaskResult):
            result = TaskResult.model_validate(result)
        if result.task_id != task_id:
            raise TaskConflictError(
                f"result task id {result.task_id!r} does not match {task_id!r}"
            )

        with self._database.transaction() as conn:
            self._complete_locked(conn, task_id, result)

    def load(self, task_id: str) -> TaskRecord | None:
        """Return the stored record or ``None`` when the task is unknown."""

        self._ensure_open()
        with self._database.reader() as conn:
            row = conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?",
                (task_id,),
            ).fetchone()
        return None if row is None else _to_record(row)

    def list_tasks(self, status: TaskStatus | str | None = None) -> tuple[TaskRecord, ...]:
        """Return records ordered by creation time, optionally filtered by status."""

        self._ensure_open()
        with self._database.reader() as conn:
            if status is None:
                rows = conn.execute(
                    "SELECT * FROM tasks ORDER BY created_at, task_id"
                ).fetchall()
            else:
                target = status if isinstance(status, TaskStatus) else TaskStatus(status)
                rows = conn.execute(
                    "SELECT * FROM tasks WHERE status = ? ORDER BY created_at, task_id",
                    (target.value,),
                ).fetchall()
        return tuple(_to_record(row) for row in rows)

    def events(self, task_id: str) -> tuple[TaskEvent, ...]:
        """Return the task's complete event log in sequence order."""

        self._ensure_open()
        with self._database.reader() as conn:
            self._require_row(conn, task_id)
            rows = conn.execute(
                "SELECT * FROM task_events WHERE task_id = ? ORDER BY sequence",
                (task_id,),
            ).fetchall()
        return tuple(_to_event(row) for row in rows)

    def recover_interrupted_tasks(self) -> tuple[TaskRecord, ...]:
        """Mark queued/running tasks failed after a runtime restart.

        Each recovered task receives a failed result and a terminal event. The
        operation is idempotent: a second call returns an empty tuple.
        """

        self._ensure_open()
        recovered: list[TaskRecord] = []
        with self._database.transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE status IN (?, ?) ORDER BY created_at, task_id",
                (TaskStatus.QUEUED.value, TaskStatus.RUNNING.value),
            ).fetchall()
            now = _utc_now()
            for row in rows:
                task_id = row["task_id"]
                previous = TaskStatus(row["status"])
                ensure_transition(previous, TaskStatus.FAILED)
                event = TaskEvent(
                    task_id=task_id,
                    sequence=self._next_sequence(conn, task_id),
                    status=TaskStatus.FAILED,
                    message="task interrupted by runtime restart",
                    timestamp=now,
                    data={"recovered": True, "previous_status": previous.value},
                )
                self._apply_event_status(conn, row, event)
                result = TaskResult(
                    task_id=task_id,
                    status=TaskStatus.FAILED,
                    exit_code=None,
                    stdout="",
                    stderr="task interrupted by runtime restart",
                    error_code=RECOVERY_ERROR_CODE,
                    started_at=_load_datetime(row["started_at"]),
                    finished_at=now,
                )
                self._complete_locked(conn, task_id, result)
                recovered_row = conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?",
                    (task_id,),
                ).fetchone()
                if recovered_row is None:  # pragma: no cover - database invariant
                    raise StorageError(f"recovered task vanished: {task_id}")
                recovered.append(_to_record(recovered_row))
        return tuple(recovered)

    def backup(self, destination: str | Path) -> Path:
        """Copy all task state to a new file; an existing file is never replaced."""

        self._ensure_open()
        return self._database.backup(destination)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_database:
            self._database.close()

    def __enter__(self) -> TaskStore:
        return self

    def __exit__(self, *exc_info: object) -> None:
        if not self._closed:
            self.close()

    def _complete_locked(
        self,
        conn: sqlite3.Connection,
        task_id: str,
        result: TaskResult,
    ) -> None:
        row = self._require_row(conn, task_id)
        current = TaskStatus(row["status"])

        if current.is_terminal and current is not result.status:
            raise TaskStateError(
                f"task {task_id!r} is {current.value}, cannot complete as {result.status.value}"
            )

        if row["result_json"] is not None:
            existing = TaskResult.model_validate_json(row["result_json"])
            if existing == result:
                return
            raise TaskConflictError(f"task {task_id!r} already has a different result")

        finished_at = result.finished_at or _utc_now()
        if not current.is_terminal:
            ensure_transition(current, result.status)
            event = TaskEvent(
                task_id=task_id,
                sequence=self._next_sequence(conn, task_id),
                status=result.status,
                message=result.error_code or "completed",
                timestamp=finished_at,
                data={"exit_code": result.exit_code},
            )
            self._apply_event_status(conn, row, event)

        finished = _dump_datetime(finished_at)
        started = _dump_datetime(result.started_at) or row["started_at"] or finished
        conn.execute(
            """
            UPDATE tasks
            SET status = ?, result_json = ?,
                started_at = COALESCE(started_at, ?),
                finished_at = ?, updated_at = ?
            WHERE task_id = ?
            """,
            (
                result.status.value,
                result.model_dump_json(),
                started,
                finished,
                finished,
                task_id,
            ),
        )

    def _apply_event_status(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        event: TaskEvent,
    ) -> None:
        current = TaskStatus(row["status"])
        ensure_transition(current, event.status)
        timestamp = _dump_datetime(event.timestamp)

        assignments = ["status = ?", "updated_at = ?"]
        parameters: list[Any] = [event.status.value, timestamp]
        if row["started_at"] is None:
            assignments.append("started_at = ?")
            parameters.append(timestamp)
        if event.status.is_terminal and row["finished_at"] is None:
            assignments.append("finished_at = ?")
            parameters.append(timestamp)

        conn.execute(
            f"UPDATE tasks SET {', '.join(assignments)} WHERE task_id = ?",
            (*parameters, row["task_id"]),
        )
        conn.execute(
            """
            INSERT INTO task_events (task_id, sequence, status, message, data_json, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event.task_id,
                event.sequence,
                event.status.value,
                event.message,
                json.dumps(dict(event.data), ensure_ascii=False),
                timestamp,
            ),
        )

    @staticmethod
    def _next_sequence(conn: sqlite3.Connection, task_id: str) -> int:
        row = conn.execute(
            "SELECT sequence FROM task_events WHERE task_id = ? ORDER BY sequence DESC LIMIT 1",
            (task_id,),
        ).fetchone()
        return 0 if row is None else int(row["sequence"]) + 1

    @staticmethod
    def _require_row(conn: sqlite3.Connection, task_id: str) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM tasks WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        if row is None:
            raise TaskNotFoundError(f"unknown task: {task_id!r}")
        return row

    def _is_authorized(self, project_path: Path) -> bool:
        candidate = os.path.normcase(os.path.abspath(os.fspath(project_path)))
        for root in self._allowed_roots:
            root_key = os.path.normcase(os.path.abspath(os.fspath(root)))
            try:
                if os.path.commonpath([candidate, root_key]) == root_key:
                    return True
            except ValueError:
                continue
        return False

    @staticmethod
    def _coerce_request(request: TaskRequest | Mapping[str, Any]) -> TaskRequest:
        if isinstance(request, TaskRequest):
            return request
        if isinstance(request, Mapping):
            return TaskRequest.model_validate(request)
        raise TypeError("request must be a TaskRequest or a mapping")

    @staticmethod
    def _validate_extension(name: str, value: str | None) -> None:
        if value is None:
            return
        if not isinstance(value, str):
            raise TypeError(f"{name} must be a string or None")
        if not value or len(value) > _MAX_EXTENSION_CHARS or "\x00" in value:
            raise ValueError(
                f"{name} must be 1-{_MAX_EXTENSION_CHARS} characters without NUL"
            )

    def _ensure_open(self) -> None:
        if self._closed:
            raise DatabaseClosedError("task store is closed")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _dump_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _load_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _to_record(row: sqlite3.Row) -> TaskRecord:
    result_json = row["result_json"]
    return TaskRecord(
        task_id=row["task_id"],
        idempotency_key=row["idempotency_key"],
        project_path=Path(row["project_path"]),
        instruction=row["instruction"],
        timeout_seconds=float(row["timeout_seconds"]),
        role=row["role"],
        status=TaskStatus(row["status"]),
        owner=row["owner"],
        agent=row["agent"],
        provider=row["provider"],
        tool=row["tool"],
        plugin=row["plugin"],
        result=(
            None if result_json is None else TaskResult.model_validate_json(result_json)
        ),
        created_at=_required_datetime(row["created_at"]),
        updated_at=_required_datetime(row["updated_at"]),
        started_at=_load_datetime(row["started_at"]),
        finished_at=_load_datetime(row["finished_at"]),
    )


def _to_event(row: sqlite3.Row) -> TaskEvent:
    data = json.loads(row["data_json"])
    if not isinstance(data, dict):
        raise EventLogError(f"event data for {row['task_id']!r} is not an object")
    return TaskEvent(
        task_id=row["task_id"],
        sequence=int(row["sequence"]),
        status=TaskStatus(row["status"]),
        message=row["message"],
        timestamp=_required_datetime(row["timestamp"]),
        data=data,
    )


def _required_datetime(value: str) -> datetime:
    parsed = _load_datetime(value)
    if parsed is None:  # pragma: no cover - column is NOT NULL
        raise StorageError("required timestamp is missing")
    return parsed
