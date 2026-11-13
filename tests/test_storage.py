from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from agent_protocol import InvalidTransitionError, TaskEvent, TaskRequest, TaskResult, TaskStatus
from storage import (
    RECOVERY_ERROR_CODE,
    SCHEMA_VERSION,
    Database,
    EventLogError,
    SchemaVersionError,
    TaskConflictError,
    TaskNotFoundError,
    TaskStateError,
    TaskStore,
    UnauthorizedProjectPathError,
)


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    path = tmp_path / "workspace"
    path.mkdir()
    return path


@pytest.fixture
def project(workspace: Path) -> Path:
    path = workspace / "project"
    path.mkdir()
    return path


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "state" / "tasks.db"


def make_request(
    project: Path,
    *,
    task_id: str = "task-1",
    idempotency_key: str = "idem-1",
    instruction: str = "do the work",
    timeout_seconds: float = 10.0,
) -> TaskRequest:
    return TaskRequest(
        task_id=task_id,
        idempotency_key=idempotency_key,
        project_path=project,
        instruction=instruction,
        timeout_seconds=timeout_seconds,
    )


def make_event(
    task_id: str,
    sequence: int,
    status: TaskStatus,
    *,
    message: str = "",
    timestamp: datetime | None = None,
) -> TaskEvent:
    return TaskEvent(
        task_id=task_id,
        sequence=sequence,
        status=status,
        message=message,
        timestamp=timestamp or datetime.now(UTC),
    )


def success_result(task_id: str = "task-1", *, stdout: str = "done\n") -> TaskResult:
    now = datetime.now(UTC)
    return TaskResult(
        task_id=task_id,
        status=TaskStatus.SUCCEEDED,
        exit_code=0,
        stdout=stdout,
        started_at=now,
        finished_at=now,
    )


def failed_result(task_id: str = "task-1") -> TaskResult:
    now = datetime.now(UTC)
    return TaskResult(
        task_id=task_id,
        status=TaskStatus.FAILED,
        exit_code=1,
        stderr="boom\n",
        error_code="nonzero_exit",
        started_at=now,
        finished_at=now,
    )


def test_create_or_get_deduplicates_by_key_and_task_id(
    db_path: Path,
    workspace: Path,
    project: Path,
) -> None:
    with TaskStore(db_path, allowed_roots=[workspace]) as store:
        request = make_request(project)
        first = store.create_or_get(
            request,
            owner="owner-1",
            agent="agent-1",
            provider="provider-1",
            tool="tool-1",
            plugin="plugin-1",
        )

        assert first.task_id == "task-1"
        assert first.status is TaskStatus.QUEUED
        assert first.owner == "owner-1"
        assert first.agent == "agent-1"
        assert first.provider == "provider-1"
        assert first.tool == "tool-1"
        assert first.plugin == "plugin-1"
        assert first.created_at == first.updated_at

        duplicate = store.create_or_get(request)
        assert duplicate == first
        assert len(store.list_tasks()) == 1

        same_key = store.create_or_get(
            make_request(project, task_id="task-2", idempotency_key="idem-1")
        )
        assert same_key.task_id == "task-1"
        assert same_key.provider == "provider-1"

        same_task_id = store.create_or_get(
            make_request(project, task_id="task-1", idempotency_key="idem-2")
        )
        assert same_task_id.idempotency_key == "idem-1"
        assert len(store.list_tasks()) == 1


def test_state_and_events_survive_reopening(
    db_path: Path,
    workspace: Path,
    project: Path,
) -> None:
    with TaskStore(db_path, allowed_roots=[workspace]) as store:
        store.create_or_get(make_request(project))
        store.append_event(
            "task-1",
            make_event("task-1", 0, TaskStatus.RUNNING, message="started"),
        )

    with TaskStore(db_path, allowed_roots=[workspace]) as store:
        record = store.load("task-1")
        assert record is not None
        assert record.status is TaskStatus.RUNNING
        assert record.started_at is not None
        assert record.result is None
        assert [(event.sequence, event.status) for event in store.events("task-1")] == [
            (0, TaskStatus.RUNNING)
        ]

        store.append_event("task-1", make_event("task-1", 1, TaskStatus.SUCCEEDED))
        result = success_result()
        store.complete("task-1", result)

    with TaskStore(db_path, allowed_roots=[workspace]) as store:
        record = store.load("task-1")
        assert record is not None
        assert record.status is TaskStatus.SUCCEEDED
        assert record.result == result
        assert record.finished_at is not None
        assert [event.status for event in store.events("task-1")] == [
            TaskStatus.RUNNING,
            TaskStatus.SUCCEEDED,
        ]


def test_event_log_integrity(
    db_path: Path,
    workspace: Path,
    project: Path,
) -> None:
    with TaskStore(db_path, allowed_roots=[workspace]) as store:
        with pytest.raises(TaskNotFoundError):
            store.append_event(
                "missing",
                make_event("missing", 0, TaskStatus.RUNNING),
            )

        store.create_or_get(make_request(project))

        with pytest.raises(TaskNotFoundError):
            store.append_event(
                "task-2",
                make_event("task-2", 0, TaskStatus.RUNNING),
            )

        with pytest.raises(EventLogError):
            store.append_event("task-1", make_event("task-2", 0, TaskStatus.RUNNING))

        with pytest.raises(EventLogError):
            store.append_event("task-1", make_event("task-1", 1, TaskStatus.RUNNING))

        store.append_event("task-1", make_event("task-1", 0, TaskStatus.RUNNING))

        with pytest.raises(EventLogError):
            store.append_event("task-1", make_event("task-1", 0, TaskStatus.RUNNING))

        with pytest.raises(InvalidTransitionError):
            store.append_event("task-1", make_event("task-1", 1, TaskStatus.RUNNING))

        store.append_event("task-1", make_event("task-1", 1, TaskStatus.SUCCEEDED))

        with pytest.raises(InvalidTransitionError):
            store.append_event("task-1", make_event("task-1", 2, TaskStatus.FAILED))

        assert [event.sequence for event in store.events("task-1")] == [0, 1]


def test_complete_attaches_result_and_rejects_conflicts(
    db_path: Path,
    workspace: Path,
    project: Path,
) -> None:
    with TaskStore(db_path, allowed_roots=[workspace]) as store:
        store.create_or_get(make_request(project))
        store.append_event("task-1", make_event("task-1", 0, TaskStatus.RUNNING))

        with pytest.raises(TaskConflictError):
            store.complete("task-1", success_result("other-task"))

        result = success_result()
        store.complete("task-1", result)
        assert store.load("task-1").result == result  # type: ignore[union-attr]

        events = store.events("task-1")
        assert [event.status for event in events] == [
            TaskStatus.RUNNING,
            TaskStatus.SUCCEEDED,
        ]

        store.complete("task-1", result)
        assert len(store.events("task-1")) == 2

        with pytest.raises(TaskConflictError):
            store.complete("task-1", success_result(stdout="different\n"))

        with pytest.raises(TaskStateError):
            store.complete("task-1", failed_result())

        with pytest.raises(TaskNotFoundError):
            store.complete("missing", success_result("missing"))


def test_complete_records_terminal_event_for_queued_failure(
    db_path: Path,
    workspace: Path,
    project: Path,
) -> None:
    with TaskStore(db_path, allowed_roots=[workspace]) as store:
        store.create_or_get(make_request(project))
        result = failed_result()

        store.complete("task-1", result)

        record = store.load("task-1")
        assert record is not None
        assert record.status is TaskStatus.FAILED
        assert record.result == result
        events = store.events("task-1")
        assert [(event.sequence, event.status) for event in events] == [
            (0, TaskStatus.FAILED)
        ]
        assert events[0].message == "nonzero_exit"


def test_complete_reuses_existing_terminal_event(
    db_path: Path,
    workspace: Path,
    project: Path,
) -> None:
    with TaskStore(db_path, allowed_roots=[workspace]) as store:
        store.create_or_get(make_request(project))
        store.append_event("task-1", make_event("task-1", 0, TaskStatus.RUNNING))
        store.append_event(
            "task-1",
            make_event("task-1", 1, TaskStatus.FAILED, message="nonzero_exit"),
        )

        result = failed_result()
        store.complete("task-1", result)

        record = store.load("task-1")
        assert record is not None
        assert record.status is TaskStatus.FAILED
        assert record.result == result
        assert len(store.events("task-1")) == 2

        # A succeeded result cannot replace the recorded failed terminal state.
        with pytest.raises(TaskStateError):
            store.complete("task-1", success_result())


def test_recover_interrupted_tasks(
    db_path: Path,
    workspace: Path,
    project: Path,
) -> None:
    with TaskStore(db_path, allowed_roots=[workspace]) as store:
        store.create_or_get(make_request(project))
        store.create_or_get(
            make_request(project, task_id="task-2", idempotency_key="idem-2")
        )
        store.append_event("task-2", make_event("task-2", 0, TaskStatus.RUNNING))

        store.create_or_get(
            make_request(project, task_id="task-3", idempotency_key="idem-3")
        )
        store.append_event("task-3", make_event("task-3", 0, TaskStatus.RUNNING))
        completed = success_result("task-3")
        store.complete("task-3", completed)

        recovered = store.recover_interrupted_tasks()
        assert [record.task_id for record in recovered] == ["task-1", "task-2"]
        for record in recovered:
            assert record.status is TaskStatus.FAILED
            assert record.result is not None
            assert record.result.error_code == RECOVERY_ERROR_CODE
            assert record.finished_at is not None

            events = store.events(record.task_id)
            assert events[-1].status is TaskStatus.FAILED
            assert events[-1].data["recovered"] is True
            assert [event.sequence for event in events] == list(range(len(events)))

        untouched = store.load("task-3")
        assert untouched is not None
        assert untouched.status is TaskStatus.SUCCEEDED
        assert untouched.result == completed

        assert store.recover_interrupted_tasks() == ()
        assert store.list_tasks(TaskStatus.QUEUED) == ()


def test_unauthorized_project_paths_are_rejected(
    db_path: Path,
    tmp_path: Path,
    workspace: Path,
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()

    with TaskStore(db_path, allowed_roots=[workspace]) as store:
        with pytest.raises(UnauthorizedProjectPathError):
            store.create_or_get(make_request(outside))
        with pytest.raises(UnauthorizedProjectPathError):
            store.create_or_get(make_request(workspace / "project" / ".." / ".." / "outside"))

        assert store.list_tasks() == ()
        assert store.load("task-1") is None


def test_backup_copies_state_and_never_overwrites(
    db_path: Path,
    tmp_path: Path,
    workspace: Path,
    project: Path,
) -> None:
    destination = tmp_path / "backup" / "tasks-backup.db"
    with TaskStore(db_path, allowed_roots=[workspace]) as store:
        store.create_or_get(make_request(project))
        store.append_event("task-1", make_event("task-1", 0, TaskStatus.RUNNING))
        result = success_result()
        store.complete("task-1", result)

        copied = store.backup(destination)
        assert copied == destination.resolve()
        assert copied.exists()

        with pytest.raises(FileExistsError):
            store.backup(destination)

    with TaskStore(destination, allowed_roots=[workspace]) as restored:
        record = restored.load("task-1")
        assert record is not None
        assert record.result == result
        assert [event.status for event in restored.events("task-1")] == [
            TaskStatus.RUNNING,
            TaskStatus.SUCCEEDED,
        ]

    protected = tmp_path / "protected.db"
    protected.write_bytes(b"user data")
    with (
        TaskStore(db_path, allowed_roots=[workspace]) as store,
        pytest.raises(FileExistsError),
    ):
        store.backup(protected)
    assert protected.read_bytes() == b"user data"


def test_schema_version_is_recorded_and_newer_schema_is_rejected(
    db_path: Path,
    workspace: Path,
) -> None:
    with Database(db_path) as database:
        assert database.schema_version == SCHEMA_VERSION

    connection = sqlite3.connect(db_path)
    try:
        connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(SchemaVersionError):
        Database(db_path)


def test_store_rejects_empty_allowed_roots(db_path: Path) -> None:
    with pytest.raises(ValueError):
        TaskStore(db_path, allowed_roots=[])
