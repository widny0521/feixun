from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from agent_protocol import (
    AgentCapabilities,
    InvalidTransitionError,
    RunHandle,
    TaskEvent,
    TaskRequest,
    TaskResult,
    TaskStatus,
    can_transition,
    ensure_transition,
)


def make_request(project_path: Path, **overrides: object) -> TaskRequest:
    data: dict[str, object] = {
        "task_id": "task-1",
        "idempotency_key": "idem-1",
        "project_path": project_path,
        "instruction": "run the tests",
        "timeout_seconds": 10.0,
    }
    data.update(overrides)
    return TaskRequest(**data)


def test_status_values_and_legal_transitions() -> None:
    assert {status.value for status in TaskStatus} == {
        "queued",
        "running",
        "succeeded",
        "failed",
        "cancelled",
        "timed_out",
    }
    assert can_transition(TaskStatus.QUEUED, TaskStatus.RUNNING)
    assert can_transition(TaskStatus.RUNNING, TaskStatus.SUCCEEDED)
    assert can_transition(TaskStatus.RUNNING, TaskStatus.FAILED)
    assert can_transition(TaskStatus.RUNNING, TaskStatus.CANCELLED)
    assert can_transition(TaskStatus.RUNNING, TaskStatus.TIMED_OUT)
    assert TaskStatus.SUCCEEDED.is_terminal
    assert not TaskStatus.RUNNING.is_terminal

    ensure_transition(TaskStatus.QUEUED, TaskStatus.RUNNING)
    ensure_transition(TaskStatus.RUNNING, TaskStatus.TIMED_OUT)

    with pytest.raises(InvalidTransitionError) as exc_info:
        ensure_transition(TaskStatus.SUCCEEDED, TaskStatus.RUNNING)
    assert exc_info.value.source is TaskStatus.SUCCEEDED
    assert exc_info.value.target is TaskStatus.RUNNING
    with pytest.raises(InvalidTransitionError):
        ensure_transition(TaskStatus.QUEUED, TaskStatus.TIMED_OUT)
    assert not can_transition(TaskStatus.CANCELLED, TaskStatus.FAILED)


def test_request_serialization_round_trip(tmp_path: Path) -> None:
    request = make_request(tmp_path, instruction="line one\nline two", role="reviewer")

    payload = request.model_dump(mode="json")
    assert payload["project_path"] == str(tmp_path)
    assert payload["role"] == "reviewer"
    assert TaskRequest.model_validate_json(request.model_dump_json()) == request


@pytest.mark.parametrize(
    "overrides",
    [
        {"instruction": ""},
        {"instruction": "   \n\t"},
        {"instruction": "bad\x00instruction"},
        {"project_path": "relative/project"},
        {"timeout_seconds": 0},
        {"timeout_seconds": -1},
        {"timeout_seconds": 3601},
        {"task_id": ""},
        {"task_id": "../escape"},
        {"task_id": "has space"},
        {"idempotency_key": ""},
        {"role": "Reviewer Role"},
        {"unknown_field": "rejected"},
    ],
)
def test_invalid_requests_are_rejected(tmp_path: Path, overrides: dict[str, object]) -> None:
    data: dict[str, object] = {
        "task_id": "task-1",
        "idempotency_key": "idem-1",
        "project_path": tmp_path,
        "instruction": "run the tests",
        "timeout_seconds": 10.0,
    }
    data.update(overrides)
    with pytest.raises(ValidationError):
        TaskRequest(**data)


def test_blank_instruction_is_rejected_even_with_nul_free_text(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="instruction"):
        make_request(tmp_path, instruction="\n\t ")


def test_successful_result_requires_zero_exit_and_no_error(tmp_path: Path) -> None:
    result = TaskResult(
        task_id="task-1",
        status=TaskStatus.SUCCEEDED,
        exit_code=0,
        stdout="ok",
    )
    assert result.error_code is None
    assert TaskResult.model_validate_json(result.model_dump_json()) == result

    with pytest.raises(ValidationError):
        TaskResult(task_id="task-1", status=TaskStatus.SUCCEEDED, exit_code=1)
    with pytest.raises(ValidationError):
        TaskResult(
            task_id="task-1",
            status=TaskStatus.SUCCEEDED,
            exit_code=0,
            error_code="nonzero_exit",
        )


def test_results_must_be_terminal_and_describe_failures() -> None:
    with pytest.raises(ValidationError):
        TaskResult(task_id="task-1", status=TaskStatus.RUNNING)
    with pytest.raises(ValidationError):
        TaskResult(task_id="task-1", status=TaskStatus.FAILED, exit_code=1)
    with pytest.raises(ValidationError):
        TaskResult(task_id="task-1", status=TaskStatus.CANCELLED)
    with pytest.raises(ValidationError):
        TaskResult(task_id="task-1", status=TaskStatus.SUCCEEDED, exit_code=0, extra="x")

    failed = TaskResult(
        task_id="task-1",
        status=TaskStatus.FAILED,
        exit_code=3,
        stdout="partial",
        stderr="fatal",
        diff=None,
        error_code="nonzero_exit",
    )
    assert failed.status is TaskStatus.FAILED
    assert failed.exit_code == 3


@pytest.mark.parametrize(
    "status",
    [TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMED_OUT],
)
def test_non_success_results_reject_zero_exit(status: TaskStatus) -> None:
    with pytest.raises(ValidationError, match="exit_code 0"):
        TaskResult(
            task_id="task-1",
            status=status,
            exit_code=0,
            error_code="failure",
        )


def test_task_event_is_ordered_and_serializable() -> None:
    event = TaskEvent(
        task_id="task-1",
        sequence=0,
        status=TaskStatus.RUNNING,
        message="started",
        timestamp=datetime(2026, 11, 12, tzinfo=UTC),
        data={"pid": 42, "nested": {"ok": True}},
    )
    assert TaskEvent.model_validate_json(event.model_dump_json()) == event

    with pytest.raises(ValidationError):
        TaskEvent(task_id="task-1", sequence=-1, status=TaskStatus.QUEUED)
    with pytest.raises(ValidationError):
        TaskEvent(task_id="task-1", sequence=0, status=TaskStatus.QUEUED, extra=True)


def test_agent_capabilities_role_matching() -> None:
    capabilities = AgentCapabilities(
        provider="test-provider",
        roles=("default", "reviewer"),
        supports_cancel=True,
        max_timeout_seconds=120,
    )
    assert capabilities.supports_role("reviewer")
    assert not capabilities.supports_role("coder")
    assert AgentCapabilities(provider="p", roles=("*",)).supports_role("anything")


def test_run_handle_defaults_and_validation() -> None:
    handle = RunHandle(task_id="task-1")
    assert handle.run_id
    assert len(handle.run_id) == 32
    assert handle.status is TaskStatus.QUEUED
    assert handle.created_at.tzinfo is not None

    with pytest.raises(ValidationError):
        RunHandle(task_id="bad task id")
