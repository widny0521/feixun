"""Pydantic models for the task protocol shared by API and agent runtime."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

from agent_protocol.status import TaskStatus

MAX_TIMEOUT_SECONDS = 3600.0
MAX_INSTRUCTION_CHARS = 100_000

TASK_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$"
IDEMPOTENCY_KEY_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
ROLE_PATTERN = r"^[a-z][a-z0-9_-]{0,63}$"


def _now() -> datetime:
    return datetime.now(UTC)


class TaskRequest(BaseModel):
    """A validated unit of work submitted to the agent runtime.

    ``project_path`` is also the working directory of the child process. It must be
    absolute; authorization against the configured project roots happens in the
    runtime, before any process is spawned.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: str = Field(min_length=1, max_length=64, pattern=TASK_ID_PATTERN)
    idempotency_key: str = Field(
        min_length=1, max_length=128, pattern=IDEMPOTENCY_KEY_PATTERN
    )
    project_path: Path
    instruction: str = Field(min_length=1, max_length=MAX_INSTRUCTION_CHARS)
    timeout_seconds: float = Field(gt=0, le=MAX_TIMEOUT_SECONDS)
    role: str = Field(default="default", pattern=ROLE_PATTERN)

    @field_validator("instruction")
    @classmethod
    def _validate_instruction(cls, value: str) -> str:
        if "\x00" in value:
            raise ValueError("instruction must not contain NUL characters")
        if not value.strip():
            raise ValueError("instruction must not be blank")
        return value

    @field_validator("project_path")
    @classmethod
    def _validate_project_path(cls, value: Path) -> Path:
        if "\x00" in str(value):
            raise ValueError("project_path must not contain NUL characters")
        if not value.is_absolute():
            raise ValueError("project_path must be an absolute path")
        return value


class TaskResult(BaseModel):
    """Terminal outcome of a task execution.

    A result may only carry a terminal status. Failed, cancelled, and timed-out
    results must describe their cause via ``error_code``; successful results must
    exit with code 0 and no error code.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: str = Field(min_length=1)
    status: TaskStatus
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""
    diff: str | None = None
    error_code: str | None = Field(default=None, max_length=64)
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @model_validator(mode="after")
    def _validate_consistency(self) -> TaskResult:
        if not self.status.is_terminal:
            raise ValueError(f"TaskResult status must be terminal, got {self.status.value}")
        if self.status is TaskStatus.SUCCEEDED:
            if self.exit_code != 0:
                raise ValueError("succeeded result must have exit_code 0")
            if self.error_code is not None:
                raise ValueError("succeeded result must not have an error_code")
        elif not self.error_code:
            raise ValueError(f"{self.status.value} result must have an error_code")
        elif self.exit_code == 0:
            raise ValueError(f"{self.status.value} result must not have exit_code 0")
        return self


class TaskEvent(BaseModel):
    """An immutable, ordered lifecycle event for a task."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: str = Field(min_length=1)
    sequence: int = Field(ge=0)
    status: TaskStatus
    message: str = ""
    timestamp: datetime = Field(default_factory=_now)
    data: dict[str, JsonValue] = Field(default_factory=dict)


class AgentCapabilities(BaseModel):
    """Static capabilities advertised by a provider adapter."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    provider: str = Field(min_length=1, max_length=64)
    roles: tuple[str, ...] = ("default",)
    supports_cancel: bool = False
    supports_streaming: bool = False
    max_timeout_seconds: float = Field(default=MAX_TIMEOUT_SECONDS, gt=0)

    def supports_role(self, role: str) -> bool:
        return "*" in self.roles or role in self.roles


class RunHandle(BaseModel):
    """Identifier returned when a task is started asynchronously."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    run_id: str = Field(default_factory=lambda: uuid4().hex, min_length=1, max_length=64)
    task_id: str = Field(min_length=1, max_length=64, pattern=TASK_ID_PATTERN)
    status: TaskStatus = TaskStatus.QUEUED
    created_at: datetime = Field(default_factory=_now)
