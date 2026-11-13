"""Task lifecycle statuses and legal transitions."""

from __future__ import annotations

from enum import StrEnum


class TaskStatus(StrEnum):
    """Lifecycle state of a task execution."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"

    @property
    def is_terminal(self) -> bool:
        return self in TERMINAL_STATUSES


TERMINAL_STATUSES = frozenset(
    {
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.TIMED_OUT,
    }
)

_ALLOWED_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.QUEUED: frozenset(
        {TaskStatus.RUNNING, TaskStatus.FAILED, TaskStatus.CANCELLED}
    ),
    TaskStatus.RUNNING: frozenset(
        {
            TaskStatus.SUCCEEDED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.TIMED_OUT,
        }
    ),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.FAILED: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
    TaskStatus.TIMED_OUT: frozenset(),
}


class InvalidTransitionError(ValueError):
    """Raised when a task state transition is not allowed."""

    def __init__(self, source: TaskStatus, target: TaskStatus) -> None:
        self.source = source
        self.target = target
        super().__init__(f"invalid task transition: {source.value} -> {target.value}")


def can_transition(source: TaskStatus, target: TaskStatus) -> bool:
    """Return whether ``source`` may legally move to ``target``."""

    return target in _ALLOWED_TRANSITIONS[source]


def ensure_transition(source: TaskStatus, target: TaskStatus) -> None:
    """Raise :class:`InvalidTransitionError` for an illegal transition."""

    if not can_transition(source, target):
        raise InvalidTransitionError(source, target)
