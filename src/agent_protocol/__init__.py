"""Shared task protocol: requests, results, events, capabilities, and run handles."""

from agent_protocol.models import (
    MAX_INSTRUCTION_CHARS,
    MAX_TIMEOUT_SECONDS,
    AgentCapabilities,
    RunHandle,
    TaskEvent,
    TaskRequest,
    TaskResult,
)
from agent_protocol.status import (
    TERMINAL_STATUSES,
    InvalidTransitionError,
    TaskStatus,
    can_transition,
    ensure_transition,
)

__all__ = [
    "MAX_INSTRUCTION_CHARS",
    "MAX_TIMEOUT_SECONDS",
    "TERMINAL_STATUSES",
    "AgentCapabilities",
    "InvalidTransitionError",
    "RunHandle",
    "TaskEvent",
    "TaskRequest",
    "TaskResult",
    "TaskStatus",
    "can_transition",
    "ensure_transition",
]
