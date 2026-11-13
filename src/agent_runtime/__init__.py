"""Windows agent runtime: process execution, adapters, registry, and service."""

from agent_runtime.adapter import AgentAdapter, ArgvExecutor, Executor, ProcessAgentAdapter
from agent_runtime.process import (
    ProcessManager,
    ProcessOutcome,
    ProcessStartError,
    classify_exit_code,
)
from agent_runtime.registry import ProviderNotFoundError, ProviderRegistry
from agent_runtime.service import AgentRuntimeService

__all__ = [
    "AgentAdapter",
    "AgentRuntimeService",
    "ArgvExecutor",
    "Executor",
    "ProcessAgentAdapter",
    "ProcessManager",
    "ProcessOutcome",
    "ProcessStartError",
    "ProviderNotFoundError",
    "ProviderRegistry",
    "classify_exit_code",
]
