"""Provider adapters: turn a validated request into a shell-free process run."""

from __future__ import annotations

import contextlib
import re
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

from agent_protocol import (
    AgentCapabilities,
    TaskEvent,
    TaskRequest,
    TaskResult,
    TaskStatus,
)
from agent_runtime.errors import (
    ERROR_CANCELLED,
    ERROR_EXECUTOR_ERROR,
    ERROR_EXECUTOR_MISSING,
    ERROR_PROCESS_START_FAILED,
    ERROR_RUNTIME_ERROR,
    ERROR_TIMEOUT,
)
from agent_runtime.process import (
    ProcessManager,
    ProcessOutcome,
    ProcessStartError,
    classify_exit_code,
)

EventSink = Callable[[TaskEvent], None]

_PLACEHOLDER = re.compile(r"\{(instruction|project_path|task_id|role)\}")


def _now() -> datetime:
    return datetime.now(UTC)


@runtime_checkable
class Executor(Protocol):
    """Builds the argv array for a request.

    Implementations must return separate argv elements. They must not join the
    instruction into a shell command string.
    """

    def build_argv(self, request: TaskRequest) -> Sequence[str]: ...


@dataclass(frozen=True, slots=True)
class ArgvExecutor:
    """Executor configured as an argv template with safe single-pass placeholders.

    Placeholders are replaced within individual argv elements. A user instruction
    therefore always remains one argument, even if it contains spaces, quotes, or
    shell metacharacters.
    """

    argv: tuple[str, ...]
    placeholders: bool = True

    def __post_init__(self) -> None:
        if not self.argv:
            raise ValueError("argv must not be empty")
        for item in self.argv:
            if not isinstance(item, str):
                raise TypeError("every argv element must be a string")
            if "\x00" in item:
                raise ValueError("argv elements must not contain NUL characters")

    def build_argv(self, request: TaskRequest) -> list[str]:
        if not self.placeholders:
            return list(self.argv)
        values = {
            "instruction": request.instruction,
            "project_path": str(request.project_path),
            "task_id": request.task_id,
            "role": request.role,
        }
        return [_PLACEHOLDER.sub(lambda match: values[match.group(1)], part) for part in self.argv]


class AgentAdapter(ABC):
    """Contract implemented by each provider adapter."""

    @property
    @abstractmethod
    def capabilities(self) -> AgentCapabilities:
        """Static capabilities advertised by this adapter."""

    @abstractmethod
    def run(
        self,
        request: TaskRequest,
        on_event: EventSink | None = None,
        cancel_event: threading.Event | None = None,
    ) -> TaskResult:
        """Execute a request and return a terminal result."""


class ProcessAgentAdapter(AgentAdapter):
    """Adapter that runs a configured executor through :class:`ProcessManager`."""

    def __init__(
        self,
        provider: str,
        executor: Executor | None,
        *,
        process_manager: ProcessManager | None = None,
        roles: Sequence[str] = ("default",),
        supports_streaming: bool = False,
        max_timeout_seconds: float = 3600.0,
    ) -> None:
        self._capabilities = AgentCapabilities(
            provider=provider,
            roles=tuple(roles),
            supports_cancel=True,
            supports_streaming=supports_streaming,
            max_timeout_seconds=max_timeout_seconds,
        )
        self._executor = executor
        self._process_manager = process_manager or ProcessManager()

    @property
    def capabilities(self) -> AgentCapabilities:
        return self._capabilities

    def run(
        self,
        request: TaskRequest,
        on_event: EventSink | None = None,
        cancel_event: threading.Event | None = None,
    ) -> TaskResult:
        started = _now()

        if cancel_event is not None and cancel_event.is_set():
            result = self._finish_precheck(
                request, TaskStatus.CANCELLED, ERROR_CANCELLED, started
            )
            self._emit_terminal(on_event, result, sequence=0)
            return result

        if self._executor is None:
            result = self._result(
                request,
                TaskStatus.FAILED,
                started=started,
                error_code=ERROR_EXECUTOR_MISSING,
                stderr=f"no executor configured for provider {self._capabilities.provider!r}",
            )
            self._emit_terminal(on_event, result, sequence=0)
            return result

        try:
            argv = list(self._executor.build_argv(request))
        except Exception as exc:
            result = self._result(
                request,
                TaskStatus.FAILED,
                started=started,
                error_code=ERROR_EXECUTOR_ERROR,
                stderr=f"failed to build argv: {type(exc).__name__}: {exc}",
            )
            self._emit_terminal(on_event, result, sequence=0)
            return result

        if not argv:
            result = self._result(
                request,
                TaskStatus.FAILED,
                started=started,
                error_code=ERROR_EXECUTOR_ERROR,
                stderr="executor produced an empty argv",
            )
            self._emit_terminal(on_event, result, sequence=0)
            return result

        self._emit(
            on_event,
            TaskEvent(
                task_id=request.task_id,
                sequence=0,
                status=TaskStatus.RUNNING,
                message=f"starting provider {self._capabilities.provider!r}",
            ),
        )

        outcome: ProcessOutcome | None = None
        try:
            outcome = self._process_manager.run(
                argv=argv,
                cwd=request.project_path,
                timeout=request.timeout_seconds,
                cancel_event=cancel_event,
            )
        except ProcessStartError as exc:
            result = self._result(
                request,
                TaskStatus.FAILED,
                started=started,
                error_code=ERROR_PROCESS_START_FAILED,
                stderr=str(exc),
            )
        except (OSError, ValueError) as exc:
            result = self._result(
                request,
                TaskStatus.FAILED,
                started=started,
                error_code=ERROR_PROCESS_START_FAILED,
                stderr=f"{type(exc).__name__}: {exc}",
            )
        except Exception as exc:
            result = self._result(
                request,
                TaskStatus.FAILED,
                started=started,
                error_code=ERROR_RUNTIME_ERROR,
                stderr=f"{type(exc).__name__}: {exc}",
            )
        else:
            result = self._map_outcome(request, outcome, started)

        self._emit_terminal(on_event, result, sequence=1, outcome=outcome)
        return result

    def _emit_terminal(
        self,
        on_event: EventSink | None,
        result: TaskResult,
        *,
        sequence: int,
        outcome: ProcessOutcome | None = None,
    ) -> None:
        self._emit(
            on_event,
            TaskEvent(
                task_id=result.task_id,
                sequence=sequence,
                status=result.status,
                message=result.error_code or "completed",
                data={
                    "exit_code": result.exit_code,
                    "timed_out": outcome.timed_out if outcome is not None else False,
                    "cancelled": outcome.cancelled if outcome is not None else False,
                },
            ),
        )

    def _map_outcome(
        self,
        request: TaskRequest,
        outcome: ProcessOutcome,
        started: datetime,
    ) -> TaskResult:
        terminated_exit_code = None if outcome.exit_code == 0 else outcome.exit_code
        if outcome.cancelled:
            return self._result(
                request,
                TaskStatus.CANCELLED,
                started=started,
                exit_code=terminated_exit_code,
                stdout=outcome.stdout,
                stderr=outcome.stderr,
                error_code=ERROR_CANCELLED,
            )
        if outcome.timed_out:
            return self._result(
                request,
                TaskStatus.TIMED_OUT,
                started=started,
                exit_code=terminated_exit_code,
                stdout=outcome.stdout,
                stderr=outcome.stderr,
                error_code=ERROR_TIMEOUT,
            )
        if outcome.exit_code == 0:
            return self._result(
                request,
                TaskStatus.SUCCEEDED,
                started=started,
                exit_code=0,
                stdout=outcome.stdout,
                stderr=outcome.stderr,
            )
        error_code = classify_exit_code(outcome.exit_code)
        if error_code == "ok":
            error_code = ERROR_RUNTIME_ERROR
        return self._result(
            request,
            TaskStatus.FAILED,
            started=started,
            exit_code=outcome.exit_code,
            stdout=outcome.stdout,
            stderr=outcome.stderr,
            error_code=error_code,
        )

    def _finish_precheck(
        self,
        request: TaskRequest,
        status: TaskStatus,
        error_code: str,
        started: datetime,
    ) -> TaskResult:
        return self._result(
            request,
            status,
            started=started,
            error_code=error_code,
            stderr="cancellation requested before process start",
        )

    @staticmethod
    def _result(
        request: TaskRequest,
        status: TaskStatus,
        *,
        started: datetime,
        exit_code: int | None = None,
        stdout: str = "",
        stderr: str = "",
        error_code: str | None = None,
    ) -> TaskResult:
        return TaskResult(
            task_id=request.task_id,
            status=status,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            diff=None,
            error_code=error_code,
            started_at=started,
            finished_at=_now(),
        )

    @staticmethod
    def _emit(on_event: EventSink | None, event: TaskEvent) -> None:
        if on_event is None:
            return
        # Observers must not affect task execution or its terminal result.
        with contextlib.suppress(Exception):
            on_event(event)
