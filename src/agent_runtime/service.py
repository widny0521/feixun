"""Task orchestration: authorization, idempotency, cancellation, and results."""

from __future__ import annotations

import contextlib
import os
import threading
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from agent_protocol import RunHandle, TaskEvent, TaskRequest, TaskResult, TaskStatus
from agent_runtime.errors import (
    ERROR_CANCELLED,
    ERROR_INVALID_PROJECT_PATH,
    ERROR_PROVIDER_NOT_FOUND,
    ERROR_RUNTIME_ERROR,
    ERROR_TIMEOUT_EXCEEDS_CAPABILITY,
    ERROR_UNAUTHORIZED_PATH,
)
from agent_runtime.registry import ProviderNotFoundError, ProviderRegistry


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass
class _ActiveRun:
    handle: RunHandle
    cancel_event: threading.Event
    done: threading.Event
    thread: threading.Thread | None = None


@dataclass
class _Claim:
    owner: bool
    gate: threading.Event | None
    cached: TaskResult | None
    active: _ActiveRun | None = None


class AgentRuntimeService:
    """Coordinates task submission for one local agent runtime.

    Responsibilities kept in this layer:

    * enforce authorized project roots before any process starts;
    * de-duplicate requests by idempotency key (and by task id);
    * resolve a logical role through :class:`ProviderRegistry`;
    * expose cancellation and result lookup for asynchronous runs.
    """

    def __init__(
        self,
        registry: ProviderRegistry,
        allowed_roots: Iterable[Path | str],
    ) -> None:
        roots = tuple(Path(root).expanduser().resolve() for root in allowed_roots)
        if not roots:
            raise ValueError("allowed_roots must not be empty")
        self._registry = registry
        self._allowed_roots = roots
        self._lock = threading.RLock()
        self._results: dict[str, TaskResult] = {}
        self._idempotency: dict[str, TaskResult] = {}
        self._inflight: dict[str, threading.Event] = {}
        self._active: dict[str, _ActiveRun] = {}
        self._handles: dict[str, RunHandle] = {}
        self._key_handles: dict[str, RunHandle] = {}
        self._events: dict[str, list[TaskEvent]] = {}

    def execute(self, request: TaskRequest | Mapping[str, Any]) -> TaskResult:
        """Run a task synchronously, returning the terminal result.

        Duplicate submissions with the same idempotency key, or for a task id
        already known to this service, return the first result without executing
        again. Concurrent duplicates wait for the owner to finish.
        """

        req = self._coerce_request(request)
        claim = self._claim(req)
        if claim.cached is not None:
            return claim.cached
        if not claim.owner:
            return self._wait_for_duplicate(req, claim.gate)

        active = claim.active
        if active is None:
            raise RuntimeError("owner claim is missing active run state")
        return self._run_claimed(req, active.cancel_event, active.done)

    def start(self, request: TaskRequest | Mapping[str, Any]) -> RunHandle:
        """Start a task on a daemon thread and return its handle."""

        req = self._coerce_request(request)
        claim = self._claim(req)
        if claim.cached is not None:
            return self._completed_handle(req, claim.cached)
        if not claim.owner:
            with self._lock:
                active = self._active.get(req.task_id)
                if active is not None:
                    return active.handle
                handle = self._key_handles.get(req.idempotency_key)
            if handle is not None:
                return handle
            result = self._wait_for_duplicate(req, claim.gate)
            return self._completed_handle(req, result)

        active = claim.active
        if active is None:
            raise RuntimeError("owner claim is missing active run state")
        thread = threading.Thread(
            target=self._run_background,
            args=(req, active.cancel_event, active.done),
            name=f"agent-task-{req.task_id}",
            daemon=True,
        )
        with self._lock:
            active.thread = thread
        thread.start()
        return active.handle

    def cancel(self, task_id: str) -> bool:
        """Request cancellation of a queued or running task.

        Returns ``False`` when the task is unknown or already finished. The
        terminal result is produced by the adapter/process layer and is available
        via :meth:`result` or :meth:`wait`.
        """

        with self._lock:
            active = self._active.get(task_id)
            if active is None:
                return False
            active.cancel_event.set()
            return True

    def wait(self, task_id: str, timeout: float = 30.0) -> TaskResult | None:
        """Wait for a task's terminal result, or return ``None`` on timeout."""

        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                result = self._results.get(task_id)
                active = self._active.get(task_id)
            if result is not None:
                return result
            if active is None:
                return None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            if active.thread is not None:
                active.thread.join(timeout=min(remaining, 0.1))
            else:
                time.sleep(min(remaining, 0.01))

    def result(self, task_id: str) -> TaskResult | None:
        with self._lock:
            return self._results.get(task_id)

    def events(self, task_id: str) -> tuple[TaskEvent, ...]:
        with self._lock:
            return tuple(self._events.get(task_id, ()))

    def is_active(self, task_id: str) -> bool:
        with self._lock:
            return task_id in self._active

    def active_task_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._active))

    def _claim(self, request: TaskRequest) -> _Claim:
        with self._lock:
            cached = self._idempotency.get(request.idempotency_key)
            if cached is None:
                cached = self._results.get(request.task_id)
            if cached is not None:
                return _Claim(owner=False, gate=None, cached=cached)

            active = self._active.get(request.task_id)
            if active is not None:
                return _Claim(owner=False, gate=active.done, cached=None)

            gate = self._inflight.get(request.idempotency_key)
            if gate is not None:
                return _Claim(owner=False, gate=gate, cached=None)

            # Reserve both the idempotency key and task id atomically, and
            # publish cancellation state before execution can begin. This closes
            # the window where a concurrent duplicate or cancel() call could
            # observe a claimed-but-not-yet-active task.
            gate = threading.Event()
            handle = RunHandle(task_id=request.task_id)
            active = _ActiveRun(handle, threading.Event(), gate)
            self._inflight[request.idempotency_key] = gate
            self._handles[request.task_id] = handle
            self._key_handles[request.idempotency_key] = handle
            self._active[request.task_id] = active
            return _Claim(owner=True, gate=gate, cached=None, active=active)

    def _wait_for_duplicate(
        self,
        request: TaskRequest,
        gate: threading.Event | None,
    ) -> TaskResult:
        if gate is None:
            return self._failure(request, ERROR_RUNTIME_ERROR, "duplicate task has no gate")
        if not gate.wait(timeout=request.timeout_seconds + 60.0):
            return self._failure(
                request,
                ERROR_RUNTIME_ERROR,
                "timed out waiting for a duplicate task to finish",
            )
        with self._lock:
            cached = self._idempotency.get(request.idempotency_key)
            if cached is None:
                cached = self._results.get(request.task_id)
        if cached is None:
            return self._failure(
                request,
                ERROR_RUNTIME_ERROR,
                "duplicate task finished without a result",
            )
        return cached

    def _run_claimed(
        self,
        request: TaskRequest,
        cancel_event: threading.Event,
        gate: threading.Event,
    ) -> TaskResult:
        result: TaskResult | None = None
        try:
            result = self._execute_once(request, cancel_event)
        except Exception as exc:
            result = self._failure(
                request,
                ERROR_RUNTIME_ERROR,
                f"{type(exc).__name__}: {exc}",
            )
        finally:
            if result is None:
                result = self._failure(
                    request,
                    ERROR_RUNTIME_ERROR,
                    "task produced no result",
                )
            self._record_terminal_event(request, result)
            with self._lock:
                self._idempotency[request.idempotency_key] = result
                self._results[request.task_id] = result
                self._inflight.pop(request.idempotency_key, None)
                self._active.pop(request.task_id, None)
                self._key_handles.pop(request.idempotency_key, None)
            gate.set()
        return result

    def _run_background(
        self,
        request: TaskRequest,
        cancel_event: threading.Event,
        gate: threading.Event,
    ) -> None:
        # _run_claimed finalizes the result and gate in its finally block.
        # Never let a thread-level exception escape into pytest/stdout noise.
        with contextlib.suppress(BaseException):
            self._run_claimed(request, cancel_event, gate)

    def _execute_once(
        self,
        request: TaskRequest,
        cancel_event: threading.Event,
    ) -> TaskResult:
        started = _now()
        if cancel_event.is_set():
            return self._terminal(
                request,
                TaskStatus.CANCELLED,
                error_code=ERROR_CANCELLED,
                stderr="cancellation requested before dispatch",
                started=started,
            )

        if not self._is_authorized(request.project_path):
            return self._failure(
                request,
                ERROR_UNAUTHORIZED_PATH,
                f"project path is outside the allowed roots: {request.project_path}",
                started=started,
            )
        if not request.project_path.is_dir():
            return self._failure(
                request,
                ERROR_INVALID_PROJECT_PATH,
                f"project path is not a directory: {request.project_path}",
                started=started,
            )

        try:
            adapter = self._registry.resolve(request.role)
        except ProviderNotFoundError:
            return self._failure(
                request,
                ERROR_PROVIDER_NOT_FOUND,
                f"no provider registered for role {request.role!r}",
                started=started,
            )

        capabilities = adapter.capabilities
        if request.timeout_seconds > capabilities.max_timeout_seconds:
            return self._failure(
                request,
                ERROR_TIMEOUT_EXCEEDS_CAPABILITY,
                (
                    f"timeout {request.timeout_seconds}s exceeds provider limit "
                    f"{capabilities.max_timeout_seconds}s"
                ),
                started=started,
            )

        try:
            return adapter.run(
                request,
                on_event=self._record_event,
                cancel_event=cancel_event,
            )
        except Exception as exc:
            return self._failure(
                request,
                ERROR_RUNTIME_ERROR,
                f"{type(exc).__name__}: {exc}",
                started=started,
            )

    def _record_event(self, event: TaskEvent) -> None:
        with self._lock:
            self._events.setdefault(event.task_id, []).append(event)

    def _record_terminal_event(self, request: TaskRequest, result: TaskResult) -> None:
        """Ensure every terminal result has a matching terminal event.

        Adapters emit a terminal event themselves. Service-level rejections
        (authorization, provider lookup, capability checks) happen before the
        adapter, so this fallback records their terminal event with the next
        contiguous sequence number.
        """

        with self._lock:
            events = self._events.setdefault(request.task_id, [])
            if events and events[-1].status is result.status:
                return
            events.append(
                TaskEvent(
                    task_id=request.task_id,
                    sequence=len(events),
                    status=result.status,
                    message=result.error_code or "completed",
                    data={"exit_code": result.exit_code},
                )
            )

    def _is_authorized(self, project_path: Path) -> bool:
        candidate = os.path.normcase(os.path.abspath(project_path.expanduser().resolve()))
        for root in self._allowed_roots:
            root_key = os.path.normcase(os.path.abspath(root))
            try:
                if os.path.commonpath([candidate, root_key]) == root_key:
                    return True
            except ValueError:
                continue
        return False

    def _completed_handle(self, request: TaskRequest, result: TaskResult) -> RunHandle:
        handle = RunHandle(task_id=request.task_id, status=result.status)
        with self._lock:
            self._handles[request.task_id] = handle
        return handle

    @staticmethod
    def _coerce_request(request: TaskRequest | Mapping[str, Any]) -> TaskRequest:
        if isinstance(request, TaskRequest):
            return request
        if isinstance(request, Mapping):
            return TaskRequest.model_validate(request)
        raise TypeError("request must be a TaskRequest or a mapping")

    def _failure(
        self,
        request: TaskRequest,
        error_code: str,
        message: str,
        *,
        started: datetime | None = None,
    ) -> TaskResult:
        return self._terminal(
            request,
            TaskStatus.FAILED,
            error_code=error_code,
            stderr=message,
            started=started,
        )

    @staticmethod
    def _terminal(
        request: TaskRequest,
        status: TaskStatus,
        *,
        error_code: str,
        stderr: str,
        started: datetime | None = None,
        exit_code: int | None = None,
    ) -> TaskResult:
        now = _now()
        return TaskResult(
            task_id=request.task_id,
            status=status,
            exit_code=exit_code,
            stdout="",
            stderr=stderr,
            diff=None,
            error_code=error_code,
            started_at=started or now,
            finished_at=now,
        )
