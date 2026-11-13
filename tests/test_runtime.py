from __future__ import annotations

import os
import sys
import threading
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from pydantic import ValidationError

from agent_protocol import TaskRequest, TaskStatus
from agent_runtime import (
    AgentRuntimeService,
    ArgvExecutor,
    ProcessAgentAdapter,
    ProcessManager,
    ProcessOutcome,
    ProviderNotFoundError,
    ProviderRegistry,
    classify_exit_code,
)
from agent_runtime.errors import (
    ERROR_ABNORMAL_EXIT,
    ERROR_CANCELLED,
    ERROR_EXECUTOR_ERROR,
    ERROR_EXECUTOR_MISSING,
    ERROR_INVALID_PROJECT_PATH,
    ERROR_NONZERO_EXIT,
    ERROR_PROVIDER_NOT_FOUND,
    ERROR_TIMEOUT,
    ERROR_UNAUTHORIZED_PATH,
)

PYTHON = sys.executable


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


def script(code: str) -> ArgvExecutor:
    return ArgvExecutor((PYTHON, "-c", code))


def make_request(
    project: Path,
    *,
    task_id: str = "task-1",
    idempotency_key: str = "idem-1",
    instruction: str = "do the work",
    timeout_seconds: float = 10.0,
    role: str = "default",
) -> TaskRequest:
    return TaskRequest(
        task_id=task_id,
        idempotency_key=idempotency_key,
        project_path=project,
        instruction=instruction,
        timeout_seconds=timeout_seconds,
        role=role,
    )


def make_service(
    workspace: Path,
    executor: ArgvExecutor | None,
    *,
    process_manager: ProcessManager | None = None,
    roles: Sequence[str] = ("default",),
) -> AgentRuntimeService:
    adapter = ProcessAgentAdapter(
        "test-provider",
        executor,
        process_manager=process_manager,
        roles=roles,
    )
    registry = ProviderRegistry()
    registry.register("default", adapter)
    return AgentRuntimeService(registry, allowed_roots=[workspace])


class RecordingExecutor:
    """Counts dispatches and forwards to a real argv executor."""

    def __init__(self, inner: ArgvExecutor) -> None:
        self._inner = inner
        self.calls = 0
        self._lock = threading.Lock()

    def build_argv(self, request: TaskRequest) -> Sequence[str]:
        with self._lock:
            self.calls += 1
        return list(self._inner.build_argv(request))


class RaisingExecutor:
    def build_argv(self, request: TaskRequest) -> Sequence[str]:
        raise RuntimeError("cannot build argv")


class StubProcessManager:
    def __init__(self, outcome: ProcessOutcome) -> None:
        self._outcome = outcome

    def run(
        self,
        argv: Sequence[str],
        cwd: Path,
        timeout: float,
        cancel_event: threading.Event | None = None,
    ) -> ProcessOutcome:
        return self._outcome


def test_normal_process_execution(workspace: Path, project: Path) -> None:
    executor = script("import os; print('hello'); print(os.getcwd())")
    service = make_service(workspace, executor)

    result = service.execute(make_request(project))

    assert result.status is TaskStatus.SUCCEEDED
    assert result.exit_code == 0
    assert result.error_code is None
    assert result.diff is None
    lines = result.stdout.splitlines()
    assert lines[0] == "hello"
    assert Path(lines[1]).resolve() == project.resolve()
    assert result.stderr == ""
    assert result.started_at is not None
    assert result.finished_at is not None
    assert result.started_at <= result.finished_at
    assert service.result("task-1") == result

    events = service.events("task-1")
    assert [event.status for event in events] == [TaskStatus.RUNNING, TaskStatus.SUCCEEDED]
    assert [event.sequence for event in events] == [0, 1]


def test_stdout_and_stderr_are_captured_separately(workspace: Path, project: Path) -> None:
    executor = script(
        "import sys; print('OUT-LINE'); print('ERR-LINE', file=sys.stderr)"
    )
    service = make_service(workspace, executor)

    result = service.execute(make_request(project))

    assert result.status is TaskStatus.SUCCEEDED
    assert result.stdout == "OUT-LINE\n"
    assert result.stderr == "ERR-LINE\n"


def test_nonzero_exit_is_failed_and_keeps_output(workspace: Path, project: Path) -> None:
    executor = script(
        "import sys; print('before'); print('fatal', file=sys.stderr); sys.exit(3)"
    )
    service = make_service(workspace, executor)

    result = service.execute(make_request(project))

    assert result.status is TaskStatus.FAILED
    assert result.exit_code == 3
    assert result.error_code == ERROR_NONZERO_EXIT
    assert result.stdout == "before\n"
    assert result.stderr == "fatal\n"


def test_timeout_terminates_process_and_preserves_logs(workspace: Path, project: Path) -> None:
    executor = script("import time; print('waiting', flush=True); time.sleep(30)")
    service = make_service(workspace, executor)

    started = time.monotonic()
    result = service.execute(make_request(project, timeout_seconds=0.4))
    elapsed = time.monotonic() - started

    assert result.status is TaskStatus.TIMED_OUT
    assert result.error_code == ERROR_TIMEOUT
    assert result.exit_code != 0
    assert "waiting" in result.stdout
    assert elapsed < 15


def test_explicit_cancellation_returns_cancelled(workspace: Path, project: Path) -> None:
    executor = script("import time; print('started', flush=True); time.sleep(30)")
    service = make_service(workspace, executor)
    request = make_request(project, timeout_seconds=30)

    handle = service.start(request)
    assert handle.task_id == "task-1"
    assert handle.status is TaskStatus.QUEUED

    time.sleep(0.5)
    assert service.cancel("task-1") is True

    result = service.wait("task-1", timeout=15)
    assert result is not None
    assert result.status is TaskStatus.CANCELLED
    assert result.error_code == ERROR_CANCELLED
    assert not service.is_active("task-1")
    assert service.cancel("task-1") is False


def test_invalid_requests_are_rejected_before_dispatch(workspace: Path) -> None:
    service = make_service(workspace, script("print('never')"))
    invalid_payload = {
        "task_id": "bad id",
        "idempotency_key": "idem-1",
        "project_path": str(workspace),
        "instruction": "do work",
        "timeout_seconds": 10,
    }

    with pytest.raises(ValidationError):
        service.execute(invalid_payload)
    with pytest.raises(TypeError):
        service.execute("not a request")  # type: ignore[arg-type]


def test_missing_executor_and_missing_provider(workspace: Path, project: Path) -> None:
    empty_registry = ProviderRegistry()
    empty_service = AgentRuntimeService(empty_registry, allowed_roots=[workspace])
    missing_provider = empty_service.execute(make_request(project))
    assert missing_provider.status is TaskStatus.FAILED
    assert missing_provider.error_code == ERROR_PROVIDER_NOT_FOUND

    service = make_service(workspace, executor=None)
    missing_executor = service.execute(make_request(project))
    assert missing_executor.status is TaskStatus.FAILED
    assert missing_executor.error_code == ERROR_EXECUTOR_MISSING
    assert "no executor configured" in missing_executor.stderr


def test_unauthorized_project_path_is_rejected_without_dispatch(
    workspace: Path,
) -> None:
    outside = workspace.parent / "outside"
    outside.mkdir()
    executor = RecordingExecutor(script("print('should not run')"))
    service = make_service(workspace, executor)

    result = service.execute(make_request(outside))

    assert result.status is TaskStatus.FAILED
    assert result.error_code == ERROR_UNAUTHORIZED_PATH
    assert executor.calls == 0
    assert [(event.sequence, event.status) for event in service.events("task-1")] == [
        (0, TaskStatus.FAILED)
    ]

    traversal = workspace / "project" / ".." / ".." / "outside"
    traversal_result = service.execute(
        make_request(traversal, task_id="task-2", idempotency_key="idem-2")
    )
    assert traversal_result.status is TaskStatus.FAILED
    assert traversal_result.error_code == ERROR_UNAUTHORIZED_PATH
    assert executor.calls == 0


def test_missing_project_directory_is_rejected(workspace: Path) -> None:
    executor = RecordingExecutor(script("print('should not run')"))
    service = make_service(workspace, executor)

    result = service.execute(make_request(workspace / "does-not-exist"))

    assert result.status is TaskStatus.FAILED
    assert result.error_code == ERROR_INVALID_PROJECT_PATH
    assert executor.calls == 0


def test_abnormal_exit_preserves_code_and_logs(workspace: Path, project: Path) -> None:
    outcome = ProcessOutcome(
        exit_code=-9,
        stdout="before crash\n",
        stderr="segfault\n",
        timed_out=False,
        cancelled=False,
        duration_seconds=0.01,
    )
    service = make_service(
        workspace,
        script("pass"),
        process_manager=StubProcessManager(outcome),
    )

    result = service.execute(make_request(project))

    assert result.status is TaskStatus.FAILED
    assert result.error_code == ERROR_ABNORMAL_EXIT
    assert result.exit_code == -9
    assert result.stdout == "before crash\n"
    assert result.stderr == "segfault\n"


@pytest.mark.parametrize(
    ("exit_code", "expected"),
    [
        (0, "ok"),
        (3, ERROR_NONZERO_EXIT),
        (-9, ERROR_ABNORMAL_EXIT),
        (3221225477, ERROR_ABNORMAL_EXIT),
        (None, ERROR_ABNORMAL_EXIT),
    ],
)
def test_exit_code_classification(exit_code: int | None, expected: str) -> None:
    assert classify_exit_code(exit_code) == expected


@pytest.mark.skipif(os.name != "nt", reason="NTSTATUS exit codes are Windows-specific")
def test_abnormal_exit_from_real_windows_process(workspace: Path, project: Path) -> None:
    executor = script("import sys; print('crash', flush=True); sys.exit(-1)")
    service = make_service(workspace, executor)

    result = service.execute(make_request(project))

    assert result.status is TaskStatus.FAILED
    assert result.error_code == ERROR_ABNORMAL_EXIT
    assert result.exit_code is not None
    assert result.exit_code > 255
    assert "crash" in result.stdout


def test_duplicate_task_requests_execute_once(workspace: Path, project: Path) -> None:
    executor = RecordingExecutor(script("print('once')"))
    service = make_service(workspace, executor)
    request = make_request(project)

    first = service.execute(request)
    second = service.execute(request)

    assert first == second
    assert first.status is TaskStatus.SUCCEEDED
    assert executor.calls == 1

    different_key = request.model_copy(update={"idempotency_key": "idem-2"})
    third = service.execute(different_key)
    assert third == first
    assert executor.calls == 1
    assert service.result("task-1") == first

    handle = service.start(request)
    assert handle.status is TaskStatus.SUCCEEDED
    assert executor.calls == 1


def test_concurrent_duplicate_requests_execute_once(workspace: Path, project: Path) -> None:
    executor = RecordingExecutor(script("import time; time.sleep(0.3); print('once')"))
    service = make_service(workspace, executor)
    request = make_request(project)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(service.execute, [request, request]))

    assert executor.calls == 1
    assert results[0] == results[1]
    assert results[0].status is TaskStatus.SUCCEEDED


def test_concurrent_same_task_different_keys_execute_once(
    workspace: Path, project: Path
) -> None:
    executor = RecordingExecutor(script("import time; time.sleep(0.3); print('once')"))
    service = make_service(workspace, executor)
    first = make_request(project, task_id="task-1", idempotency_key="idem-a")
    second = make_request(project, task_id="task-1", idempotency_key="idem-b")

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(service.execute, [first, second]))

    assert executor.calls == 1
    assert results[0] == results[1]
    assert results[0].status is TaskStatus.SUCCEEDED


def test_cancel_during_synchronous_execute(workspace: Path, project: Path) -> None:
    executor = script("import time; time.sleep(30)")
    service = make_service(workspace, executor)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            service.execute,
            make_request(project, timeout_seconds=30),
        )
        deadline = time.monotonic() + 5
        while not service.active_task_ids():
            if time.monotonic() > deadline:
                pytest.fail("task was never claimed")
            time.sleep(0.005)
        assert service.cancel("task-1") is True
        result = future.result(timeout=15)

    assert result.status is TaskStatus.CANCELLED
    assert result.error_code == ERROR_CANCELLED


@pytest.mark.parametrize(
    ("cancelled", "timed_out", "expected_status", "expected_error"),
    [
        (True, False, TaskStatus.CANCELLED, ERROR_CANCELLED),
        (False, True, TaskStatus.TIMED_OUT, ERROR_TIMEOUT),
    ],
)
def test_terminated_zero_exit_is_normalized(
    workspace: Path,
    project: Path,
    cancelled: bool,
    timed_out: bool,
    expected_status: TaskStatus,
    expected_error: str,
) -> None:
    outcome = ProcessOutcome(
        exit_code=0,
        stdout="done\n",
        stderr="",
        timed_out=timed_out,
        cancelled=cancelled,
        duration_seconds=0.01,
    )
    service = make_service(
        workspace,
        script("pass"),
        process_manager=StubProcessManager(outcome),
    )

    result = service.execute(make_request(project))

    assert result.status is expected_status
    assert result.error_code == expected_error
    assert result.exit_code is None
    assert result.stdout == "done\n"


def test_instruction_is_one_argv_element_not_shell_text(workspace: Path, project: Path) -> None:
    instruction = '"; echo PWNED & echo SECOND; "'
    executor = ArgvExecutor(
        (PYTHON, "-c", "import sys; print(sys.argv[1])", "{instruction}")
    )
    service = make_service(workspace, executor)

    result = service.execute(make_request(project, instruction=instruction))

    assert result.status is TaskStatus.SUCCEEDED
    assert result.stdout == instruction + "\n"


def test_adapter_emits_terminal_event_when_executor_missing(project: Path) -> None:
    events = []
    adapter = ProcessAgentAdapter("missing", None)

    result = adapter.run(make_request(project), on_event=events.append)

    assert result.error_code == ERROR_EXECUTOR_MISSING
    assert [(event.sequence, event.status) for event in events] == [
        (0, TaskStatus.FAILED)
    ]


def test_adapter_emits_terminal_event_on_executor_error(project: Path) -> None:
    events = []
    adapter = ProcessAgentAdapter("broken", RaisingExecutor())

    result = adapter.run(make_request(project), on_event=events.append)

    assert result.error_code == ERROR_EXECUTOR_ERROR
    assert [(event.sequence, event.status) for event in events] == [
        (0, TaskStatus.FAILED)
    ]


def test_adapter_emits_terminal_event_when_cancelled_before_dispatch(project: Path) -> None:
    cancel_event = threading.Event()
    cancel_event.set()
    events = []
    adapter = ProcessAgentAdapter("test-provider", script("print('never')"))

    result = adapter.run(
        make_request(project),
        on_event=events.append,
        cancel_event=cancel_event,
    )

    assert result.status is TaskStatus.CANCELLED
    assert result.error_code == ERROR_CANCELLED
    assert [(event.sequence, event.status) for event in events] == [
        (0, TaskStatus.CANCELLED)
    ]


@pytest.mark.parametrize("bad_timeout", [float("nan"), float("inf"), float("-inf")])
def test_process_manager_rejects_non_finite_timeouts(
    project: Path, bad_timeout: float
) -> None:
    with pytest.raises(ValueError, match="finite"):
        ProcessManager().run(
            [PYTHON, "-c", "pass"],
            cwd=project,
            timeout=bad_timeout,
        )


def test_process_manager_rejects_command_strings(project: Path) -> None:
    manager = ProcessManager()

    with pytest.raises(TypeError):
        manager.run("echo unsafe", cwd=project, timeout=5)  # type: ignore[arg-type]


def test_provider_registry_resolves_logical_roles(workspace: Path) -> None:
    adapter = ProcessAgentAdapter("test-provider", script("pass"), roles=("default", "reviewer"))
    registry = ProviderRegistry()
    registry.register("default", adapter)
    registry.register("reviewer", adapter)

    assert registry.resolve("default") is adapter
    assert registry.roles() == ("default", "reviewer")
    assert registry.capabilities()["reviewer"].supports_cancel
    with pytest.raises(ValueError):
        registry.register("default", adapter)
    with pytest.raises(ProviderNotFoundError):
        registry.resolve("missing")
