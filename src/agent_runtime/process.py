"""Shell-free child process execution with capture, timeout, and cancellation."""

from __future__ import annotations

import contextlib
import math
import os
import signal
import subprocess
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from agent_runtime.errors import ERROR_ABNORMAL_EXIT, ERROR_NONZERO_EXIT

_READ_CHUNK = 4096
_KILL_GRACE_SECONDS = 5.0
_TASKKILL_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True, slots=True)
class ProcessOutcome:
    """Raw outcome of one child process, before task-level classification."""

    exit_code: int | None
    stdout: str
    stderr: str
    timed_out: bool
    cancelled: bool
    duration_seconds: float


class ProcessStartError(RuntimeError):
    """Raised when the operating system refuses to start the child process."""


def classify_exit_code(exit_code: int | None) -> str:
    """Return ``ok``, ``nonzero_exit``, or ``abnormal_exit`` for an exit code.

    Negative codes mean signal termination on POSIX. Windows reports NTSTATUS
    crashes (for example ``0xC0000005``) as values above 255, so both are
    treated as abnormal rather than ordinary application failures.
    """

    if exit_code == 0:
        return "ok"
    if exit_code is None or exit_code < 0 or exit_code > 255:
        return ERROR_ABNORMAL_EXIT
    return ERROR_NONZERO_EXIT


class ProcessManager:
    """Runs an argv array without a shell and terminates the whole process tree.

    The child is started with an explicit working directory and pipe-backed
    stdout/stderr read by dedicated threads, so large output cannot deadlock the
    wait loop. User instructions must already be individual argv elements; this
    class never joins them into a command string.
    """

    def __init__(
        self,
        *,
        poll_interval: float = 0.05,
        terminate_grace_seconds: float = 2.0,
    ) -> None:
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        if terminate_grace_seconds <= 0:
            raise ValueError("terminate_grace_seconds must be positive")
        self._poll_interval = poll_interval
        self._terminate_grace_seconds = terminate_grace_seconds

    def run(
        self,
        argv: Sequence[str],
        cwd: Path,
        timeout: float,
        cancel_event: threading.Event | None = None,
    ) -> ProcessOutcome:
        """Execute ``argv`` in ``cwd`` and return its captured outcome.

        A cancellation that is already requested prevents the process from
        starting. Timeout and cancellation both terminate the process tree and
        still return whatever output was captured.
        """

        args = self._validate_argv(argv)
        working_dir = Path(cwd)
        if not working_dir.is_absolute():
            raise ValueError("cwd must be an absolute path")
        if not working_dir.is_dir():
            raise ValueError(f"cwd is not an existing directory: {working_dir}")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be a positive finite number")

        if cancel_event is not None and cancel_event.is_set():
            return ProcessOutcome(
                exit_code=None,
                stdout="",
                stderr="",
                timed_out=False,
                cancelled=True,
                duration_seconds=0.0,
            )

        started = time.monotonic()
        try:
            process = subprocess.Popen(  # noqa: S603 - argv array, shell disabled
                args,
                cwd=str(working_dir),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                text=True,
                encoding="utf-8",
                errors="replace",
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
                start_new_session=os.name != "nt",
            )
        except OSError as exc:
            raise ProcessStartError(str(exc)) from exc

        stdout_parts: list[str] = []
        stderr_parts: list[str] = []
        readers = [
            threading.Thread(
                target=self._read_stream,
                args=(process.stdout, stdout_parts),
                daemon=True,
            ),
            threading.Thread(
                target=self._read_stream,
                args=(process.stderr, stderr_parts),
                daemon=True,
            ),
        ]
        for reader in readers:
            reader.start()

        timed_out = False
        cancelled = False
        deadline = started + timeout

        while True:
            if process.poll() is not None:
                break
            if cancel_event is not None and cancel_event.is_set():
                cancelled = True
                self._terminate_tree(process)
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                self._terminate_tree(process)
                break
            try:
                process.wait(timeout=min(self._poll_interval, remaining))
                break
            except subprocess.TimeoutExpired:
                continue

        self._reap(process)
        for reader in readers:
            reader.join(timeout=_KILL_GRACE_SECONDS)

        exit_code = process.returncode
        if (timed_out or cancelled) and exit_code == 0:
            # The process may have exited cleanly in the same poll window in
            # which cancellation/timeout was observed. A terminal non-success
            # result must not claim exit code 0.
            exit_code = None

        return ProcessOutcome(
            exit_code=exit_code,
            stdout="".join(stdout_parts),
            stderr="".join(stderr_parts),
            timed_out=timed_out,
            cancelled=cancelled,
            duration_seconds=time.monotonic() - started,
        )

    @staticmethod
    def _validate_argv(argv: Sequence[str]) -> list[str]:
        if isinstance(argv, (str, bytes)):
            raise TypeError("argv must be a sequence of strings, not a command string")
        args = list(argv)
        if not args:
            raise ValueError("argv must not be empty")
        if not args[0]:
            raise ValueError("argv[0] must not be empty")
        for item in args:
            if not isinstance(item, str):
                raise TypeError("every argv element must be a string")
            if "\x00" in item:
                raise ValueError("argv elements must not contain NUL characters")
        return args

    @staticmethod
    def _read_stream(stream: TextIO | None, sink: list[str]) -> None:
        if stream is None:
            return
        try:
            while chunk := stream.read(_READ_CHUNK):
                sink.append(chunk)
        finally:
            stream.close()

    def _reap(self, process: subprocess.Popen[str]) -> None:
        try:
            process.wait(timeout=self._terminate_grace_seconds)
        except subprocess.TimeoutExpired:
            self._kill_tree(process)
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=_KILL_GRACE_SECONDS)

    def _terminate_tree(self, process: subprocess.Popen[str]) -> None:
        if process.poll() is not None:
            return
        if os.name == "nt":
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                subprocess.run(  # noqa: S603 - fixed taskkill argv, no shell
                    ["taskkill", "/F", "/T", "/PID", str(process.pid)],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                    timeout=_TASKKILL_TIMEOUT_SECONDS,
                )
            if process.poll() is None:
                with contextlib.suppress(OSError):
                    process.kill()
        else:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                with contextlib.suppress(OSError):
                    process.terminate()

    @staticmethod
    def _kill_tree(process: subprocess.Popen[str]) -> None:
        if process.poll() is not None:
            return
        if os.name == "nt":
            with contextlib.suppress(OSError):
                process.kill()
        else:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                with contextlib.suppress(OSError):
                    process.kill()
