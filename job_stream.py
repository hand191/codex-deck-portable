"""Small in-process stream hub for live Codex job output.

The database remains the source of truth for terminal job state and messages.
This hub only carries transient text while the current service process is
running. A reconnecting subscriber always receives a full snapshot first.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class JobStreamSnapshot:
    job_id: str
    text: str
    revision: int
    status: str
    terminal: bool
    updated_at: float


class _JobStreamState:
    def __init__(self, job_id: str, status: str):
        self.job_id = job_id
        self.text = ""
        self.revision = 0
        self.status = status
        self.terminal = status in {"completed", "failed", "cancelled"}
        self.updated_at = time.monotonic()
        self.condition = threading.Condition()

    def snapshot(self) -> JobStreamSnapshot:
        return JobStreamSnapshot(
            job_id=self.job_id,
            text=self.text,
            revision=self.revision,
            status=self.status,
            terminal=self.terminal,
            updated_at=self.updated_at,
        )


class JobStreamHub:
    """Thread-safe snapshots and wakeups for a small number of live jobs."""

    def __init__(self, max_output_chars: int, terminal_ttl_seconds: int = 300):
        self.max_output_chars = max(1, int(max_output_chars))
        self.terminal_ttl_seconds = max(30, int(terminal_ttl_seconds))
        self._lock = threading.Lock()
        self._states: dict[str, _JobStreamState] = {}

    def _cleanup_locked(self) -> None:
        cutoff = time.monotonic() - self.terminal_ttl_seconds
        stale = [
            job_id
            for job_id, state in self._states.items()
            if state.terminal and state.updated_at < cutoff
        ]
        for job_id in stale:
            self._states.pop(job_id, None)

    def ensure(self, job_id: str, status: str = "queued") -> JobStreamSnapshot:
        job_id = str(job_id)
        with self._lock:
            self._cleanup_locked()
            state = self._states.get(job_id)
            if state is None:
                state = _JobStreamState(job_id, status)
                self._states[job_id] = state
        return state.snapshot()

    def set_status(self, job_id: str, status: str) -> JobStreamSnapshot:
        job_id = str(job_id)
        self.ensure(job_id, status)
        with self._lock:
            state = self._states[job_id]
        with state.condition:
            if state.status != status:
                state.status = status
                state.revision += 1
            state.terminal = status in {"completed", "failed", "cancelled"}
            state.updated_at = time.monotonic()
            state.condition.notify_all()
            return state.snapshot()

    def append(self, job_id: str, text: str) -> JobStreamSnapshot:
        delta = str(text or "")
        if not delta:
            return self.ensure(job_id, "running")
        job_id = str(job_id)
        self.ensure(job_id, "running")
        with self._lock:
            state = self._states[job_id]
        with state.condition:
            remaining = self.max_output_chars - len(state.text)
            if remaining > 0:
                state.text += delta[:remaining]
                state.revision += 1
                state.status = "running"
                state.updated_at = time.monotonic()
                state.condition.notify_all()
            return state.snapshot()

    def finish(self, job_id: str, status: str) -> JobStreamSnapshot:
        return self.set_status(job_id, status)

    def snapshot(
        self,
        job_id: str,
        default_status: str = "queued",
    ) -> JobStreamSnapshot:
        return self.ensure(job_id, default_status)

    def wait(
        self,
        job_id: str,
        after_revision: int,
        timeout: float,
        default_status: str = "queued",
    ) -> JobStreamSnapshot:
        job_id = str(job_id)
        self.ensure(job_id, default_status)
        with self._lock:
            state = self._states[job_id]
        with state.condition:
            if state.revision <= int(after_revision) and not state.terminal:
                state.condition.wait(max(0.0, float(timeout)))
            return state.snapshot()
