"""Runtime adapter for streamed Codex app-server turns.

The web app consumes only normalized text deltas and a RuntimeResult. The
official SDK and its generated protocol types stay isolated in this module so
the legacy `codex exec` path can remain a controlled startup fallback.
"""

from __future__ import annotations

import importlib.metadata
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Mapping, Sequence


TextDeltaCallback = Callable[[str], None]
ThreadStartedCallback = Callable[[str], None]


class RuntimeUnavailable(RuntimeError):
    """The app-server could not start before a turn was submitted."""


@dataclass(slots=True)
class RuntimeRequest:
    project_path: Path
    mode: str
    prompt: str
    model: str
    reasoning_effort: str
    speed: str
    environment: Mapping[str, str]
    attachments: Sequence[Mapping[str, object]] = field(default_factory=tuple)
    thread_id: str | None = None
    ephemeral: bool = False
    unrestricted_write: bool = False
    timeout_seconds: int = 900
    max_output_chars: int = 2_000_000


@dataclass(slots=True)
class RuntimeResult:
    ok: bool
    cancelled: bool
    returncode: int | None
    output: str
    thread_id: str | None
    error: str
    duration_seconds: float

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "cancelled": self.cancelled,
            "returncode": self.returncode,
            "output": self.output,
            "thread_id": self.thread_id,
            "error": self.error,
            "duration_seconds": self.duration_seconds,
        }


def runtime_diagnostics() -> dict[str, object]:
    try:
        sdk_version = importlib.metadata.version("openai-codex")
        cli_version = importlib.metadata.version("openai-codex-cli-bin")
    except importlib.metadata.PackageNotFoundError:
        return {
            "name": "app-server",
            "available": False,
            "sdk_version": None,
            "cli_version": None,
        }
    return {
        "name": "app-server",
        "available": True,
        "sdk_version": sdk_version,
        "cli_version": cli_version,
    }


def _isolated_launch_args(
    environment: Mapping[str, str],
    speed: str,
) -> tuple[str, ...]:
    try:
        from codex_cli_bin import bundled_codex_path
    except ImportError as exc:
        raise RuntimeUnavailable("未安装 Codex app-server 运行时") from exc

    codex_bin = Path(bundled_codex_path())
    if not codex_bin.is_file():
        raise RuntimeUnavailable("Codex app-server 可执行文件不存在")

    # openai-codex currently extends os.environ when launching. `env -i`
    # preserves the existing per-job allowlist and prevents web/API secrets or
    # another LifeOS job envelope from leaking into this subprocess.
    args = ["/usr/bin/env", "-i"]
    for key, value in sorted(environment.items()):
        if "\x00" in key or "\x00" in str(value) or "=" in key:
            raise RuntimeUnavailable("Codex 子进程环境变量无效")
        args.append(f"{key}={value}")
    args.extend(
        [
            str(codex_bin),
            "--enable" if speed == "fast" else "--disable",
            "fast_mode",
            "app-server",
            "--listen",
            "stdio://",
        ]
    )
    return tuple(args)


def _sandbox_for(request: RuntimeRequest):
    from openai_codex import Sandbox

    if request.mode == "read":
        return Sandbox.read_only
    if request.unrestricted_write:
        return Sandbox.full_access
    return Sandbox.workspace_write


def _turn_error_text(turn) -> str:
    error = getattr(turn, "error", None)
    if error is None:
        return ""
    return str(getattr(error, "message", "") or error)


def run_app_server(
    request: RuntimeRequest,
    *,
    on_delta: TextDeltaCallback | None = None,
    on_thread_started: ThreadStartedCallback | None = None,
    cancel_event: threading.Event | None = None,
) -> RuntimeResult:
    started_at = time.monotonic()
    if cancel_event is not None and cancel_event.is_set():
        return RuntimeResult(
            ok=False,
            cancelled=True,
            returncode=None,
            output="",
            thread_id=request.thread_id,
            error="用户已停止",
            duration_seconds=0.0,
        )

    try:
        from openai_codex import (
            ApprovalMode,
            Codex,
            CodexConfig,
            LocalImageInput,
            TextInput,
        )
    except ImportError as exc:
        raise RuntimeUnavailable("未安装 Codex app-server Python SDK") from exc

    config = CodexConfig(
        launch_args_override=_isolated_launch_args(
            request.environment,
            request.speed,
        ),
        cwd=str(request.project_path),
    )
    codex = None
    turn_started = False
    output_parts: list[str] = []
    output_chars = 0
    final_messages: list[str] = []
    resolved_thread_id = request.thread_id
    completed_status = ""
    completed_error = ""
    control_done = threading.Event()
    control_state = {"cancelled": False, "timed_out": False}
    monitor = None

    try:
        try:
            codex = Codex(config)
            sandbox = _sandbox_for(request)
            thread_options = {
                "approval_mode": ApprovalMode.deny_all,
                "cwd": str(request.project_path),
                "model": request.model,
                "sandbox": sandbox,
                "service_tier": (
                    "priority" if request.speed == "fast" else "default"
                ),
            }
            if request.thread_id:
                thread = codex.thread_resume(
                    request.thread_id,
                    **thread_options,
                )
            else:
                thread = codex.thread_start(
                    ephemeral=request.ephemeral,
                    **thread_options,
                )
            resolved_thread_id = str(thread.id)
            if on_thread_started:
                on_thread_started(resolved_thread_id)
        except Exception as exc:
            if codex is not None:
                codex.close()
            raise RuntimeUnavailable(str(exc)) from exc

        inputs = [TextInput(request.prompt)]
        for attachment in request.attachments:
            if attachment.get("kind") == "image" and attachment.get("path"):
                inputs.append(LocalImageInput(str(attachment["path"])))

        turn = thread.turn(
            inputs,
            approval_mode=ApprovalMode.deny_all,
            cwd=str(request.project_path),
            effort=request.reasoning_effort,
            model=request.model,
            sandbox=sandbox,
            service_tier=(
                "priority" if request.speed == "fast" else "default"
            ),
        )
        turn_started = True

        def monitor_turn() -> None:
            deadline = started_at + max(1, int(request.timeout_seconds))
            while not control_done.is_set():
                if cancel_event is not None and cancel_event.wait(0.1):
                    control_state["cancelled"] = True
                    break
                if time.monotonic() >= deadline:
                    control_state["timed_out"] = True
                    break
                control_done.wait(0.1)
            if control_done.is_set():
                return
            try:
                turn.interrupt()
            except Exception:
                pass
            if not control_done.wait(5):
                try:
                    codex.close()
                except Exception:
                    pass

        monitor = threading.Thread(
            target=monitor_turn,
            name="codex-app-server-control",
            daemon=True,
        )
        monitor.start()

        try:
            for event in turn.stream():
                if event.method == "item/agentMessage/delta":
                    delta = str(getattr(event.payload, "delta", "") or "")
                    if delta:
                        remaining = request.max_output_chars - output_chars
                        if remaining > 0:
                            accepted = delta[:remaining]
                            output_parts.append(accepted)
                            output_chars += len(accepted)
                            if on_delta:
                                on_delta(accepted)
                    continue
                if event.method == "item/completed":
                    item = getattr(event.payload, "item", None)
                    root = getattr(item, "root", item)
                    if getattr(root, "type", None) == "agentMessage":
                        text = str(getattr(root, "text", "") or "")
                        if text:
                            final_messages.append(text)
                    continue
                if event.method == "turn/completed":
                    completed = event.payload.turn
                    completed_status = str(
                        getattr(getattr(completed, "status", None), "value", "")
                    )
                    completed_error = _turn_error_text(completed)
        except Exception as exc:
            if not control_state["cancelled"] and not control_state["timed_out"]:
                completed_error = str(exc)
                completed_status = "failed"

        output = (
            final_messages[-1]
            if final_messages
            else "".join(output_parts)
        )[: request.max_output_chars]
        cancelled = bool(control_state["cancelled"])
        timed_out = bool(control_state["timed_out"])
        ok = completed_status == "completed" and not cancelled and not timed_out
        if cancelled:
            error = "用户已停止"
        elif timed_out:
            error = "Codex 执行超时"
        else:
            error = completed_error
        return RuntimeResult(
            ok=ok,
            cancelled=cancelled,
            returncode=0 if ok else None,
            output=output,
            thread_id=resolved_thread_id,
            error=error,
            duration_seconds=round(time.monotonic() - started_at, 1),
        )
    except RuntimeUnavailable:
        raise
    except Exception as exc:
        if not turn_started:
            raise RuntimeUnavailable(str(exc)) from exc
        return RuntimeResult(
            ok=False,
            cancelled=bool(control_state["cancelled"]),
            returncode=None,
            output="".join(output_parts)[: request.max_output_chars],
            thread_id=resolved_thread_id,
            error=str(exc),
            duration_seconds=round(time.monotonic() - started_at, 1),
        )
    finally:
        control_done.set()
        if monitor is not None and monitor.is_alive():
            monitor.join(timeout=0.3)
        if codex is not None:
            try:
                codex.close()
            except Exception:
                pass
