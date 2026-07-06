"""
Persistent install progress tracking.

``cephtools testenv install`` is normally driven over a single SSH pipe from a
CI runner (see ``cephtools testflinger deploy``). When that pipe breaks -- a
runner timeout, a network blip, an OOM kill -- the only copy of the output is
lost. This module leaves breadcrumbs *on the host* so a dead session still
tells you how far the install got and where it stopped.

Two artifacts live under the cephtools state directory (``$CEPHTOOLS_STATE_HOME``
or ``~/src/cephtools/state``):

* ``install.log``       -- append-only, timestamped transcript of every emitted
                           line (mirrors stdout so nothing is lost).
* ``install-state.json`` -- atomically-rewritten checkpoint of the current
                            step/sub-operation, with an ``updated`` timestamp.
                            A stale timestamp on a non-``complete`` record is
                            the single fastest way to spot a hang.

Every function here is best-effort: logging must never break the install.
"""

from __future__ import annotations

import atexit
import contextlib
import datetime
import json
import os
import signal
from typing import Iterator

from cephtools.state import get_state_file

INSTALL_LOG_NAME = "install.log"
INSTALL_STATE_NAME = "install-state.json"

# Module-level completion flag so the atexit hook can distinguish a clean
# finish from an abnormal exit without inspecting the JSON file.
_COMPLETED = False


def _utc() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def install_log_path():
    """Path to the persistent install transcript."""
    return get_state_file(INSTALL_LOG_NAME)


def install_state_path():
    """Path to the atomic step checkpoint."""
    return get_state_file(INSTALL_STATE_NAME)


def _append_log(line: str) -> None:
    try:
        path = install_log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(line.rstrip("\n") + "\n")
    except OSError:
        pass


def emit(message: str) -> None:
    """Echo a timestamped line to stdout *and* the persistent install log.

    Use this for any progress line worth keeping after a disconnect. Never
    raises.
    """
    line = f"{_utc()} {message}"
    try:
        print(line, flush=True)
    except Exception:  # pragma: no cover - stdout can be a closed pipe
        pass
    _append_log(line)


def checkpoint(
    step: str,
    sub: str | None = None,
    *,
    status: str = "running",
    detail: str | None = None,
) -> None:
    """Atomically rewrite ``install-state.json``.

    A hang is diagnosed by reading this file: a ``running`` record whose
    ``updated`` timestamp is minutes stale points at the stuck operation.
    """
    record = {
        "step": step,
        "sub": sub,
        "status": status,
        "detail": detail,
        "updated": _utc(),
        "pid": os.getpid(),
    }
    try:
        path = install_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(record, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, path)
    except OSError:
        pass


def mark_complete() -> None:
    """Record a successful end of install (disarms the atexit fault marker)."""
    global _COMPLETED
    _COMPLETED = True
    checkpoint("done", status="complete")


def mark_failed(step: str, sub: str | None = None, detail: str | None = None) -> None:
    """Record a failure at the given step/sub."""
    global _COMPLETED
    _COMPLETED = True  # a deliberate failure is not "abnormal exit"
    checkpoint(step, sub, status="failed", detail=detail)


@contextlib.contextmanager
def operation(step: str, sub: str, *, detail: str | None = None) -> Iterator[None]:
    """Context manager that marks a sub-operation running, then done/failed.

    Usage::

        with operation("3/7", "maas-init:_ensure_maas_postgres"):
            _ensure_maas_postgres(admin_pw)

    On exception the checkpoint records ``failed`` with the error type/message
    (truncated) before re-raising, so a crash mid-step is visible in
    ``install-state.json`` without inspecting the log.
    """
    checkpoint(step, sub, status="running", detail=detail)
    suffix = f" ({detail})" if detail else ""
    emit(f"[{step}] {sub}: start{suffix}")
    try:
        yield
    except BaseException as exc:  # includes ClickException, KeyboardInterrupt
        msg = f"{type(exc).__name__}: {exc}"[:500]
        mark_failed(step, sub, detail=msg)
        emit(f"[{step}] {sub}: FAILED ({msg})")
        raise
    checkpoint(step, sub, status="done")
    emit(f"[{step}] {sub}: done")


def install_fault_handlers(step: str = "install") -> None:
    """Record a crash marker on SIGTERM/SIGINT/atexit.

    A CI runner that hits ``timeout-minutes`` sends SIGTERM. Without this the
    ``install-state.json`` would still claim ``running`` forever; with it the
    record shows ``failed`` with ``detail=terminated by SIGTERM``.
    """
    global _COMPLETED

    def _signal_handler(signum, _frame):
        if _COMPLETED:
            # Already done/failed; restore default disposition and re-raise so
            # the process actually dies and the runner's kill propagates.
            signal.signal(signum, signal.SIG_DFL)
            os._exit(128 + int(signum))
        try:
            name = signal.Signals(signum).name
        except ValueError:
            name = f"signal {signum}"
        mark_failed(step, detail=f"terminated by {name}")
        # Record is written; now let the process die so the SSH pipe closes
        # and the runner's timeout actually terminates the job.
        signal.signal(signum, signal.SIG_DFL)
        os._exit(128 + int(signum))

    def _atexit_handler():
        if _COMPLETED:
            return
        mark_failed(step, detail="process exited without marking complete")

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _signal_handler)
        except (ValueError, OSError):
            # Not in the main thread (e.g. invoked from a test); skip.
            pass
    atexit.register(_atexit_handler)
