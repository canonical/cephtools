"""Detached, reconnectable jobs running on a prepared test environment."""

from __future__ import annotations

import datetime as dt
import fcntl
import io
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Sequence

import click


PROTOCOL_VERSION = 1
DEFAULT_RUN_ROOT = "/home/ubuntu/.local/state/cephtools/jobs"
DEFAULT_LOCK_FILE = "/run/lock/cephtools-testenv-job.lock"
UNIT_PREFIX = "cephtools-testenv-job-"
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,159}$")
TARGET_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*@[A-Za-z0-9][A-Za-z0-9_.-]*$")
FINAL_STATES = {"finished", "terminated"}
NONFINAL_STATES = {"launching", "running"}
REMOTE_PREFIX = ("cephtools", "testenv", "job")
STOP_STATUS_MARGIN_SECONDS = 5
MIN_STOP_TIMEOUT_SECONDS = STOP_STATUS_MARGIN_SECONDS + 5


class RemoteLifecycleError(click.ClickException):
    """A successful SSH call reported a definitive remote lifecycle failure."""


@dataclass(frozen=True)
class JobPaths:
    run_id: str
    run_root: Path
    run_dir: Path
    status_file: Path
    log_file: Path
    unit: str

    @property
    def status(self) -> Path:
        return self.status_file

    @property
    def log(self) -> Path:
        return self.log_file


def validate_run_id(value: str) -> str:
    if not RUN_ID_RE.fullmatch(value):
        raise click.BadParameter(
            "must be 1-160 ASCII letters, digits, underscores, or hyphens, "
            "and start with a letter or digit"
        )
    return value


def validate_target(value: str) -> str:
    if not TARGET_RE.fullmatch(value):
        raise click.BadParameter("must be a conservative USER@HOST SSH target")
    return value


def validate_absolute_path(
    value: str, *, label: str | None = None, name: str | None = None
) -> Path:
    description = label or name or "path"
    candidate = PurePosixPath(value)
    if (
        not candidate.is_absolute()
        or ".." in candidate.parts
        or value != str(candidate)
        or not re.fullmatch(r"/[A-Za-z0-9._/-]+", value)
    ):
        raise click.BadParameter(f"{description} must be a normalized absolute path")
    return Path(str(candidate))


def validate_relative_path(value: str) -> PurePosixPath:
    candidate = PurePosixPath(value)
    if (
        not value
        or candidate.is_absolute()
        or ".." in candidate.parts
        or candidate == PurePosixPath(".")
        or value != str(candidate)
        or any(not re.fullmatch(r"[A-Za-z0-9._-]+", part) for part in candidate.parts)
    ):
        raise click.BadParameter(
            "staged destination must be a non-empty relative path without '..'"
        )
    return candidate


def derive_paths(run_root: str, run_id: str) -> JobPaths:
    run_id = validate_run_id(run_id)
    root = validate_absolute_path(run_root, name="run root")
    unit = f"{UNIT_PREFIX}{run_id}.service"
    if len(unit.encode()) > 255:  # defensive if the prefix changes
        raise click.BadParameter("derived systemd unit name is too long")
    run_dir = root / run_id
    return JobPaths(
        run_id, root, run_dir, run_dir / "status.json", run_dir / "run.log", unit
    )


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(data, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _status_document(
    paths: JobPaths,
    state: str,
    *,
    exit_code: int | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
    child_pid: int | None = None,
    command: Sequence[str] = (),
    termination_signal: int | None = None,
    message: str | None = None,
) -> dict[str, Any]:
    document: dict[str, Any] = {
        "protocol": PROTOCOL_VERSION,
        "run_id": paths.run_id,
        "unit": paths.unit,
        "state": state,
        "updated_at": _utc_now(),
    }
    optional = {
        "exit_code": exit_code,
        "started_at": started_at,
        "finished_at": finished_at,
        "child_pid": child_pid,
        "process_group": child_pid,
        "argv": list(command) if command else None,
        "termination_signal": termination_signal,
        "message": message,
    }
    document.update(
        {key: value for key, value in optional.items() if value is not None}
    )
    return document


def _read_status(paths: JobPaths) -> dict[str, Any] | None:
    try:
        value = json.loads(paths.status.read_text())
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        raise click.ClickException(
            f"Invalid durable status {paths.status}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise click.ClickException(
            f"Invalid durable status {paths.status}: expected object"
        )
    return value


def _is_final_status(
    status: dict[str, Any] | None, paths: JobPaths | None = None
) -> bool:
    if not status:
        return False
    exit_code = status.get("exit_code")
    identity_matches = paths is None or (
        status.get("run_id") == paths.run_id and status.get("unit") == paths.unit
    )
    return bool(
        status.get("protocol") == PROTOCOL_VERSION
        and identity_matches
        and status.get("state") in FINAL_STATES
        and isinstance(exit_code, int)
        and not isinstance(exit_code, bool)
        and 0 <= exit_code <= 255
    )


def _run_process(
    argv: Sequence[str],
    *,
    input_data: bytes | None = None,
    timeout: float | None = None,
    check: bool = False,
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        list(argv),
        input=input_data,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=check,
    )


def _remote_command(target: str, remote_argv: Sequence[str]) -> list[str]:
    validate_target(target)
    return ["ssh", target, shlex.join(remote_argv)]


def run_remote(
    target: str,
    arguments: Sequence[str],
    *,
    input_bytes: bytes | None = None,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[Any]:
    return _run_process(
        _remote_command(target, arguments), input_data=input_bytes, timeout=timeout
    )


def _remote_agent(
    target: str,
    command: str,
    args: Sequence[str] = (),
    *,
    input_data: bytes | None = None,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[bytes]:
    return run_remote(
        target,
        [*REMOTE_PREFIX, command, *args],
        input_bytes=input_data,
        timeout=timeout,
    )


def _text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    return value.decode(errors="replace") if isinstance(value, bytes) else value


def _process_error(
    action: str, result: subprocess.CompletedProcess[Any]
) -> click.ClickException:
    detail = _text(result.stderr).strip() or _text(result.stdout).strip()
    return click.ClickException(f"{action} failed ({result.returncode}): {detail}")


def _remote_agent_or_error(
    target: str,
    command: str,
    args: Sequence[str],
    *,
    action: str,
    input_data: bytes | None = None,
    timeout: float,
) -> subprocess.CompletedProcess[bytes]:
    try:
        return _remote_agent(
            target,
            command,
            args,
            input_data=input_data,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise click.ClickException(
            f"{action} timed out after {timeout:g} seconds"
        ) from exc


def _check_protocol(target: str) -> None:
    result = _remote_agent_or_error(
        target,
        "protocol",
        (),
        action="remote cephtools job protocol check",
        timeout=25,
    )
    if result.returncode != 0:
        raise _process_error("remote cephtools job protocol check", result)
    remote = _text(result.stdout).strip()
    if remote != str(PROTOCOL_VERSION):
        raise click.ClickException(
            f"incompatible cephtools job protocol: local={PROTOCOL_VERSION}, remote={remote!r}"
        )


def build_stage_archive(stage: Sequence[tuple[str, str]]) -> bytes:
    seen: set[PurePosixPath] = set()
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for local_text, remote_text in stage:
            local = Path(local_text)
            remote = validate_relative_path(remote_text)
            if remote in seen:
                raise click.BadParameter(f"duplicate staged destination: {remote}")
            seen.add(remote)
            if local.is_symlink() or not local.is_file():
                raise click.BadParameter(
                    f"staged source must be a regular non-symlink file: {local}"
                )
            info = archive.gettarinfo(str(local), arcname=str(remote))
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mode = 0o555 if local.stat().st_mode & 0o111 else 0o444
            with local.open("rb") as stream:
                archive.addfile(info, stream)
    return buffer.getvalue()


def _safe_extract_stage(stream: Any, paths: JobPaths) -> None:
    paths.run_root.mkdir(parents=True, exist_ok=True)
    if os.path.lexists(paths.run_dir):
        raise click.ClickException(f"run directory already exists: {paths.run_dir}")

    staging_dir = Path(
        tempfile.mkdtemp(prefix=f".{paths.run_id}.staging-", dir=paths.run_root)
    )
    seen: set[PurePosixPath] = set()
    try:
        with tarfile.open(fileobj=stream, mode="r|*") as archive:
            for member in archive:
                relative = validate_relative_path(member.name)
                if relative in seen:
                    raise click.ClickException(
                        f"duplicate staged destination: {relative}"
                    )
                seen.add(relative)
                if not member.isfile():
                    raise click.ClickException(
                        f"only regular staged files are accepted: {member.name}"
                    )
                destination = staging_dir.joinpath(*relative.parts)
                destination.parent.mkdir(parents=True, exist_ok=True)
                source = archive.extractfile(member)
                if source is None:
                    raise click.ClickException(
                        f"could not read staged file: {member.name}"
                    )
                with destination.open("xb") as output:
                    shutil.copyfileobj(source, output)
                destination.chmod(0o555 if member.mode & 0o111 else 0o444)
        staging_dir.chmod(0o755)
        try:
            staging_dir.rename(paths.run_dir)
        except OSError as exc:
            if os.path.lexists(paths.run_dir):
                raise click.ClickException(
                    f"run directory already exists: {paths.run_dir}"
                ) from exc
            raise
    except click.ClickException:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise
    except (tarfile.TarError, OSError) as exc:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise click.ClickException(f"invalid stage archive: {exc}") from exc
    except BaseException:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise


def extract_stage_archive(paths: JobPaths, payload: bytes) -> None:
    _safe_extract_stage(io.BytesIO(payload), paths)


def _lock_probe(lock_file: Path) -> tuple[bool, str]:
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(lock_file, os.O_CREAT | os.O_RDWR, 0o666)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            try:
                diagnostics = subprocess.run(
                    ["fuser", "-v", str(lock_file)], capture_output=True, text=True
                )
            except OSError:
                return False, "lock is held"
            detail = (diagnostics.stdout + diagnostics.stderr).strip()
            return False, detail or "lock is held"
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            except OSError:
                pass
    finally:
        os.close(descriptor)
    return True, ""


def _ancestor_pids() -> set[int]:
    ancestors = {os.getpid()}
    pid = os.getppid()
    while pid > 1 and pid not in ancestors:
        ancestors.add(pid)
        try:
            fields = Path(f"/proc/{pid}/stat").read_text().split()
            pid = int(fields[3])
        except (OSError, ValueError, IndexError):
            break
    return ancestors


def preflight_host(
    lock_file: Path,
    active_unit_patterns: Sequence[str] = (),
    conflict_processes: Sequence[str] = (),
    *,
    runner: Any = subprocess.run,
) -> None:
    available, detail = _lock_probe(lock_file)
    if not available:
        raise click.ClickException(f"test environment lock is held: {detail}")

    units: list[str] = []
    for pattern in active_unit_patterns:
        result = runner(
            [
                "systemctl",
                "list-units",
                "--state=active,activating,deactivating",
                "--no-legend",
                "--no-pager",
                pattern,
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            detail = _text(result.stderr).strip() or _text(result.stdout).strip()
            raise click.ClickException(
                f"could not check active units matching {pattern!r}: "
                f"{detail or f'exit {result.returncode}'}"
            )
        units.extend(
            line.split()[0]
            for line in _text(result.stdout).splitlines()
            if line.split()
        )

    excluded = _ancestor_pids()
    processes: list[str] = []
    for pattern in conflict_processes:
        try:
            re.compile(pattern)
        except re.error as exc:
            raise click.BadParameter(
                f"invalid conflict process regex {pattern!r}: {exc}"
            ) from exc
        result = runner(["pgrep", "-af", "--", pattern], capture_output=True, text=True)
        if result.returncode not in (0, 1):
            detail = _text(result.stderr).strip() or _text(result.stdout).strip()
            raise click.ClickException(
                f"could not check conflicting processes matching {pattern!r}: "
                f"{detail or f'exit {result.returncode}'}"
            )
        for line in _text(result.stdout).splitlines():
            fields = line.strip().split(maxsplit=1)
            if fields and fields[0].isdigit() and int(fields[0]) not in excluded:
                processes.append(line.strip())

    problems: list[str] = []
    if units:
        problems.append("active conflicting units: " + ", ".join(sorted(set(units))))
    if processes:
        problems.append("conflicting host processes:\n" + "\n".join(processes))
    if problems:
        raise click.ClickException("\n".join(problems))


def _systemd_properties(unit: str, *, runner: Any | None = None) -> dict[str, str]:
    runner = runner or subprocess.run
    result = runner(
        [
            "systemctl",
            "show",
            unit,
            "--property",
            "LoadState,ActiveState,SubState,Result,ExecMainStatus",
            "--no-pager",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return {"LoadState": "unknown", "error": _text(result.stderr).strip()}
    properties: dict[str, str] = {}
    for line in _text(result.stdout).splitlines():
        key, separator, value = line.partition("=")
        if separator:
            properties[key] = value
    return properties


def _write_status(paths: JobPaths, values: dict[str, Any]) -> None:
    document = {
        "protocol": PROTOCOL_VERSION,
        "run_id": paths.run_id,
        "unit": paths.unit,
        "updated_at": _utc_now(),
        **values,
    }
    _atomic_write_json(paths.status_file, document)


def _durable_status_is_valid_nonfinal(
    durable: dict[str, Any] | None, paths: JobPaths
) -> bool:
    if not durable:
        return False
    return bool(
        durable.get("protocol") == PROTOCOL_VERSION
        and durable.get("run_id") == paths.run_id
        and durable.get("unit") == paths.unit
        and durable.get("state") in NONFINAL_STATES
        and "exit_code" not in durable
    )


def read_host_status(
    paths: JobPaths, *, runner: Any = subprocess.run
) -> dict[str, Any]:
    lifecycle_error: dict[str, str] | None = None
    try:
        durable = _read_status(paths)
    except click.ClickException as exc:
        durable = None
        lifecycle_error = {
            "kind": "invalid-durable-status",
            "message": str(exc),
        }
    systemd = _systemd_properties(paths.unit, runner=runner)
    if (
        lifecycle_error is None
        and durable is not None
        and not (
            _is_final_status(durable, paths)
            or _durable_status_is_valid_nonfinal(durable, paths)
        )
    ):
        lifecycle_error = {
            "kind": "invalid-durable-status",
            "message": f"{paths.status} has invalid identity or state",
        }
    if lifecycle_error is None and not _is_final_status(durable, paths):
        load_state = systemd.get("LoadState")
        active_state = systemd.get("ActiveState")
        if load_state in {None, "", "unknown"}:
            lifecycle_error = {
                "kind": "systemd-query-failed",
                "message": systemd.get("error") or "systemd state is unavailable",
            }
        elif load_state == "not-found":
            lifecycle_error = {
                "kind": "unit-missing-without-final-status",
                "message": (
                    f"{paths.unit} is missing and no authoritative final status exists"
                ),
            }
        elif active_state in {"failed", "inactive"}:
            lifecycle_error = {
                "kind": "unit-inactive-without-final-status",
                "message": (
                    f"{paths.unit} is not active and no authoritative final status exists"
                ),
            }
    response: dict[str, Any] = {
        "protocol": PROTOCOL_VERSION,
        "run_id": paths.run_id,
        "unit": paths.unit,
        "status": durable or {"state": "pending"},
        "systemd": systemd,
    }
    if lifecycle_error is not None:
        response["lifecycle_error"] = lifecycle_error
    return response


def _host_status(paths: JobPaths) -> dict[str, Any]:
    return read_host_status(paths)


def _normalize_exit_code(returncode: int) -> int:
    return 128 + abs(returncode) if returncode < 0 else returncode


def _supervise(
    paths: JobPaths, lock_file: Path, command: Sequence[str], kill_after: float
) -> int:
    if not command:
        raise click.ClickException("payload command is required")
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    lock_descriptor = os.open(lock_file, os.O_CREAT | os.O_RDWR, 0o666)
    started_at = _utc_now()
    try:
        try:
            fcntl.flock(lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            status = _status_document(
                paths,
                "finished",
                exit_code=75,
                started_at=started_at,
                finished_at=_utc_now(),
                command=command,
                message="test environment lock is held",
            )
            _atomic_write_json(paths.status, status)
            return 75

        if not paths.run_dir.is_dir():
            raise click.ClickException(
                f"staged run directory is missing: {paths.run_dir}"
            )
        terminated_by: int | None = None
        child: subprocess.Popen[Any] | None = None

        def terminate(signum: int, _frame: Any) -> None:
            nonlocal terminated_by
            terminated_by = signum
            if child is not None and child.poll() is None:
                try:
                    os.killpg(child.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass

        install_signal_handlers = threading.current_thread() is threading.main_thread()
        previous_term: Any = None
        previous_int: Any = None
        if install_signal_handlers:
            previous_term = signal.signal(signal.SIGTERM, terminate)
            previous_int = signal.signal(signal.SIGINT, terminate)
        try:
            with paths.log.open("w", encoding="utf-8") as log:
                try:
                    child = subprocess.Popen(
                        list(command),
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        start_new_session=True,
                    )
                except OSError as exc:
                    log.write(f"could not start payload: {exc}\n")
                    log.flush()
                    _atomic_write_json(
                        paths.status,
                        _status_document(
                            paths,
                            "finished",
                            exit_code=127,
                            started_at=started_at,
                            finished_at=_utc_now(),
                            command=command,
                            message=str(exc),
                        ),
                    )
                    return 127
                _atomic_write_json(
                    paths.status,
                    _status_document(
                        paths,
                        "running",
                        started_at=started_at,
                        child_pid=child.pid,
                        command=command,
                    ),
                )
                # A stop may arrive in the narrow interval before Popen assigns
                # child. Honour it as soon as the process group exists.
                if terminated_by is not None and child.poll() is None:
                    try:
                        os.killpg(child.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                termination_deadline: float | None = None
                while child.poll() is None:
                    if terminated_by is not None and termination_deadline is None:
                        termination_deadline = time.monotonic() + kill_after
                    if (
                        termination_deadline is not None
                        and time.monotonic() >= termination_deadline
                    ):
                        try:
                            os.killpg(child.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        termination_deadline = None
                    time.sleep(0.1)
                return_code = child.wait()
        finally:
            if install_signal_handlers:
                signal.signal(signal.SIGTERM, previous_term)
                signal.signal(signal.SIGINT, previous_int)

        if terminated_by is not None:
            exit_code = 128 + terminated_by
            state = "terminated"
        else:
            exit_code = _normalize_exit_code(return_code)
            state = "finished"
        _atomic_write_json(
            paths.status,
            _status_document(
                paths,
                state,
                exit_code=exit_code,
                started_at=started_at,
                finished_at=_utc_now(),
                child_pid=child.pid,
                command=command,
                termination_signal=terminated_by,
            ),
        )
        return exit_code
    finally:
        try:
            fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(lock_descriptor)


def run_host_job(
    paths: JobPaths,
    lock_file: Path,
    command: Sequence[str],
    *,
    kill_after: float = 5.0,
) -> int:
    return _supervise(paths, lock_file, command, kill_after)


def _common_remote_args(run_id: str, run_root: str) -> list[str]:
    return ["--run-id", run_id, "--run-root", run_root]


def _decode_json_output(
    result: subprocess.CompletedProcess[bytes], action: str
) -> dict[str, Any]:
    if result.returncode != 0:
        raise _process_error(action, result)
    try:
        value = json.loads(_text(result.stdout))
    except json.JSONDecodeError as exc:
        raise click.ClickException(f"{action} returned invalid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise click.ClickException(f"{action} returned invalid JSON object")
    return value


def _validate_status_response(
    response: dict[str, Any], paths: JobPaths
) -> dict[str, Any]:
    if response.get("protocol") != PROTOCOL_VERSION:
        raise click.ClickException("remote job status has an incompatible protocol")
    if response.get("run_id") != paths.run_id or response.get("unit") != paths.unit:
        raise click.ClickException("remote job status has the wrong run identity")
    lifecycle_error = response.get("lifecycle_error")
    if lifecycle_error is not None:
        if not isinstance(lifecycle_error, dict) or not isinstance(
            lifecycle_error.get("message"), str
        ):
            raise click.ClickException(
                "remote job status has a malformed lifecycle error"
            )
        kind = lifecycle_error.get("kind", "lifecycle-error")
        raise RemoteLifecycleError(f"{kind}: {lifecycle_error['message']}")
    durable = response.get("status")
    if not isinstance(durable, dict):
        raise click.ClickException(
            "remote job status is missing its durable status object"
        )
    if durable == {"state": "pending"}:
        return durable
    if _is_final_status(durable, paths):
        return durable
    if not _durable_status_is_valid_nonfinal(durable, paths):
        raise click.ClickException("remote job status has an invalid durable state")
    return durable


@click.group("job", help="Run reconnectable jobs on a prepared test environment.")
def cli() -> None:
    pass


@cli.command("protocol", help="Print the testenv job protocol version.")
def protocol_cmd() -> None:
    click.echo(PROTOCOL_VERSION)


@cli.command("start", context_settings={"ignore_unknown_options": True})
@click.option("--target", required=True, callback=lambda _c, _p, v: validate_target(v))
@click.option("--run-id", required=True, callback=lambda _c, _p, v: validate_run_id(v))
@click.option("--run-root", default=DEFAULT_RUN_ROOT, show_default=True)
@click.option("--lock-file", default=DEFAULT_LOCK_FILE, show_default=True)
@click.option("--runtime-seconds", required=True, type=click.IntRange(min=1))
@click.option(
    "--stop-timeout-seconds",
    default=300,
    type=click.IntRange(min=MIN_STOP_TIMEOUT_SECONDS),
)
@click.option("--working-directory", default="/home/ubuntu", show_default=True)
@click.option("--stage", nargs=2, multiple=True, metavar="LOCAL REMOTE_RELATIVE")
@click.option("--active-unit-pattern", multiple=True)
@click.option("--conflict-process", multiple=True)
@click.argument("command", nargs=-1, required=True, type=click.UNPROCESSED)
def start_cmd(
    target: str,
    run_id: str,
    run_root: str,
    lock_file: str,
    runtime_seconds: int,
    stop_timeout_seconds: int,
    working_directory: str,
    stage: tuple[tuple[str, str], ...],
    active_unit_pattern: tuple[str, ...],
    conflict_process: tuple[str, ...],
    command: tuple[str, ...],
) -> None:
    paths = derive_paths(run_root, run_id)
    lock = validate_absolute_path(lock_file, name="lock file")
    working = validate_absolute_path(working_directory, name="working directory")
    archive = build_stage_archive(stage)
    _check_protocol(target)

    stage_result = _remote_agent_or_error(
        target,
        "_stage",
        _common_remote_args(run_id, str(paths.run_root)),
        action="remote job staging",
        input_data=archive,
        timeout=60,
    )
    if stage_result.returncode != 0:
        raise _process_error("remote job staging", stage_result)

    preflight_args = [
        *_common_remote_args(run_id, str(paths.run_root)),
        "--lock-file",
        str(lock),
    ]
    for pattern in (f"{UNIT_PREFIX}*.service", *active_unit_pattern):
        preflight_args.extend(("--active-unit-pattern", pattern))
    for pattern in conflict_process:
        preflight_args.extend(("--conflict-process", pattern))
    preflight = _remote_agent_or_error(
        target,
        "_preflight",
        preflight_args,
        action="remote job preflight",
        timeout=25,
    )
    if preflight.returncode != 0:
        raise _process_error("remote job preflight", preflight)

    launch_args = [
        *_common_remote_args(run_id, str(paths.run_root)),
        "--lock-file",
        str(lock),
        "--runtime-seconds",
        str(runtime_seconds),
        "--stop-timeout-seconds",
        str(stop_timeout_seconds),
        "--working-directory",
        str(working),
        "--",
        *command,
    ]
    launch = _remote_agent_or_error(
        target,
        "_start",
        launch_args,
        action="remote systemd launch",
        timeout=25,
    )
    if launch.returncode != 0:
        raise _process_error("remote systemd launch", launch)
    response = _decode_json_output(launch, "remote systemd launch")
    response["target"] = target
    click.echo(json.dumps(response, sort_keys=True))


@cli.command("status")
@click.option("--target", required=True, callback=lambda _c, _p, v: validate_target(v))
@click.option("--run-id", required=True, callback=lambda _c, _p, v: validate_run_id(v))
@click.option("--run-root", default=DEFAULT_RUN_ROOT, show_default=True)
@click.option("--ssh-timeout", default=25.0, type=click.FloatRange(min=0.1))
def status_cmd(target: str, run_id: str, run_root: str, ssh_timeout: float) -> None:
    paths = derive_paths(run_root, run_id)
    result = _remote_agent_or_error(
        target,
        "_status",
        _common_remote_args(run_id, str(paths.run_root)),
        action="remote job status",
        timeout=ssh_timeout,
    )
    response = _decode_json_output(result, "remote job status")
    _validate_status_response(response, paths)
    click.echo(json.dumps(response, sort_keys=True))


@cli.command("wait")
@click.option("--target", required=True, callback=lambda _c, _p, v: validate_target(v))
@click.option("--run-id", required=True, callback=lambda _c, _p, v: validate_run_id(v))
@click.option("--run-root", default=DEFAULT_RUN_ROOT, show_default=True)
@click.option("--poll-interval", default=30.0, type=click.FloatRange(min=0.0))
@click.option("--ssh-timeout", default=25.0, type=click.FloatRange(min=0.1))
@click.option("--max-consecutive-errors", default=10, type=click.IntRange(min=1))
@click.option("--tail-every", default=10, type=click.IntRange(min=1))
@click.option("--tail-lines", default=40, type=click.IntRange(min=1))
def wait_cmd(
    target: str,
    run_id: str,
    run_root: str,
    poll_interval: float,
    ssh_timeout: float,
    max_consecutive_errors: int,
    tail_every: int,
    tail_lines: int,
) -> None:
    paths = derive_paths(run_root, run_id)
    failures = 0
    poll_count = 0
    while True:
        poll_count += 1
        try:
            result = _remote_agent(
                target,
                "_status",
                _common_remote_args(run_id, str(paths.run_root)),
                timeout=ssh_timeout,
            )
        except subprocess.TimeoutExpired as exc:
            result = subprocess.CompletedProcess([], 124, b"", str(exc).encode())
        if result.returncode == 0:
            try:
                response = _decode_json_output(result, "remote job status")
                durable = _validate_status_response(response, paths)
            except RemoteLifecycleError:
                raise
            except click.ClickException as exc:
                failures += 1
                click.echo(f"warning: {exc}", err=True)
            else:
                failures = 0
                click.echo(json.dumps(response, sort_keys=True))
                if _is_final_status(durable, paths):
                    raise SystemExit(int(durable["exit_code"]))
                if poll_count % tail_every == 0:
                    try:
                        tail = _remote_agent(
                            target,
                            "_tail",
                            [
                                *_common_remote_args(run_id, str(paths.run_root)),
                                "--lines",
                                str(tail_lines),
                            ],
                            timeout=ssh_timeout,
                        )
                    except subprocess.TimeoutExpired as exc:
                        click.echo(f"warning: log tail failed: {exc}", err=True)
                    else:
                        if tail.returncode != 0:
                            click.echo(
                                "warning: log tail failed: "
                                f"{_text(tail.stderr).strip()}",
                                err=True,
                            )
                        elif tail.stdout:
                            click.echo(_text(tail.stdout), nl=False)
        else:
            failures += 1
            detail = _text(result.stderr).strip()
            click.echo(
                f"warning: Polling failed ({failures}/{max_consecutive_errors}): {detail}",
                err=True,
            )
        if failures >= max_consecutive_errors:
            raise click.ClickException(
                f"lost contact with {paths.unit} after {failures} consecutive polls"
            )
        time.sleep(poll_interval)


@cli.command("stop")
@click.option("--target", required=True, callback=lambda _c, _p, v: validate_target(v))
@click.option("--run-id", required=True, callback=lambda _c, _p, v: validate_run_id(v))
@click.option("--run-root", default=DEFAULT_RUN_ROOT, show_default=True)
@click.option("--attempts", default=3, type=click.IntRange(min=1))
@click.option("--retry-delay", default=5.0, type=click.FloatRange(min=0.0))
@click.option("--ssh-timeout", default=330.0, type=click.FloatRange(min=0.1))
@click.option("--tail-lines", default=40, type=click.IntRange(min=1))
def stop_cmd(
    target: str,
    run_id: str,
    run_root: str,
    attempts: int,
    retry_delay: float,
    ssh_timeout: float,
    tail_lines: int,
) -> None:
    paths = derive_paths(run_root, run_id)
    args = [
        *_common_remote_args(run_id, str(paths.run_root)),
        "--lines",
        str(tail_lines),
    ]
    last: subprocess.CompletedProcess[bytes] | None = None
    for attempt in range(1, attempts + 1):
        try:
            last = _remote_agent(target, "_stop", args, timeout=ssh_timeout)
        except subprocess.TimeoutExpired as exc:
            last = subprocess.CompletedProcess([], 124, b"", str(exc).encode())
        if last.returncode == 0:
            click.echo(_text(last.stdout), nl=False)
            return
        click.echo(
            f"warning: finalization attempt {attempt}/{attempts} failed: "
            f"{_text(last.stderr).strip()}",
            err=True,
        )
        if attempt < attempts:
            time.sleep(retry_delay)
    assert last is not None
    raise _process_error("remote job finalization", last)


@cli.command("_stage", hidden=True)
@click.option("--run-id", required=True)
@click.option("--run-root", required=True)
def stage_agent_cmd(run_id: str, run_root: str) -> None:
    paths = derive_paths(run_root, run_id)
    _safe_extract_stage(sys.stdin.buffer, paths)
    click.echo(json.dumps({"run_id": run_id, "run_dir": str(paths.run_dir)}))


@cli.command("_preflight", hidden=True)
@click.option("--run-id", required=True)
@click.option("--run-root", required=True)
@click.option("--lock-file", required=True)
@click.option("--active-unit-pattern", multiple=True)
@click.option("--conflict-process", multiple=True)
def preflight_agent_cmd(
    run_id: str,
    run_root: str,
    lock_file: str,
    active_unit_pattern: tuple[str, ...],
    conflict_process: tuple[str, ...],
) -> None:
    paths = derive_paths(run_root, run_id)
    if not paths.run_dir.is_dir():
        raise click.ClickException(f"staged run directory is missing: {paths.run_dir}")
    lock = validate_absolute_path(lock_file, name="lock file")
    preflight_host(lock, active_unit_pattern, conflict_process)
    click.echo(json.dumps({"state": "ready", "run_id": run_id}))


@cli.command("_start", hidden=True, context_settings={"ignore_unknown_options": True})
@click.option("--run-id", required=True)
@click.option("--run-root", required=True)
@click.option("--lock-file", required=True)
@click.option("--runtime-seconds", required=True, type=click.IntRange(min=1))
@click.option(
    "--stop-timeout-seconds",
    required=True,
    type=click.IntRange(min=MIN_STOP_TIMEOUT_SECONDS),
)
@click.option("--working-directory", required=True)
@click.argument("command", nargs=-1, required=True, type=click.UNPROCESSED)
def launch_agent_cmd(
    run_id: str,
    run_root: str,
    lock_file: str,
    runtime_seconds: int,
    stop_timeout_seconds: int,
    working_directory: str,
    command: tuple[str, ...],
) -> None:
    paths = derive_paths(run_root, run_id)
    if not paths.run_dir.is_dir():
        raise click.ClickException(f"staged run directory is missing: {paths.run_dir}")
    lock = validate_absolute_path(lock_file, name="lock file")
    working = validate_absolute_path(working_directory, name="working directory")
    executable = shutil.which("cephtools")
    if executable is None:
        raise click.ClickException(
            "cephtools executable is not available on the remote host"
        )
    _write_status(
        paths,
        {
            "state": "launching",
            "started_at": _utc_now(),
            "argv": list(command),
        },
    )
    argv = [
        "sudo",
        "systemd-run",
        "--collect",
        "--unit",
        paths.unit,
        "--uid",
        "ubuntu",
        "--gid",
        "ubuntu",
        "--working-directory",
        str(working),
        "--property",
        "KillMode=control-group",
        "--property",
        f"RuntimeMaxSec={runtime_seconds}s",
        "--property",
        f"TimeoutStopSec={stop_timeout_seconds}s",
        executable,
        "testenv",
        "job",
        "_run",
        "--run-id",
        run_id,
        "--run-root",
        str(paths.run_root),
        "--lock-file",
        str(lock),
        "--kill-after-seconds",
        str(stop_timeout_seconds - STOP_STATUS_MARGIN_SECONDS),
        "--",
        *command,
    ]
    result = subprocess.run(argv, capture_output=True, text=True)
    if result.returncode != 0:
        _write_status(
            paths,
            {
                "state": "finished",
                "exit_code": 125,
                "started_at": _utc_now(),
                "finished_at": _utc_now(),
                "argv": list(command),
                "message": (
                    f"systemd launch failed ({result.returncode}): "
                    f"{result.stderr.strip()}"
                ),
            },
        )
        raise click.ClickException(
            f"systemd launch failed ({result.returncode}): {result.stderr.strip()}"
        )
    click.echo(
        json.dumps(
            {
                "protocol": PROTOCOL_VERSION,
                "run_id": run_id,
                "run_dir": str(paths.run_dir),
                "unit": paths.unit,
                "launched": True,
            },
            sort_keys=True,
        )
    )


@cli.command("_run", hidden=True, context_settings={"ignore_unknown_options": True})
@click.option("--run-id", required=True)
@click.option("--run-root", required=True)
@click.option("--lock-file", required=True)
@click.option("--kill-after-seconds", default=5.0, type=click.FloatRange(min=0.1))
@click.argument("command", nargs=-1, required=True, type=click.UNPROCESSED)
def run_agent_cmd(
    run_id: str,
    run_root: str,
    lock_file: str,
    kill_after_seconds: float,
    command: tuple[str, ...],
) -> None:
    paths = derive_paths(run_root, run_id)
    lock = validate_absolute_path(lock_file, name="lock file")
    raise SystemExit(_supervise(paths, lock, command, kill_after_seconds))


@cli.command("_status", hidden=True)
@click.option("--run-id", required=True)
@click.option("--run-root", required=True)
def status_agent_cmd(run_id: str, run_root: str) -> None:
    paths = derive_paths(run_root, run_id)
    click.echo(json.dumps(_host_status(paths), sort_keys=True))


@cli.command("_tail", hidden=True)
@click.option("--run-id", required=True)
@click.option("--run-root", required=True)
@click.option("--lines", default=40, type=click.IntRange(min=1))
def tail_agent_cmd(run_id: str, run_root: str, lines: int) -> None:
    paths = derive_paths(run_root, run_id)
    try:
        content = paths.log.read_text(errors="replace").splitlines()
    except FileNotFoundError:
        return
    click.echo("\n".join(content[-lines:]))


@cli.command("_stop", hidden=True)
@click.option("--run-id", required=True)
@click.option("--run-root", required=True)
@click.option("--lines", default=40, type=click.IntRange(min=1))
def stop_agent_cmd(run_id: str, run_root: str, lines: int) -> None:
    paths = derive_paths(run_root, run_id)
    active = subprocess.run(
        ["systemctl", "is-active", "--quiet", paths.unit], capture_output=True
    )
    was_active = active.returncode == 0
    if was_active:
        stopped = subprocess.run(
            ["sudo", "systemctl", "stop", paths.unit], capture_output=True, text=True
        )
        if stopped.returncode != 0:
            raise click.ClickException(
                f"failed to stop exact unit {paths.unit}: {stopped.stderr.strip()}"
            )
    status_error: dict[str, str] | None = None
    try:
        durable = _read_status(paths)
    except click.ClickException as exc:
        durable = None
        status_error = {
            "kind": "invalid-durable-status",
            "message": str(exc),
        }
    systemd = _systemd_properties(paths.unit)
    response = {
        "protocol": PROTOCOL_VERSION,
        "run_id": run_id,
        "unit": paths.unit,
        "durable": durable or {"state": "pending"},
        "systemd": systemd,
        "log_tail": (
            "\n".join(paths.log.read_text(errors="replace").splitlines()[-lines:])
            if paths.log.exists()
            else ""
        ),
    }
    if status_error is not None:
        response["lifecycle_error"] = status_error
        response["outcome"] = (
            "stopped-with-invalid-durable-status"
            if was_active
            else "invalid-durable-status"
        )
        click.echo(json.dumps(response, sort_keys=True))
        return
    if _is_final_status(durable, paths):
        click.echo(json.dumps(response, sort_keys=True))
        return
    if not was_active and durable is None and systemd.get("LoadState") == "not-found":
        response["outcome"] = "never-launched"
        click.echo(json.dumps(response, sort_keys=True))
        return
    detail = json.dumps(response, sort_keys=True)
    raise click.ClickException(
        "exact unit has no authoritative final status after stop: " + detail
    )
