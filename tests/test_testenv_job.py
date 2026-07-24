from __future__ import annotations

import fcntl
import io
import json
import os
import signal
import subprocess
import sys
import tarfile
import threading
import time
from pathlib import Path
from typing import Any

import click
import pytest
from click.testing import CliRunner

import cephtools.testenv as testenv
import cephtools.testenv_job as job


def completed(
    returncode: int = 0, stdout: str = "", stderr: str = ""
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


def status_response(
    status: dict[str, Any],
    *,
    run_id: str = "run",
    lifecycle_error: dict[str, str] | None = None,
) -> str:
    response: dict[str, Any] = {
        "protocol": job.PROTOCOL_VERSION,
        "run_id": run_id,
        "unit": f"cephtools-testenv-job-{run_id}.service",
        "status": status,
        "systemd": {"LoadState": "loaded", "ActiveState": "active"},
    }
    if lifecycle_error is not None:
        response["lifecycle_error"] = lifecycle_error
    return json.dumps(response)


def test_job_cli_does_not_discover_primary_ip(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        testenv, "primary_ip", lambda: (_ for _ in ()).throw(AssertionError())
    )

    result = CliRunner().invoke(testenv.cli, ["job", "protocol"])

    assert result.exit_code == 0
    assert result.output == f"{job.PROTOCOL_VERSION}\n"


@pytest.mark.parametrize(
    "run_id",
    ["", "-leading", "space bad", "slash/bad", "x" * 161, "line\nbreak"],
)
def test_validate_run_id_rejects_unsafe_values(run_id: str) -> None:
    with pytest.raises(click.BadParameter):
        job.validate_run_id(run_id)


@pytest.mark.parametrize(
    "path", ["relative", "/tmp/../root", "/tmp//double", "/tmp/trailing/", "/tmp\nbad"]
)
def test_validate_absolute_path_rejects_unsafe_values(path: str) -> None:
    with pytest.raises(click.BadParameter):
        job.validate_absolute_path(path, label="path")


def test_derive_paths_is_deterministic_and_exact() -> None:
    paths = job.derive_paths("/home/ubuntu/runs", "run-1")

    assert paths.run_dir == Path("/home/ubuntu/runs/run-1")
    assert paths.status_file == paths.run_dir / "status.json"
    assert paths.log_file == paths.run_dir / "run.log"
    assert paths.unit == "cephtools-testenv-job-run-1.service"


def test_stage_archive_preserves_relative_layout_and_normalizes_modes(
    tmp_path: Path,
) -> None:
    script = tmp_path / "script.sh"
    script.write_text("#!/bin/sh\necho ok\n")
    script.chmod(0o755)
    config = tmp_path / "job.env"
    config.write_text("A=1\n")
    paths = job.derive_paths(str(tmp_path / "runs"), "run-1")

    payload = job.build_stage_archive(
        [(str(script), "bin/script.sh"), (str(config), "job.env")]
    )
    job.extract_stage_archive(paths, payload)

    assert (paths.run_dir / "bin/script.sh").read_text() == script.read_text()
    assert (paths.run_dir / "job.env").read_text() == config.read_text()
    assert (paths.run_dir / "bin/script.sh").stat().st_mode & 0o777 == 0o555
    assert (paths.run_dir / "job.env").stat().st_mode & 0o777 == 0o444


def test_stage_refuses_duplicate_run_directory(tmp_path: Path) -> None:
    source = tmp_path / "file"
    source.write_text("first")
    payload = job.build_stage_archive([(str(source), "file")])
    paths = job.derive_paths(str(tmp_path / "runs"), "same")
    job.extract_stage_archive(paths, payload)

    source.write_text("second")
    with pytest.raises(click.ClickException):
        job.extract_stage_archive(
            paths, job.build_stage_archive([(str(source), "file")])
        )

    assert (paths.run_dir / "file").read_text() == "first"


def test_stage_archive_rejects_symlink_and_duplicate_destination(
    tmp_path: Path,
) -> None:
    source = tmp_path / "file"
    source.write_text("data")
    symlink = tmp_path / "link"
    symlink.symlink_to(source)

    with pytest.raises(click.BadParameter, match="non-symlink"):
        job.build_stage_archive([(str(symlink), "link")])
    with pytest.raises(click.BadParameter, match="duplicate"):
        job.build_stage_archive([(str(source), "same"), (str(source), "same")])


def test_truncated_stage_archive_leaves_no_claimed_run_directory(
    tmp_path: Path,
) -> None:
    source = tmp_path / "large"
    source.write_bytes(b"x" * 2048)
    payload = job.build_stage_archive([(str(source), "large")])
    paths = job.derive_paths(str(tmp_path / "runs"), "run")

    with pytest.raises(click.ClickException, match="invalid stage archive"):
        job.extract_stage_archive(paths, payload[:700])

    assert not paths.run_dir.exists()
    assert not list(paths.run_root.glob(".run.staging-*"))


def test_extract_stage_archive_rejects_traversal(tmp_path: Path) -> None:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        content = b"escape"
        info = tarfile.TarInfo("../outside")
        info.size = len(content)
        archive.addfile(info, io.BytesIO(content))
    paths = job.derive_paths(str(tmp_path / "runs"), "run")

    with pytest.raises(click.BadParameter):
        job.extract_stage_archive(paths, buffer.getvalue())

    assert not (tmp_path / "runs" / "outside").exists()


def test_remote_command_quotes_each_argument_and_rejects_target_injection() -> None:
    command = job._remote_command(
        "ubuntu@example.test", ["printf", "%s", "hello; touch /tmp/bad"]
    )

    assert command == [
        "ssh",
        "ubuntu@example.test",
        "printf %s 'hello; touch /tmp/bad'",
    ]
    with pytest.raises(click.BadParameter):
        job._remote_command("ubuntu@example.test;bad", ["true"])
    with pytest.raises(click.BadParameter):
        job._remote_command("-bad@example.test", ["true"])


def test_preflight_refuses_held_lock(tmp_path: Path) -> None:
    lock_path = tmp_path / "lock"
    lock_path.touch()
    with lock_path.open("a+") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(click.ClickException, match="lock is held"):
            job.preflight_host(
                lock_path,
                runner=lambda *_args, **_kwargs: completed(),
            )


def test_preflight_reports_held_lock_when_fuser_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lock_path = tmp_path / "lock"
    lock_path.touch()
    monkeypatch.setattr(
        job.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(FileNotFoundError()),
    )

    with lock_path.open("a+") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(click.ClickException, match="lock is held"):
            job.preflight_host(lock_path)


def test_preflight_refuses_active_unit_and_conflicting_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_runner(arguments: list[str], **_kwargs: Any):
        if arguments[0] == "systemctl" and arguments[-1] == "legacy-*.service":
            return completed(stdout="legacy-one.service loaded active running\n")
        if arguments[0] == "pgrep":
            return completed(stdout="99999 /tmp/legacy-script integration\n")
        return completed()

    monkeypatch.setattr(job, "_ancestor_pids", lambda: {os.getpid()})
    with pytest.raises(click.ClickException) as exc_info:
        job.preflight_host(
            tmp_path / "lock",
            active_unit_patterns=["legacy-*.service"],
            conflict_processes=["legacy-script"],
            runner=fake_runner,
        )

    message = str(exc_info.value)
    assert "legacy-one.service" in message
    assert "99999 /tmp/legacy-script" in message


def test_preflight_fails_closed_when_conflict_checks_fail(tmp_path: Path) -> None:
    def systemctl_failure(arguments: list[str], **_kwargs: Any):
        assert arguments[0] == "systemctl"
        return completed(1, stderr="dbus unavailable")

    with pytest.raises(click.ClickException, match="dbus unavailable"):
        job.preflight_host(
            tmp_path / "systemctl.lock",
            active_unit_patterns=["legacy-*.service"],
            runner=systemctl_failure,
        )

    seen: list[str] = []

    def pgrep_failure(arguments: list[str], **_kwargs: Any):
        seen.extend(arguments)
        return completed(2, stderr="pgrep failed")

    with pytest.raises(click.ClickException, match="pgrep failed"):
        job.preflight_host(
            tmp_path / "pgrep.lock",
            conflict_processes=["-legacy"],
            runner=pgrep_failure,
        )
    assert seen == ["pgrep", "-af", "--", "-legacy"]


def test_start_checks_protocol_before_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "payload"
    source.write_text("data")
    calls: list[list[str]] = []

    def incompatible(_target: str, arguments: list[str], **_kwargs: Any):
        calls.append(arguments)
        return completed(stdout="999\n")

    monkeypatch.setattr(job, "run_remote", incompatible)
    result = CliRunner().invoke(
        job.cli,
        [
            "start",
            "--target",
            "ubuntu@example.test",
            "--run-id",
            "run-1",
            "--run-root",
            "/home/ubuntu/runs",
            "--runtime-seconds",
            "60",
            "--stage",
            str(source),
            "payload",
            "--",
            "/bin/true",
        ],
    )

    assert result.exit_code != 0
    assert "incompatible" in result.output
    assert calls == [["cephtools", "testenv", "job", "protocol"]]


def test_controller_timeouts_are_reported_as_click_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "payload"
    source.write_text("data")
    monkeypatch.setattr(
        job,
        "run_remote",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            subprocess.TimeoutExpired(["ssh"], 25)
        ),
    )

    start = CliRunner().invoke(
        job.cli,
        [
            "start",
            "--target",
            "ubuntu@example.test",
            "--run-id",
            "run",
            "--runtime-seconds",
            "60",
            "--stage",
            str(source),
            "payload",
            "--",
            "/bin/true",
        ],
    )
    status = CliRunner().invoke(
        job.cli,
        [
            "status",
            "--target",
            "ubuntu@example.test",
            "--run-id",
            "run",
        ],
    )

    assert start.exit_code != 0
    assert "protocol check timed out after 25 seconds" in start.output
    assert status.exit_code != 0
    assert "remote job status timed out after 25 seconds" in status.output


def test_start_stages_preflights_and_launches_in_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "payload"
    source.write_text("data")
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def fake_remote(_target: str, arguments: list[str], **kwargs: Any):
        calls.append((arguments, kwargs))
        if arguments[-1] == "protocol":
            return completed(stdout=f"{job.PROTOCOL_VERSION}\n")
        if "_stage" in arguments:
            return subprocess.CompletedProcess([], 0, b"", b"")
        if "_start" in arguments:
            return completed(stdout='{"state":"launched"}\n')
        return completed()

    monkeypatch.setattr(job, "run_remote", fake_remote)
    result = CliRunner().invoke(
        job.cli,
        [
            "start",
            "--target",
            "ubuntu@example.test",
            "--run-id",
            "run-1",
            "--run-root",
            "/home/ubuntu/runs",
            "--lock-file",
            "/run/lock/shared.lock",
            "--runtime-seconds",
            "99",
            "--stop-timeout-seconds",
            "12",
            "--stage",
            str(source),
            "nested/payload",
            "--active-unit-pattern",
            "ceph-qa-*.service",
            "--conflict-process",
            "legacy-script",
            "--",
            "/bin/echo",
            "hello world",
        ],
    )

    assert result.exit_code == 0, result.output
    assert [
        next(
            (
                name
                for name in ("protocol", "_stage", "_preflight", "_start")
                if name in call[0]
            ),
            "",
        )
        for call in calls
    ] == [
        "protocol",
        "_stage",
        "_preflight",
        "_start",
    ]
    start = calls[-1][0]
    assert "99" in start
    assert "12" in start
    assert start[-2:] == ["/bin/echo", "hello world"]
    assert isinstance(calls[1][1]["input_bytes"], bytes)


def test_start_agent_refuses_missing_staged_run(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        job.cli,
        [
            "_start",
            "--run-id",
            "missing",
            "--run-root",
            str(tmp_path / "runs"),
            "--lock-file",
            str(tmp_path / "lock"),
            "--runtime-seconds",
            "60",
            "--stop-timeout-seconds",
            "10",
            "--working-directory",
            "/home/ubuntu",
            "--",
            "/bin/true",
        ],
    )

    assert result.exit_code != 0
    assert "staged run directory is missing" in result.output


def test_start_agent_uses_detached_bounded_exact_systemd_unit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = job.derive_paths(str(tmp_path / "runs"), "run-1")
    paths.run_dir.mkdir(parents=True)
    captured: list[str] = []

    def fake_run(arguments: list[str], **_kwargs: Any):
        captured.extend(arguments)
        return completed(stdout="launched")

    monkeypatch.setattr(job.shutil, "which", lambda _name: "/usr/local/bin/cephtools")
    monkeypatch.setattr(job.subprocess, "run", fake_run)
    result = CliRunner().invoke(
        job.cli,
        [
            "_start",
            "--run-id",
            "run-1",
            "--run-root",
            str(paths.run_root),
            "--lock-file",
            str(tmp_path / "lock"),
            "--runtime-seconds",
            "60",
            "--stop-timeout-seconds",
            "30",
            "--working-directory",
            "/home/ubuntu",
            "--",
            "/bin/true",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured[:2] == ["sudo", "systemd-run"]
    assert "--collect" in captured
    assert paths.unit in captured
    assert "KillMode=control-group" in captured
    assert "RuntimeMaxSec=60s" in captured
    assert "TimeoutStopSec=30s" in captured
    grace_index = captured.index("--kill-after-seconds")
    assert captured[grace_index + 1] == "25"
    assert "--pipe" not in captured
    assert "--wait" not in captured
    assert captured[-1] == "/bin/true"


def test_host_job_records_output_and_exit_code(tmp_path: Path) -> None:
    paths = job.derive_paths(str(tmp_path / "runs"), "run")
    paths.run_dir.mkdir(parents=True)

    result = job.run_host_job(
        paths,
        tmp_path / "lock",
        ["bash", "-c", "echo durable-output; exit 7"],
    )

    status = json.loads(paths.status_file.read_text())
    assert result == 7
    assert paths.log_file.read_text() == "durable-output\n"
    assert status["state"] == "finished"
    assert status["exit_code"] == 7
    assert status["argv"] == ["bash", "-c", "echo durable-output; exit 7"]
    assert not list(paths.run_dir.glob("status.json.tmp.*"))


def test_host_job_records_spawn_failure_as_final_status(tmp_path: Path) -> None:
    paths = job.derive_paths(str(tmp_path / "runs"), "run")
    paths.run_dir.mkdir(parents=True)

    result = job.run_host_job(paths, tmp_path / "lock", ["/missing-command"])

    status = json.loads(paths.status_file.read_text())
    assert result == 127
    assert status["state"] == "finished"
    assert status["exit_code"] == 127
    assert "could not start payload" in paths.log_file.read_text()


def test_host_job_refuses_held_lock_and_records_final_status(tmp_path: Path) -> None:
    paths = job.derive_paths(str(tmp_path / "runs"), "run")
    paths.run_dir.mkdir(parents=True)
    lock_path = tmp_path / "lock"
    lock_path.touch()

    with lock_path.open("a+") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = job.run_host_job(paths, lock_path, ["touch", str(tmp_path / "bad")])

    status = json.loads(paths.status_file.read_text())
    assert result == 75
    assert status["state"] == "finished"
    assert status["exit_code"] == 75
    assert not (tmp_path / "bad").exists()


def test_host_job_holds_lock_for_payload_lifetime(tmp_path: Path) -> None:
    paths = job.derive_paths(str(tmp_path / "runs"), "run")
    paths.run_dir.mkdir(parents=True)
    lock_path = tmp_path / "lock"
    results: list[int] = []
    thread = threading.Thread(
        target=lambda: results.append(
            job.run_host_job(paths, lock_path, ["sleep", "0.4"])
        )
    )
    thread.start()
    deadline = time.time() + 2
    while not paths.status_file.exists() and time.time() < deadline:
        time.sleep(0.02)

    with lock_path.open("a+") as contender:
        with pytest.raises(BlockingIOError):
            fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
    thread.join(timeout=2)

    assert results == [0]


def test_host_job_terminates_process_group_and_records_status(tmp_path: Path) -> None:
    paths = job.derive_paths(str(tmp_path / "runs"), "run")
    paths.run_dir.mkdir(parents=True)
    script = tmp_path / "parent.sh"
    child_pid_file = tmp_path / "child.pid"
    script.write_text(
        "#!/bin/bash\n"
        "trap '' TERM\n"
        "(trap '' TERM; while true; do sleep 1; done) &\n"
        f"echo $! > {child_pid_file}\n"
        "while true; do sleep 1; done\n"
    )
    script.chmod(0o755)
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "from pathlib import Path; from cephtools.testenv_job import "
                "derive_paths, run_host_job; import sys; "
                "sys.exit(run_host_job(derive_paths(sys.argv[1], 'run'), "
                "Path(sys.argv[2]), [sys.argv[3]], kill_after=0.2))"
            ),
            str(paths.run_root),
            str(tmp_path / "lock"),
            str(script),
        ]
    )
    deadline = time.time() + 4
    while not child_pid_file.exists() and time.time() < deadline:
        time.sleep(0.05)
    assert child_pid_file.exists()
    child_pid = int(child_pid_file.read_text())

    process.send_signal(signal.SIGTERM)
    assert process.wait(timeout=5) == 143
    status = json.loads(paths.status_file.read_text())
    assert status["state"] == "terminated"
    assert status["exit_code"] == 143
    deadline = time.time() + 2
    while Path(f"/proc/{child_pid}").exists() and time.time() < deadline:
        time.sleep(0.05)
    assert not Path(f"/proc/{child_pid}").exists()


def test_host_job_allows_term_cleanup_within_configured_grace(
    tmp_path: Path,
) -> None:
    paths = job.derive_paths(str(tmp_path / "runs"), "run")
    paths.run_dir.mkdir(parents=True)
    marker = tmp_path / "collected"
    ready = tmp_path / "ready"
    script = tmp_path / "collect.sh"
    script.write_text(
        "#!/bin/bash\n"
        f"trap 'sleep 0.4; echo collected > {marker}; exit 0' TERM\n"
        f"touch {ready}\n"
        "while true; do sleep 0.1; done\n"
    )
    script.chmod(0o755)
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "from pathlib import Path; from cephtools.testenv_job import "
                "derive_paths, run_host_job; import sys; "
                "sys.exit(run_host_job(derive_paths(sys.argv[1], 'run'), "
                "Path(sys.argv[2]), [sys.argv[3]], kill_after=1.5))"
            ),
            str(paths.run_root),
            str(tmp_path / "lock"),
            str(script),
        ]
    )
    deadline = time.time() + 3
    while not ready.exists() and time.time() < deadline:
        time.sleep(0.02)
    assert ready.exists()

    process.send_signal(signal.SIGTERM)
    assert process.wait(timeout=4) == 143
    assert marker.read_text().strip() == "collected"
    status = json.loads(paths.status_file.read_text())
    assert status["state"] == "terminated"
    assert status["exit_code"] == 143


def test_final_status_is_authoritative_after_unit_gc(tmp_path: Path) -> None:
    paths = job.derive_paths(str(tmp_path / "runs"), "run")
    paths.run_dir.mkdir(parents=True)
    job._write_status(paths, {"state": "finished", "exit_code": 0})

    document = job.read_host_status(
        paths,
        runner=lambda *_args, **_kwargs: completed(stdout="LoadState=not-found\n"),
    )

    assert document["status"]["exit_code"] == 0


def test_missing_unit_without_final_status_is_an_error(tmp_path: Path) -> None:
    paths = job.derive_paths(str(tmp_path / "runs"), "run")
    paths.run_dir.mkdir(parents=True)

    document = job.read_host_status(
        paths,
        runner=lambda *_args, **_kwargs: completed(stdout="LoadState=not-found\n"),
    )

    assert document["lifecycle_error"]["kind"] == "unit-missing-without-final-status"


def test_stale_or_malformed_final_status_is_not_authoritative(tmp_path: Path) -> None:
    paths = job.derive_paths(str(tmp_path / "runs"), "run")
    paths.run_dir.mkdir(parents=True)
    paths.status_file.write_text(
        json.dumps(
            {
                "protocol": job.PROTOCOL_VERSION,
                "run_id": "another-run",
                "unit": paths.unit,
                "state": "finished",
                "exit_code": 0,
            }
        )
    )

    document = job.read_host_status(
        paths,
        runner=lambda *_args, **_kwargs: completed(stdout="LoadState=not-found\n"),
    )

    assert document["lifecycle_error"]["kind"] == "invalid-durable-status"


def test_inactive_unit_without_final_status_is_an_error(tmp_path: Path) -> None:
    paths = job.derive_paths(str(tmp_path / "runs"), "run")
    paths.run_dir.mkdir(parents=True)

    document = job.read_host_status(
        paths,
        runner=lambda *_args, **_kwargs: completed(
            stdout="LoadState=loaded\nActiveState=failed\n"
        ),
    )

    assert document["lifecycle_error"]["kind"] == "unit-inactive-without-final-status"


def test_wait_tolerates_transient_failures_and_propagates_remote_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [completed(1, stderr="offline"), completed(1, stderr="offline")]
    responses.append(
        completed(
            stdout=status_response(
                {
                    "protocol": job.PROTOCOL_VERSION,
                    "run_id": "run",
                    "unit": "cephtools-testenv-job-run.service",
                    "state": "finished",
                    "exit_code": 7,
                }
            )
        )
    )
    monkeypatch.setattr(job, "run_remote", lambda *_args, **_kwargs: responses.pop(0))
    monkeypatch.setattr(job.time, "sleep", lambda _seconds: None)

    result = CliRunner().invoke(
        job.cli,
        [
            "wait",
            "--target",
            "ubuntu@example.test",
            "--run-id",
            "run",
            "--run-root",
            "/tmp/runs",
            "--poll-interval",
            "1",
            "--max-consecutive-errors",
            "3",
        ],
    )

    assert result.exit_code == 7
    assert "Polling failed (2/3)" in result.output


def test_wait_ignores_a_failed_periodic_log_tail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        completed(
            stdout=status_response(
                {
                    "protocol": job.PROTOCOL_VERSION,
                    "run_id": "run",
                    "unit": "cephtools-testenv-job-run.service",
                    "state": "running",
                }
            )
        ),
        subprocess.TimeoutExpired(["ssh"], 1),
        completed(
            stdout=status_response(
                {
                    "protocol": job.PROTOCOL_VERSION,
                    "run_id": "run",
                    "unit": "cephtools-testenv-job-run.service",
                    "state": "finished",
                    "exit_code": 0,
                }
            )
        ),
    ]

    def fake_remote(*_args: Any, **_kwargs: Any):
        response = responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

    monkeypatch.setattr(job, "run_remote", fake_remote)
    monkeypatch.setattr(job.time, "sleep", lambda _seconds: None)

    result = CliRunner().invoke(
        job.cli,
        [
            "wait",
            "--target",
            "ubuntu@example.test",
            "--run-id",
            "run",
            "--run-root",
            "/tmp/runs",
            "--poll-interval",
            "0",
            "--tail-every",
            "1",
        ],
    )

    assert result.exit_code == 0
    assert "log tail failed" in result.output


def test_malformed_durable_status_is_a_structured_lifecycle_error(
    tmp_path: Path,
) -> None:
    paths = job.derive_paths(str(tmp_path / "runs"), "run")
    paths.run_dir.mkdir(parents=True)
    paths.status_file.write_text("not-json")

    document = job.read_host_status(
        paths,
        runner=lambda *_args, **_kwargs: completed(
            stdout="LoadState=loaded\nActiveState=active\n"
        ),
    )

    assert document["lifecycle_error"]["kind"] == "invalid-durable-status"
    with pytest.raises(job.RemoteLifecycleError):
        job._validate_status_response(document, paths)


def test_wait_fails_immediately_on_definitive_lifecycle_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def remote(*_args: Any, **_kwargs: Any):
        nonlocal calls
        calls += 1
        return completed(
            stdout=status_response(
                {"state": "launching"},
                lifecycle_error={
                    "kind": "unit-inactive-without-final-status",
                    "message": "service failed before durable completion",
                },
            )
        )

    monkeypatch.setattr(job, "run_remote", remote)
    result = CliRunner().invoke(
        job.cli,
        [
            "wait",
            "--target",
            "ubuntu@example.test",
            "--run-id",
            "run",
            "--run-root",
            "/tmp/runs",
            "--poll-interval",
            "0",
        ],
    )

    assert result.exit_code != 0
    assert "service failed before durable completion" in result.output
    assert "lost contact" not in result.output
    assert calls == 1


@pytest.mark.parametrize(
    "mutation",
    [
        lambda response: response.update(protocol=999),
        lambda response: response.update(run_id="wrong"),
        lambda response: response.update(unit="wrong.service"),
        lambda response: response["status"].update(state="unknown"),
    ],
)
def test_status_response_validation_rejects_wrong_envelope_and_state(
    mutation: Any,
) -> None:
    paths = job.derive_paths("/tmp/runs", "run")
    response = json.loads(
        status_response(
            {
                "protocol": job.PROTOCOL_VERSION,
                "run_id": "run",
                "unit": paths.unit,
                "state": "running",
            }
        )
    )
    mutation(response)

    with pytest.raises(click.ClickException):
        job._validate_status_response(response, paths)


def test_wait_fails_at_configured_error_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(job, "run_remote", lambda *_args, **_kwargs: completed(1))
    monkeypatch.setattr(job.time, "sleep", lambda _seconds: None)

    result = CliRunner().invoke(
        job.cli,
        [
            "wait",
            "--target",
            "ubuntu@example.test",
            "--run-id",
            "run",
            "--run-root",
            "/tmp/runs",
            "--poll-interval",
            "1",
            "--max-consecutive-errors",
            "2",
        ],
    )

    assert result.exit_code != 0
    assert "lost contact" in result.output


def test_stop_agent_targets_only_the_exact_derived_unit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = job.derive_paths(str(tmp_path / "runs"), "run")
    paths.run_dir.mkdir(parents=True)
    job._write_status(paths, {"state": "terminated", "exit_code": 143})
    calls: list[list[str]] = []

    def fake_run(arguments: list[str], **_kwargs: Any):
        calls.append(arguments)
        if arguments[:3] == ["systemctl", "is-active", "--quiet"]:
            return completed()
        if arguments[:3] == ["sudo", "systemctl", "stop"]:
            return completed()
        return completed(stdout="LoadState=not-found\n")

    monkeypatch.setattr(job.subprocess, "run", fake_run)
    result = CliRunner().invoke(
        job.cli,
        [
            "_stop",
            "--run-id",
            "run",
            "--run-root",
            str(paths.run_root),
        ],
    )

    assert result.exit_code == 0, result.output
    assert ["sudo", "systemctl", "stop", paths.unit] in calls
    assert all("*" not in argument for call in calls for argument in call)


def test_stop_agent_reports_malformed_status_after_best_effort_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = job.derive_paths(str(tmp_path / "runs"), "run")
    paths.run_dir.mkdir(parents=True)
    paths.status_file.write_text("{truncated")
    calls: list[list[str]] = []

    def fake_run(arguments: list[str], **_kwargs: Any):
        calls.append(arguments)
        if arguments[:3] == ["systemctl", "is-active", "--quiet"]:
            return completed()
        if arguments[:3] == ["sudo", "systemctl", "stop"]:
            return completed()
        return completed(stdout="LoadState=not-found\n")

    monkeypatch.setattr(job.subprocess, "run", fake_run)
    result = CliRunner().invoke(
        job.cli,
        ["_stop", "--run-id", "run", "--run-root", str(paths.run_root)],
    )

    assert result.exit_code == 0, result.output
    assert ["sudo", "systemctl", "stop", paths.unit] in calls
    response = json.loads(result.output)
    assert response["outcome"] == "stopped-with-invalid-durable-status"
    assert response["lifecycle_error"]["kind"] == "invalid-durable-status"
    assert "Invalid durable status" in response["lifecycle_error"]["message"]


def test_stop_agent_is_idempotent_for_never_launched_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = job.derive_paths(str(tmp_path / "runs"), "run")
    paths.run_dir.mkdir(parents=True)

    def fake_run(arguments: list[str], **_kwargs: Any):
        if arguments[:3] == ["systemctl", "is-active", "--quiet"]:
            return completed(4)
        return completed(stdout="LoadState=not-found\n")

    monkeypatch.setattr(job.subprocess, "run", fake_run)
    result = CliRunner().invoke(
        job.cli,
        ["_stop", "--run-id", "run", "--run-root", str(paths.run_root)],
    )

    assert result.exit_code == 0, result.output
    assert '"outcome": "never-launched"' in result.output


def test_stop_agent_rejects_failed_unit_without_final_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = job.derive_paths(str(tmp_path / "runs"), "run")
    paths.run_dir.mkdir(parents=True)
    job._write_status(paths, {"state": "launching"})

    def fake_run(arguments: list[str], **_kwargs: Any):
        if arguments[:3] == ["systemctl", "is-active", "--quiet"]:
            return completed(3)
        return completed(stdout="LoadState=loaded\nActiveState=failed\n")

    monkeypatch.setattr(job.subprocess, "run", fake_run)
    result = CliRunner().invoke(
        job.cli,
        ["_stop", "--run-id", "run", "--run-root", str(paths.run_root)],
    )

    assert result.exit_code != 0
    assert "no authoritative final status" in result.output
    assert '"ActiveState": "failed"' in result.output
