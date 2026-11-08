from __future__ import annotations

import datetime as dt
import shutil
import subprocess
from pathlib import Path
from typing import Any

import click
import pytest
from click.testing import CliRunner

from cephtools import moveroot
from cephtools.moveroot import (
    POLL_MARKER,
    Remote,
    UnitState,
    build_finish_script,
    build_prepare_script,
    normalise_device,
    parse_poll_output,
)
from cephtools.testflinger import (
    ReservationDetails,
    cli,
    load_latest_reservation_target,
    save_latest_reservation,
)

requires_bash = pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")


@pytest.fixture
def state_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / "state"
    monkeypatch.setenv("CEPHTOOLS_STATE_HOME", str(home))
    return home


def _completed(returncode: int = 0, stdout: str = "", stderr: str = ""):
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


def _poll_output(
    lines: list[str],
    *,
    cursor: str | None = None,
    active: str = "active",
    sub: str = "running",
    result: str = "success",
    status: str = "0",
    load: str = "loaded",
) -> str:
    journal = list(lines)
    if cursor:
        journal.append(f"-- cursor: {cursor}")
    props = [
        f"LoadState={load}",
        f"ActiveState={active}",
        f"SubState={sub}",
        f"Result={result}",
        f"ExecMainStatus={status}",
    ]
    return "\n".join([*journal, POLL_MARKER, *props, ""])


class FakeHost:
    """Fake SSH runner that answers by the kind of remote command."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.inputs: dict[str, str] = {}
        self.responses: dict[str, list[Any]] = {}

    def queue(self, kind: str, *responses: Any) -> None:
        self.responses.setdefault(kind, []).extend(responses)

    @staticmethod
    def kind_of(command: str) -> str:
        if "sudo tee" in command:
            return "upload"
        if "--on-active" in command:
            return "reboot"
        if "systemd-run" in command:
            return "start"
        if "journalctl" in command:
            return "poll"
        if "boot_id" in command:
            return "boot_id"
        if "findmnt -no UUID /" in command:
            return "probe"
        raise AssertionError(f"unexpected remote command: {command}")

    def __call__(self, argv: list[str], **kwargs: Any):
        assert argv[0] == "ssh"
        assert kwargs["check"] is False
        command = argv[-1]
        kind = self.kind_of(command)
        self.calls.append((kind, command))
        if kind == "upload":
            self.inputs[command] = kwargs["input"]
        queued = self.responses.get(kind)
        if queued:
            response = queued.pop(0)
            return response(command) if callable(response) else response
        return _completed()

    def kinds(self) -> list[str]:
        return [kind for kind, _ in self.calls]


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _remote(host: FakeHost, echo: list[str], clock: FakeClock | None = None) -> Remote:
    clock = clock or FakeClock()
    return Remote(
        "ubuntu@10.0.0.2",
        runner=host,
        echo=echo.append,
        sleep=clock.sleep,
        clock=clock,
    )


# --- device names and generated scripts ------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("sdb", "/dev/sdb"),
        ("/dev/nvme0n1", "/dev/nvme0n1"),
        ("/dev/disk/by-id/nvme-Foo_SSD_123", "/dev/disk/by-id/nvme-Foo_SSD_123"),
        (
            "disk/by-path/pci-0000:00:1f.2-ata-1",
            "/dev/disk/by-path/pci-0000:00:1f.2-ata-1",
        ),
    ],
)
def test_normalise_device_accepts(value: str, expected: str) -> None:
    assert normalise_device(value) == expected


@pytest.mark.parametrize(
    "value", ["", "/dev/", "/dev/../etc/passwd", "sd b", "/dev/sdb;reboot", "/dev/.x"]
)
def test_normalise_device_rejects(value: str) -> None:
    with pytest.raises(click.BadParameter):
        normalise_device(value)


def test_prepare_script_header_quotes_values() -> None:
    script = build_prepare_script("/dev/nvme0n1", "/dev/x y", force=True)
    assert "OLD_DEV=/dev/nvme0n1\n" in script
    assert "NEW_DEV='/dev/x y'\n" in script
    assert "FORCE=1\n" in script
    assert f"STATE_FILE={moveroot.STATE_FILE}\n" in script
    assert script.rstrip().endswith('main "$@"')


def test_finish_script_wipe_flag() -> None:
    assert "WIPE_OLD=1\n" in build_finish_script()
    assert "WIPE_OLD=0\n" in build_finish_script(wipe_old=False)


@requires_bash
@pytest.mark.parametrize(
    "script",
    [
        build_prepare_script("/dev/nvme0n1", "/dev/sdb"),
        build_finish_script(wipe_old=True),
    ],
)
def test_generated_scripts_are_valid_bash(script: str, tmp_path: Path) -> None:
    path = tmp_path / "script.sh"
    path.write_text(script)
    result = subprocess.run(["bash", "-n", str(path)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def _run_prepare_helper(snippet: str) -> subprocess.CompletedProcess:
    # Load the helpers without running main.
    source = moveroot._COMMON_SH + moveroot._PREPARE_SH + "\n" + snippet
    return subprocess.run(
        ["bash", "-c", source], capture_output=True, text=True, check=False
    )


@requires_bash
@pytest.mark.parametrize(
    ("orig", "after", "expected"),
    [
        # grub-install reused the entry number: keep the original order.
        ("0001,0000", "0000,0001", "0001,0000"),
        # it replaced Boot0000 with Boot0003: put the new one in its place.
        ("0001,0002,0000", "0003,0001,0002", "0001,0002,0003"),
        # it added an entry without deleting one: append it.
        ("0001,0000", "0003,0001,0000", "0001,0000,0003"),
        # there was no BootOrder before.
        ("", "0003", "0003"),
    ],
)
def test_compute_boot_order(orig: str, after: str, expected: str) -> None:
    result = _run_prepare_helper(f'compute_boot_order "{orig}" "{after}"')
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected


@requires_bash
@pytest.mark.parametrize(
    ("disk", "number", "expected"),
    [
        ("/dev/sdb", 1, "/dev/sdb1"),
        ("/dev/nvme1n1", 2, "/dev/nvme1n1p2"),
        ("/dev/mmcblk0", 1, "/dev/mmcblk0p1"),
    ],
)
def test_part_path(disk: str, number: int, expected: str) -> None:
    result = _run_prepare_helper(f"part_path {disk} {number}")
    assert result.stdout.strip() == expected


@requires_bash
def test_rewrite_fstab_replaces_root_and_esp_sources(tmp_path: Path) -> None:
    src = tmp_path / "fstab"
    dst = tmp_path / "fstab.new"
    src.write_text(
        "# comment\n"
        "LABEL=cloudimg-rootfs\t/\text4\tdiscard,errors=remount-ro\t0 1\n"
        "\n"
        "/dev/disk/by-uuid/1709-D254 /boot/efi vfat umask=0077 0 1\n"
        "/swap.img none swap sw 0 0\n"
    )
    result = _run_prepare_helper(f"rewrite_fstab {src} {dst} ROOTUUID ESPUUID")
    assert result.returncode == 0, result.stderr
    assert dst.read_text() == (
        "# comment\n"
        "UUID=ROOTUUID / ext4 discard,errors=remount-ro 0 1\n"
        "\n"
        "UUID=ESPUUID /boot/efi vfat umask=0077 0 1\n"
        "/swap.img none swap sw 0 0\n"
    )


@requires_bash
def test_rewrite_fstab_fails_without_esp_entry(tmp_path: Path) -> None:
    src = tmp_path / "fstab"
    src.write_text("UUID=abc / ext4 defaults 0 1\n")
    result = _run_prepare_helper(
        f"rewrite_fstab {src} {tmp_path / 'out'} R E || echo rc=$?"
    )
    assert "rc=3" in result.stdout


# --- journal polling --------------------------------------------------------


def test_parse_poll_output_splits_logs_cursor_and_state() -> None:
    parsed = parse_poll_output(
        _poll_output(["line one", "line two"], cursor="s=abc", sub="running")
    )
    assert parsed is not None
    lines, cursor, state = parsed
    assert lines == ["line one", "line two"]
    assert cursor == "s=abc"
    assert state == UnitState("loaded", "active", "running", "success", "0")
    assert not state.finished


def test_parse_poll_output_without_marker() -> None:
    assert parse_poll_output("ssh: connect to host timed out\n") is None


@pytest.mark.parametrize(
    ("state", "finished", "succeeded"),
    [
        (UnitState("loaded", "active", "running", "success", "0"), False, False),
        (UnitState("loaded", "active", "exited", "success", "0"), True, True),
        (UnitState("loaded", "failed", "failed", "exit-code", "1"), True, False),
        (UnitState("loaded", "inactive", "dead", "success", "0"), True, False),
        (UnitState("not-found", "inactive", "dead", "success", ""), True, False),
    ],
)
def test_unit_state(state: UnitState, finished: bool, succeeded: bool) -> None:
    assert state.finished is finished
    assert state.succeeded is succeeded


def test_build_poll_command_uses_cursor() -> None:
    first = moveroot.build_poll_command("unit-a", None)
    assert "--after-cursor" not in first
    assert "_SYSTEMD_UNIT=unit-a.service" in first
    later = moveroot.build_poll_command("unit-a", "s=1;i=2")
    assert "'--after-cursor=s=1;i=2'" in later


# --- running a remote step --------------------------------------------------


def test_run_remote_step_streams_output_until_success() -> None:
    host = FakeHost()
    host.queue(
        "poll",
        _completed(stdout=_poll_output(["copying"], cursor="c1")),
        _completed(stdout=_poll_output(["done"], cursor="c2", sub="exited")),
        _completed(stdout=_poll_output(["trailing"], cursor="c3", sub="exited")),
    )
    echo: list[str] = []

    moveroot.run_remote_step(
        _remote(host, echo), name="prepare", script="#!/bin/bash\n", run_token="t1"
    )

    assert host.kinds() == ["upload", "start", "poll", "poll", "poll"]
    assert list(host.inputs.values()) == ["#!/bin/bash\n"]
    start = host.calls[1][1]
    assert "--unit=cephtools-moveroot-prepare-t1" in start
    assert "RemainAfterExit=yes" in start
    assert "LoadState --value cephtools-moveroot-prepare-t1.service" in start
    polls = [command for kind, command in host.calls if kind == "poll"]
    assert "--after-cursor" not in polls[0]
    assert "--after-cursor=c1" in polls[1]
    assert "--after-cursor=c2" in polls[2]
    assert echo[-3:] == ["copying", "done", "trailing"]


def test_run_remote_step_rides_out_a_dropped_connection() -> None:
    host = FakeHost()
    host.queue(
        "poll",
        _completed(stdout=_poll_output(["one"], cursor="c1")),
        _completed(255, stderr="Connection timed out"),
        _completed(255, stderr="Connection timed out"),
        _completed(stdout=_poll_output(["two"], cursor="c2", sub="exited")),
        _completed(stdout=_poll_output([], sub="exited")),
    )
    echo: list[str] = []

    moveroot.run_remote_step(
        _remote(host, echo), name="prepare", script="x", run_token="t1"
    )

    assert sum("Lost the SSH connection" in m for m in echo) == 1
    assert any("Reconnected" in m for m in echo)
    assert "two" in echo
    # The cursor survives the outage so no lines are repeated.
    polls = [command for kind, command in host.calls if kind == "poll"]
    assert all("--after-cursor=c1" in command for command in polls[1:4])


def test_run_remote_step_gives_up_after_long_outage() -> None:
    host = FakeHost()
    host.queue("poll", *[_completed(255) for _ in range(50)])
    clock = FakeClock()

    with pytest.raises(click.ClickException, match="Lost contact .* for more than 30s"):
        moveroot.run_remote_step(
            _remote(host, [], clock),
            name="prepare",
            script="x",
            run_token="t1",
            poll_interval=10,
            disconnect_timeout=30,
        )


def test_run_remote_step_reports_failure_with_journal_hint() -> None:
    host = FakeHost()
    failed = _poll_output(
        ["moveroot: ERROR: /dev/sdb has partitions"],
        active="failed",
        sub="failed",
        result="exit-code",
        status="1",
    )
    host.queue("poll", _completed(stdout=failed), _completed(stdout=failed))
    echo: list[str] = []

    with pytest.raises(click.ClickException) as excinfo:
        moveroot.run_remote_step(
            _remote(host, echo), name="prepare", script="x", run_token="t1"
        )

    message = excinfo.value.message
    assert "exit status 1" in message
    assert "journalctl -u cephtools-moveroot-prepare-t1.service" in message
    assert "moveroot: ERROR: /dev/sdb has partitions" in echo


def test_run_remote_step_retries_start_after_connection_failure() -> None:
    host = FakeHost()
    host.queue("start", _completed(255), _completed())
    host.queue(
        "poll",
        _completed(stdout=_poll_output([], sub="exited")),
        _completed(stdout=_poll_output([], sub="exited")),
    )

    moveroot.run_remote_step(
        _remote(host, []), name="finish", script="x", run_token="t"
    )

    assert host.kinds()[:3] == ["upload", "start", "start"]


def test_run_remote_step_upload_failure() -> None:
    host = FakeHost()
    host.queue("upload", _completed(1, stderr="sudo: a password is required"))

    with pytest.raises(click.ClickException, match="password is required"):
        moveroot.run_remote_step(
            _remote(host, []), name="prepare", script="x", run_token="t"
        )
    assert host.kinds() == ["upload"]


# --- reboot -----------------------------------------------------------------


def test_wait_for_reboot_returns_once_boot_id_changes() -> None:
    host = FakeHost()
    host.queue(
        "boot_id",
        _completed(stdout="old\n"),
        _completed(255),
        _completed(255),
        _completed(stdout="new\n"),
    )
    echo: list[str] = []

    moveroot.wait_for_reboot(_remote(host, echo), previous_boot_id="old", timeout=600)

    assert host.kinds() == ["boot_id"] * 4
    assert echo[-1] == "ubuntu@10.0.0.2 is back up."


def test_wait_for_reboot_times_out() -> None:
    host = FakeHost()
    host.queue("boot_id", *[_completed(255) for _ in range(100)])
    echo: list[str] = []

    with pytest.raises(click.ClickException, match="did not come back within 120s"):
        moveroot.wait_for_reboot(
            _remote(host, echo), previous_boot_id="old", timeout=120, interval=10
        )
    assert any("Still waiting" in m for m in echo)


def test_reboot_host_schedules_delayed_reboot_once() -> None:
    host = FakeHost()
    moveroot.reboot_host(_remote(host, []), "t9")
    command = host.calls[0][1]
    assert "--on-active=5" in command
    assert "LoadState --value cephtools-moveroot-reboot-t9.timer" in command


# --- probing and the whole flow ---------------------------------------------


def _probe_output(state: str, root_uuid: str) -> str:
    return f"{state}{POLL_MARKER}\n{root_uuid}\n"


@pytest.mark.parametrize(
    ("state", "root_uuid", "moved"),
    [
        ("", "aaaa", False),
        ("NEW_ROOT_UUID=bbbb\nOLD_ROOT_UUID=aaaa\n", "aaaa", False),
        ("NEW_ROOT_UUID=bbbb\nOLD_ROOT_UUID=aaaa\n", "bbbb", True),
    ],
)
def test_probe_host(state: str, root_uuid: str, moved: bool) -> None:
    host = FakeHost()
    host.queue("probe", _completed(stdout=_probe_output(state, root_uuid)))
    result = moveroot.probe_host(_remote(host, []))
    assert result.root_uuid == root_uuid
    assert result.already_moved is moved


def test_probe_host_unreachable() -> None:
    host = FakeHost()
    host.queue(
        "probe", *[_completed(255, stderr="Connection timed out") for _ in range(30)]
    )
    with pytest.raises(click.ClickException, match="Cannot run commands .* timed out"):
        moveroot.probe_host(_remote(host, []))


def _finished_polls() -> list[Any]:
    done = _poll_output([], sub="exited")
    return [_completed(stdout=done), _completed(stdout=done)]


def _script_for(host: FakeHost, name: str) -> str:
    return next(
        script for command, script in host.inputs.items() if f"-{name}-" in command
    )


def test_move_root_full_flow() -> None:
    host = FakeHost()
    host.queue("probe", _completed(stdout=_probe_output("", "old-uuid")))
    host.queue("poll", *_finished_polls(), *_finished_polls())
    host.queue("boot_id", _completed(stdout="boot-1\n"), _completed(stdout="boot-2\n"))
    echo: list[str] = []

    moveroot.move_root(
        _remote(host, echo), old_dev="/dev/nvme0n1", new_dev="/dev/sdb", run_token="t"
    )

    assert host.kinds() == [
        "probe",
        "upload",
        "start",
        "poll",
        "poll",
        "boot_id",
        "reboot",
        "boot_id",
        "upload",
        "start",
        "poll",
        "poll",
    ]
    prepare = _script_for(host, "prepare")
    assert "OLD_DEV=/dev/nvme0n1\n" in prepare
    assert "NEW_DEV=/dev/sdb\n" in prepare
    assert "FORCE=0\n" in prepare
    assert "WIPE_OLD=1\n" in _script_for(host, "finish")
    assert "old disk is wiped" in echo[-1]


def test_move_root_resumes_after_reboot() -> None:
    host = FakeHost()
    host.queue("probe", _completed(stdout=_probe_output("NEW_ROOT_UUID=new\n", "new")))
    host.queue("poll", *_finished_polls())
    echo: list[str] = []

    moveroot.move_root(
        _remote(host, echo),
        old_dev="/dev/nvme0n1",
        new_dev="/dev/sdb",
        wipe_old=False,
        run_token="t",
    )

    assert host.kinds() == ["probe", "upload", "start", "poll", "poll"]
    assert "WIPE_OLD=0\n" in _script_for(host, "finish")
    assert any("skipping the copy and reboot" in m for m in echo)
    assert "old disk was kept" in echo[-1]


def test_move_root_stops_when_prepare_fails() -> None:
    host = FakeHost()
    host.queue("probe", _completed(stdout=_probe_output("", "old-uuid")))
    failed = _poll_output(
        [], active="failed", sub="failed", result="exit-code", status="1"
    )
    host.queue("poll", _completed(stdout=failed), _completed(stdout=failed))

    with pytest.raises(click.ClickException, match="prepare step failed"):
        moveroot.move_root(
            _remote(host, []), old_dev="/dev/sda", new_dev="/dev/sdb", run_token="t"
        )
    assert "reboot" not in host.kinds()


# --- CLI --------------------------------------------------------------------


def _capture_move_root(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    def fake_move_root(remote: Remote, **kwargs: Any) -> None:
        captured["target"] = remote.target
        captured.update(kwargs)

    monkeypatch.setattr("cephtools.testflinger.move_root", fake_move_root)
    return captured


def _save_reservation() -> None:
    save_latest_reservation(
        ReservationDetails(
            job_id="job-1",
            queue_name="ceph-qa-1",
            user="ubuntu",
            ip="10.241.199.15",
            expires_at=dt.datetime(2099, 1, 1, tzinfo=dt.timezone.utc),
            timeout_seconds=604000,
        )
    )


def test_load_latest_reservation_target(state_home: Path) -> None:
    _save_reservation()
    assert load_latest_reservation_target() == "ubuntu@10.241.199.15"


def test_moveroot_cli_defaults_to_latest_reservation(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
) -> None:
    _save_reservation()
    captured = _capture_move_root(monkeypatch)

    result = CliRunner().invoke(cli, ["moveroot", "nvme0n1", "/dev/sdb"])

    assert result.exit_code == 0, result.output
    assert captured == {
        "target": "ubuntu@10.241.199.15",
        "old_dev": "/dev/nvme0n1",
        "new_dev": "/dev/sdb",
        "force": False,
        "wipe_old": True,
        "reboot_timeout": moveroot.DEFAULT_REBOOT_TIMEOUT,
    }


def test_moveroot_cli_options(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
) -> None:
    captured = _capture_move_root(monkeypatch)

    result = CliRunner().invoke(
        cli,
        [
            "moveroot",
            "/dev/nvme0n1",
            "sdb",
            "--host",
            "ubuntu@10.0.0.9",
            "--force",
            "--keep-old",
            "--reboot-timeout",
            "60",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["target"] == "ubuntu@10.0.0.9"
    assert captured["force"] is True
    assert captured["wipe_old"] is False
    assert captured["reboot_timeout"] == 60


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["/dev/sda", "/dev/sd b"], "is not a device"),
        (["/dev/sda", "/dev/sdb", "--host", "not a target"], "USER@HOST"),
    ],
)
def test_moveroot_cli_rejects_bad_input(
    monkeypatch: pytest.MonkeyPatch,
    state_home: Path,
    args: list[str],
    message: str,
) -> None:
    captured = _capture_move_root(monkeypatch)
    result = CliRunner().invoke(cli, ["moveroot", *args])
    assert result.exit_code == 2
    assert message in result.output
    assert captured == {}


def test_moveroot_cli_without_reservation(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
) -> None:
    _capture_move_root(monkeypatch)
    result = CliRunner().invoke(cli, ["moveroot", "/dev/sda", "/dev/sdb"])
    assert result.exit_code == 1
    assert "Pass --host USER@HOST" in result.output
