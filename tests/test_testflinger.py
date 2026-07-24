from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import pytest
from click import ClickException
from click.testing import CliRunner

import cephtools.testflinger as testflinger
from cephtools.testflinger import (
    BackendConfig,
    ReservationDetails,
    build_job_file,
    build_deploy_script,
    cancel_reservation,
    clear_latest_reservation,
    cli,
    ensure_backend_config,
    latest_reservation_state_path,
    load_latest_reservation_job_id,
    machine_ids,
    parse_submit_output,
    perform_remote_deploy,
    read_testenv_cloud_config,
    read_testenv_credentials,
    read_testenv_network_config,
    save_latest_reservation,
    _parse_reservation_window,
    _ssh_key_reference_warning,
)


@pytest.fixture
def state_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / "state"
    monkeypatch.setenv("CEPHTOOLS_STATE_HOME", str(home))
    return home


def test_build_job_file_includes_required_fields() -> None:
    config = BackendConfig(
        launchpad_account="lp:tester",
        job_tag="foo",
        mattermost_name="@test",
    )
    job_file = build_job_file(config, "ceph-qa-1", reserve_for=900)

    assert "job_queue: ceph-qa-1" in job_file
    assert "    - lp:tester" in job_file
    assert "  - foo" in job_file
    assert "# Ask @test on Mattermost" in job_file


def test_build_job_file_preserves_custom_ssh_key_ref() -> None:
    config = BackendConfig(
        launchpad_account="gh:test",
        job_tag=None,
        mattermost_name=None,
    )
    job_file = build_job_file(config, "ceph-qa-1", reserve_for=600)

    assert "    - gh:test" in job_file


def test_ssh_key_reference_warning_for_valid_ref() -> None:
    assert _ssh_key_reference_warning("lp:tester") is None
    assert _ssh_key_reference_warning("gh:test-user") is None


@pytest.mark.parametrize("value", ["tester", "lp:", "gh:", "lp: test"])
def test_ssh_key_reference_warning_for_invalid_ref(value: str) -> None:
    warning = _ssh_key_reference_warning(value)
    assert warning is not None
    assert "lp:<launchpad-id>" in warning


@pytest.mark.parametrize(
    ("stdout", "expected"),
    [
        (
            "Job submitted successfully!\nJob ID: 1234-abcd\n",
            "1234-abcd",
        ),
        (
            "Job submitted successfully!\nJob abcdef\n",
            "abcdef",
        ),
    ],
)
def test_parse_submit_output_success(stdout: str, expected: str) -> None:
    assert parse_submit_output(stdout) == expected


@pytest.mark.parametrize(
    "stdout",
    [
        "Something went wrong",
        "Job submitted successfully!\nInvalid\n",
    ],
)
def test_parse_submit_output_failure(stdout: str) -> None:
    with pytest.raises(ClickException):
        parse_submit_output(stdout)


def test_cancel_reservation_invokes_testflinger() -> None:
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def fake_runner(cmd: list[str], **kwargs: Any):
        calls.append((cmd, kwargs))

        class Result:
            returncode = 0
            stdout = "Cancelled job-1\n"
            stderr = ""

        return Result()

    result = cancel_reservation("job-1", runner=fake_runner, testflinger_bin="tf")

    assert result.stdout == "Cancelled job-1\n"
    assert calls == [
        (
            ["tf", "cancel", "job-1"],
            {"capture_output": True, "text": True, "check": False},
        )
    ]


@pytest.mark.parametrize(
    ("stdout", "stderr", "expected_message"),
    [
        ("", "boom", "boom"),
        ("failed", "", "failed"),
        ("", "", "testflinger cancel failed"),
    ],
)
def test_cancel_reservation_failure(
    stdout: str,
    stderr: str,
    expected_message: str,
) -> None:
    def failing_runner(cmd: list[str], **kwargs: Any):
        return type(
            "Result",
            (),
            {"returncode": 1, "stdout": stdout, "stderr": stderr},
        )()

    with pytest.raises(ClickException, match=expected_message):
        cancel_reservation("job-1", runner=failing_runner, testflinger_bin="tf")


def test_cancel_cli_invokes_helper(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
) -> None:
    runner = CliRunner()
    captured: dict[str, object] = {}
    details = ReservationDetails(
        job_id="job-9",
        queue_name="ceph-qa-1",
        user="ubuntu",
        ip="10.0.0.9",
        expires_at=dt.datetime.now(),
        timeout_seconds=600,
    )
    save_latest_reservation(details)

    def fake_cancel_reservation(job_id: str, runner, testflinger_bin: str):
        captured["job_id"] = job_id
        captured["runner"] = runner
        captured["testflinger_bin"] = testflinger_bin

        class Result:
            stdout = "Cancelled job-9\n"
            stderr = ""

        return Result()

    monkeypatch.setattr(
        "cephtools.testflinger.cancel_reservation", fake_cancel_reservation
    )

    result = runner.invoke(cli, ["cancel", "job-9", "--testflinger-bin", "tf"])

    assert result.exit_code == 0
    assert result.output == "Cancelled job-9\n"
    assert captured["job_id"] == "job-9"
    assert captured["testflinger_bin"] == "tf"
    assert not latest_reservation_state_path().exists()


def test_save_and_load_latest_reservation(state_home: Path) -> None:
    details = ReservationDetails(
        job_id="job-12",
        queue_name="ceph-qa-2",
        user="ubuntu",
        ip="10.0.0.12",
        expires_at=dt.datetime(2024, 10, 16, 16, 0, 0),
        timeout_seconds=1800,
    )

    save_latest_reservation(details)

    assert latest_reservation_state_path().exists()
    assert load_latest_reservation_job_id() == "job-12"


def test_clear_latest_reservation_only_clears_matching_job(state_home: Path) -> None:
    details = ReservationDetails(
        job_id="job-22",
        queue_name="ceph-qa-2",
        user="ubuntu",
        ip="10.0.0.22",
        expires_at=dt.datetime(2024, 10, 16, 16, 0, 0),
        timeout_seconds=1800,
    )
    save_latest_reservation(details)

    clear_latest_reservation("other-job")
    assert latest_reservation_state_path().exists()

    clear_latest_reservation("job-22")
    assert not latest_reservation_state_path().exists()


def test_cancel_cli_latest_uses_saved_job(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
) -> None:
    runner = CliRunner()
    captured: dict[str, object] = {}
    save_latest_reservation(
        ReservationDetails(
            job_id="job-latest",
            queue_name="ceph-qa-1",
            user="ubuntu",
            ip="10.0.0.10",
            expires_at=dt.datetime.now(),
            timeout_seconds=600,
        )
    )

    def fake_cancel_reservation(job_id: str, runner, testflinger_bin: str):
        captured["job_id"] = job_id
        captured["testflinger_bin"] = testflinger_bin

        class Result:
            stdout = "Cancelled job-latest\n"
            stderr = ""

        return Result()

    monkeypatch.setattr(
        "cephtools.testflinger.cancel_reservation", fake_cancel_reservation
    )

    result = runner.invoke(cli, ["cancel", "--latest", "--testflinger-bin", "tf"])

    assert result.exit_code == 0
    assert result.output == "Cancelled job-latest\n"
    assert captured["job_id"] == "job-latest"
    assert captured["testflinger_bin"] == "tf"
    assert not latest_reservation_state_path().exists()


def test_cancel_cli_latest_without_saved_job(state_home: Path) -> None:
    runner = CliRunner()

    result = runner.invoke(cli, ["cancel", "--latest"])

    assert result.exit_code != 0
    assert "No saved Testflinger reservation found" in result.output


def test_cancel_cli_rejects_job_id_with_latest(state_home: Path) -> None:
    runner = CliRunner()

    result = runner.invoke(cli, ["cancel", "job-1", "--latest"])

    assert result.exit_code != 0
    assert "Pass either JOB_ID or --latest, not both." in result.output


def test_cancel_cli_requires_job_or_latest(state_home: Path) -> None:
    runner = CliRunner()

    result = runner.invoke(cli, ["cancel"])

    assert result.exit_code != 0
    assert "Missing JOB_ID. Pass a job id or use --latest." in result.output


def test_reserve_cli_saves_latest_reservation(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
) -> None:
    runner = CliRunner()
    details = ReservationDetails(
        job_id="job-saved",
        queue_name="ceph-qa-1",
        user="ubuntu",
        ip="10.0.0.30",
        expires_at=dt.datetime(2024, 10, 16, 16, 0, 0),
        timeout_seconds=1800,
    )

    monkeypatch.setattr(
        "cephtools.testflinger.ensure_backend_config",
        lambda *args, **kwargs: (BackendConfig("lp:tester"), False),
    )
    monkeypatch.setattr(
        "cephtools.testflinger._ssh_key_reference_warning", lambda value: None
    )
    monkeypatch.setattr("cephtools.testflinger.reserve_node", lambda **kwargs: details)
    monkeypatch.setattr(
        "cephtools.testflinger.print_reservation_summary", lambda *args, **kwargs: None
    )

    result = runner.invoke(
        cli,
        ["reserve", "ceph-qa-1", "--testflinger-bin", "tf"],
    )

    assert result.exit_code == 0
    assert load_latest_reservation_job_id() == "job-saved"


def test_parse_reservation_window_success() -> None:
    now = "2024-10-16T15:00:00.000000"
    expiry = "2024-10-16T16:00:00.000000"
    window = [
        "*** TESTFLINGER SYSTEM RESERVED ***",
        "You can now connect to ubuntu@10.0.0.1",
        f"Current time:           [{now}]",
        f"Reservation expires at: [{expiry}]",
        "Reservation will automatically timeout in 3600 seconds",
        "To end the reservation sooner use: testflinger-cli cancel job-1",
    ]

    details = _parse_reservation_window(window, "ceph-qa-1")
    assert details is not None
    assert details.job_id == "job-1"
    assert details.queue_name == "ceph-qa-1"
    assert details.user == "ubuntu"
    assert details.ip == "10.0.0.1"
    assert details.timeout_seconds == 3600
    assert details.expires_at == dt.datetime.fromisoformat(expiry)


def test_ensure_backend_config_creates_and_loads(tmp_path: Path) -> None:
    config_path = tmp_path / "backend.yaml"
    config, created = ensure_backend_config(
        config_path,
        launchpad_account="tester",
        job_tag=None,
        mattermost_name=None,
    )
    assert created is True
    assert config_path.exists()
    assert config.launchpad_account == "tester"

    config2, created2 = ensure_backend_config(
        config_path,
        launchpad_account=None,
        job_tag=None,
        mattermost_name=None,
    )
    assert created2 is False
    assert config2.launchpad_account == "tester"


def test_build_deploy_script() -> None:
    script = build_deploy_script()
    assert "releases/download/latest/cephtools" in script
    assert "sudo chmod 0755 /usr/local/bin/cephtools" in script
    assert 'test "$(cephtools testenv job protocol)" = "1"' in script
    assert "mkdir -p ~/src" in script
    assert "cd ~/src" in script
    assert "git clone https://github.com/canonical/cephtools.git" in script
    assert "uv pip install" not in script
    assert script.strip().endswith("cephtools testenv install")


def test_build_deploy_script_includes_testenv_args() -> None:
    script = build_deploy_script("--substrate maas-vm")
    assert script.strip().endswith("cephtools testenv --substrate maas-vm install")


def test_build_deploy_script_quotes_testenv_args() -> None:
    script = build_deploy_script("--maas-vm-memory '16 GiB'")
    assert "cephtools testenv --maas-vm-memory '16 GiB' install" in script


def test_perform_remote_deploy_invokes_ssh() -> None:
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def fake_runner(cmd: list[str], **kwargs: Any):
        calls.append((cmd, kwargs))

        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        return Result()

    details = ReservationDetails(
        job_id="job-1",
        queue_name="ceph-qa-1",
        user="ubuntu",
        ip="10.0.0.2",
        expires_at=dt.datetime.now(),
        timeout_seconds=600,
    )

    perform_remote_deploy(details, "echo hi", runner=fake_runner)

    assert calls
    cmd, kwargs = calls[0]
    assert cmd == [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "ubuntu@10.0.0.2",
        "bash",
        "-se",
    ]
    assert kwargs["input"] == "echo hi"
    assert kwargs["text"] is True
    assert kwargs["check"] is False


def test_perform_remote_deploy_failure() -> None:
    def failing_runner(cmd: list[str], **kwargs: Any):
        class Result:
            returncode = 42
            stdout = ""
            stderr = ""

        return Result()

    with pytest.raises(ClickException):
        perform_remote_deploy(
            ReservationDetails(
                job_id="job-1",
                queue_name="ceph-qa-1",
                user="ubuntu",
                ip="10.0.0.2",
                expires_at=dt.datetime.now(),
                timeout_seconds=600,
            ),
            "echo hi",
            runner=failing_runner,
        )


def _mk_details(job_id: str, ip: str = "10.0.0.2") -> ReservationDetails:
    return ReservationDetails(
        job_id=job_id,
        queue_name="ceph-qa-1",
        user="ubuntu",
        ip=ip,
        expires_at=dt.datetime(2024, 10, 16, 16, 0, 0),
        timeout_seconds=1800,
    )


def _raise_click(message: str) -> None:
    raise ClickException(message)


def _install_deploy_fakes(monkeypatch, *, perform_behaviour):
    """Monkeypatch the sub-steps of deploy_with_retries for loop testing.

    ``perform_behaviour`` is a list of callables, one per expected
    ``perform_remote_deploy`` invocation; each receives the job_id and may raise.
    Returns dicts capturing submit/await/perform/cancel calls.
    """
    submit_calls: list[str] = []
    await_calls: list[str] = []
    perform_calls: list[str] = []
    cancel_calls: list[str] = []

    def fake_submit(config, queue, rf, *, runner, testflinger_bin):
        jid = f"job-{len(submit_calls) + 1}"
        submit_calls.append(jid)
        return jid

    def fake_await(*, queue_name, job_id, testflinger_bin, echo):
        await_calls.append(job_id)
        return _mk_details(job_id, ip=f"10.0.0.{len(await_calls) + 1}")

    perform_iter = iter(perform_behaviour)

    def fake_perform(*, details, script, runner):
        perform_calls.append(details.job_id)
        next(perform_iter)(details.job_id)

    def fake_cancel(*, job_id, runner, testflinger_bin):
        cancel_calls.append(job_id)

    monkeypatch.setattr(testflinger, "submit_reserve_job", fake_submit)
    monkeypatch.setattr(testflinger, "await_reservation_details", fake_await)
    monkeypatch.setattr(testflinger, "perform_remote_deploy", fake_perform)
    monkeypatch.setattr(testflinger, "cancel_reservation", fake_cancel)
    monkeypatch.setattr(testflinger, "print_reservation_summary", lambda *a, **k: None)
    return {
        "submit": submit_calls,
        "await": await_calls,
        "perform": perform_calls,
        "cancel": cancel_calls,
    }


def _no_op_runner(**_kwargs: Any):
    raise AssertionError("runner should not be called directly by deploy_with_retries")


def test_deploy_with_retries_success_first_attempt(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
) -> None:
    calls = _install_deploy_fakes(
        monkeypatch,
        perform_behaviour=[lambda _jid: None],  # succeeds immediately
    )
    echo: list[str] = []

    details = testflinger.deploy_with_retries(
        queue_name="ceph-qa-1",
        reserve_for=600,
        config=BackendConfig("lp:tester"),
        testenv_args="",
        testflinger_bin="tf",
        max_attempts=2,
        runner=_no_op_runner,
        echo=echo.append,
    )

    assert details.job_id == "job-1"
    assert calls["submit"] == ["job-1"]
    assert calls["perform"] == ["job-1"]
    assert calls["cancel"] == []  # no cancellation on success
    assert load_latest_reservation_job_id() == "job-1"


def test_deploy_with_retries_succeeds_on_second_attempt(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
) -> None:
    calls = _install_deploy_fakes(
        monkeypatch,
        perform_behaviour=[
            lambda _jid: _raise_click("Remote deployment failed with exit code 1."),
            lambda _jid: None,  # second attempt succeeds
        ],
    )
    echo: list[str] = []

    details = testflinger.deploy_with_retries(
        queue_name="ceph-qa-1",
        reserve_for=600,
        config=BackendConfig("lp:tester"),
        testenv_args="",
        testflinger_bin="tf",
        max_attempts=2,
        runner=_no_op_runner,
        echo=echo.append,
    )

    assert details.job_id == "job-2"
    assert calls["submit"] == ["job-1", "job-2"]
    assert calls["perform"] == ["job-1", "job-2"]
    assert calls["cancel"] == ["job-1"]  # failed attempt's reservation cancelled
    assert load_latest_reservation_job_id() == "job-2"
    assert any("Retrying with a fresh reservation" in m for m in echo)


def test_deploy_with_retries_all_attempts_fail(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
) -> None:
    def always_fail(_jid):
        raise ClickException("Remote deployment failed with exit code 1.")

    calls = _install_deploy_fakes(
        monkeypatch, perform_behaviour=[always_fail, always_fail]
    )

    with pytest.raises(ClickException, match="Deploy failed after 2 attempt"):
        testflinger.deploy_with_retries(
            queue_name="ceph-qa-1",
            reserve_for=600,
            config=BackendConfig("lp:tester"),
            testenv_args="",
            testflinger_bin="tf",
            max_attempts=2,
            runner=_no_op_runner,
            echo=lambda _m: None,
        )

    assert calls["cancel"] == ["job-1", "job-2"]  # each failed attempt cancelled


def test_deploy_with_retries_cancels_when_await_fails(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
) -> None:
    # submit succeeds (job_id known), but await fails -> details is None, yet we
    # must still cancel the in-flight job.
    monkeypatch.setattr(
        testflinger,
        "submit_reserve_job",
        lambda *a, **k: "job-1",
    )
    monkeypatch.setattr(
        testflinger,
        "await_reservation_details",
        lambda **k: _raise_click("Failed to identify reservation details."),
    )
    perform_calls: list[str] = []
    monkeypatch.setattr(
        testflinger,
        "perform_remote_deploy",
        lambda *, details, script, runner: perform_calls.append(details.job_id),
    )
    cancel_calls: list[str] = []
    monkeypatch.setattr(
        testflinger,
        "cancel_reservation",
        lambda *, job_id, runner, testflinger_bin: cancel_calls.append(job_id),
    )
    monkeypatch.setattr(testflinger, "print_reservation_summary", lambda *a, **k: None)

    with pytest.raises(ClickException, match="Deploy failed after 1 attempt"):
        testflinger.deploy_with_retries(
            queue_name="ceph-qa-1",
            reserve_for=600,
            config=BackendConfig("lp:tester"),
            testenv_args="",
            testflinger_bin="tf",
            max_attempts=1,
            runner=_no_op_runner,
            echo=lambda _m: None,
        )

    assert perform_calls == []  # never got that far
    assert cancel_calls == ["job-1"]  # job_id was known before await failed


def test_deploy_with_retries_rejects_zero_attempts() -> None:
    with pytest.raises(ClickException, match="must be a positive integer"):
        testflinger.deploy_with_retries(
            queue_name="ceph-qa-1",
            reserve_for=600,
            config=BackendConfig("lp:tester"),
            testenv_args="",
            testflinger_bin="tf",
            max_attempts=0,
            runner=_no_op_runner,
            echo=lambda _m: None,
        )


def test_deploy_cli_passes_max_attempts(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
) -> None:
    runner = CliRunner()
    monkeypatch.setattr(
        "cephtools.testflinger.ensure_backend_config",
        lambda *a, **k: (BackendConfig("lp:tester"), False),
    )
    monkeypatch.setattr(
        "cephtools.testflinger._ssh_key_reference_warning", lambda v: None
    )
    captured: dict[str, Any] = {}

    def fake_deploy(**kwargs: Any) -> ReservationDetails:
        captured.update(kwargs)
        return _mk_details("job-1")

    monkeypatch.setattr("cephtools.testflinger.deploy_with_retries", fake_deploy)

    result = runner.invoke(
        cli,
        ["deploy", "ceph-qa-1", "--max-attempts", "3", "--testflinger-bin", "tf"],
    )

    assert result.exit_code == 0, result.output
    assert captured["max_attempts"] == 3


def test_read_testenv_network_config(tmp_path: Path) -> None:
    network_yaml = (
        "network:\n"
        "  bridge: lxdbr0\n"
        "  cidr: 10.0.0.0/24\n"
        "  gateway: 10.0.0.1\n"
        "  dynamic_range:\n"
        "    start: 10.0.0.100\n"
        "    end: 10.0.0.199\n"
        "  subnet_id: 1\n"
        "  fabric_id: 2\n"
        "  vlan_id: 3\n"
        "  rack_sysid: racksys-1\n"
        "  space_id: 4\n"
        "  external:\n"
        "    bridge: ext\n"
        "    cidr: 10.10.0.0/24\n"
        "    gateway: 10.10.0.1\n"
        "    dynamic_range:\n"
        "      start: 10.10.0.100\n"
        "      end: 10.10.0.199\n"
        "    subnet_id: 10\n"
        "    fabric_id: 11\n"
        "    vlan_id: 12\n"
        "    rack_sysid: racksys-2\n"
        "    space_id: 13\n"
    )
    path = tmp_path / "network.yaml"
    path.write_text(network_yaml)
    network = read_testenv_network_config(path)
    assert network["bridge"] == "lxdbr0"
    assert network["dynamic_range"]["start"] == "10.0.0.100"
    assert network["dynamic_range"]["end"] == "10.0.0.199"
    assert network["external"]["bridge"] == "ext"
    assert network["external"]["dynamic_range"]["end"] == "10.10.0.199"


def test_read_testenv_cloud_config(tmp_path: Path) -> None:
    cloud_yaml = (
        "clouds:\n"
        "  maas-cloud:\n"
        "    type: maas\n"
        "    auth-types: [oauth1]\n"
        "    endpoint: http://10.0.0.1:5240/MAAS\n"
    )
    path = tmp_path / "cloud.yaml"
    path.write_text(cloud_yaml)
    clouds = read_testenv_cloud_config(path)
    assert clouds["maas-cloud"]["auth-types"] == ["oauth1"]
    assert clouds["maas-cloud"]["endpoint"] == "http://10.0.0.1:5240/MAAS"


def test_read_testenv_credentials(tmp_path: Path) -> None:
    cred_yaml = (
        "credentials:\n"
        "  maas-cloud:\n"
        "    admin:\n"
        "      auth-type: oauth1\n"
        "      maas-oauth: AAA:BBB:CCC\n"
    )
    path = tmp_path / "cred.yaml"
    path.write_text(cred_yaml)
    creds = read_testenv_credentials(path)
    assert creds["maas-cloud"]["admin"]["maas-oauth"] == "AAA:BBB:CCC"


def _write_testenv_files(base: Path) -> None:
    base.mkdir(parents=True, exist_ok=True)
    (base / "cloud.yaml").write_text(
        "clouds:\n"
        "  maas-cloud:\n"
        "    type: maas\n"
        "    auth-types: [oauth1]\n"
        "    endpoint: http://10.0.0.1:5240/MAAS\n"
    )
    (base / "cred.yaml").write_text(
        "credentials:\n"
        "  maas-cloud:\n"
        "    admin:\n"
        "      auth-type: oauth1\n"
        "      maas-oauth: AAA:BBB:CCC\n"
    )
    (base / "network.yaml").write_text("network:\n  bridge: lxdbr0\n")


def test_machine_ids_returns_requested_count(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
) -> None:
    _write_testenv_files(state_home)

    def fake_run(cmd, check=True, capture_output=True, text=True):
        assert cmd[:3] == ["maas", "admin", "machines"]

        class Result:
            stdout = json.dumps(
                [
                    {"system_id": "0"},
                    {"system_id": "1"},
                    {"system_id": "2"},
                ]
            )
            stderr = ""

        return Result()

    monkeypatch.setattr("cephtools.testflinger.subprocess.run", fake_run)

    ids = machine_ids(2)
    assert ids == ["0", "1"]


def test_machine_ids_with_offset(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
) -> None:
    _write_testenv_files(state_home)

    def fake_run(cmd, check=True, capture_output=True, text=True):
        class Result:
            stdout = json.dumps(
                [
                    {"system_id": "10"},
                    {"system_id": "11"},
                    {"system_id": "12"},
                ]
            )
            stderr = ""

        return Result()

    monkeypatch.setattr("cephtools.testflinger.subprocess.run", fake_run)

    ids = machine_ids(1, offset=2)
    assert ids == ["12"]


def test_machine_ids_offset_out_of_range(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
) -> None:
    _write_testenv_files(state_home)

    def fake_run(cmd, check=True, capture_output=True, text=True):
        class Result:
            stdout = json.dumps([{"system_id": "5"}])
            stderr = ""

        return Result()

    monkeypatch.setattr("cephtools.testflinger.subprocess.run", fake_run)

    assert machine_ids(2, offset=5) == []


def test_machine_ids_invalid_count() -> None:
    with pytest.raises(ClickException):
        machine_ids(0)
