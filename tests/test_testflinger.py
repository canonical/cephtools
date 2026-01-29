from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import pytest
from click import ClickException

import cephtools.config as config_module
from cephtools.config import DEFAULT_TERRAFORM_ROOT, load_cephtools_config
from cephtools.testflinger import (
    BackendConfig,
    ReservationDetails,
    build_job_file,
    ensure_backend_config,
    parse_submit_output,
    _parse_reservation_window,
    build_deploy_script,
    perform_remote_deploy,
    read_testenv_network_config,
    read_testenv_cloud_config,
    read_testenv_credentials,
    machine_ids,
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
    assert "snap install astral-uv --classic" in script
    assert "mkdir -p ~/src" in script
    assert "cd ~/src" in script
    assert "git clone https://github.com/canonical/cephtools.git" in script
    assert 'export PATH="$PATH:$HOME/.local/bin"' in script
    assert script.strip().endswith("cephtools testenv install")


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


def test_load_cephtools_config_missing_file(state_home: Path) -> None:
    config = load_cephtools_config()
    assert config["terraform_root"] == str(DEFAULT_TERRAFORM_ROOT)
    assert config["juju_model"] == config_module.DEFAULT_JUJU_MODEL
    assert config["testenv"] == config_module.DEFAULT_TESTENV_DEFAULTS


def test_load_cephtools_config_with_section(state_home: Path) -> None:
    state_home.mkdir(parents=True, exist_ok=True)
    config_path = state_home / "cephtools.yaml"
    config_path.write_text(
        "cephtools:\n"
        "  terragrunt_dir: /srv/maas-nodes\n"
        "  juju_model: custom-model\n"
        "  paths:\n"
        "    terragrunt_dir: /ignored\n"
    )

    config = load_cephtools_config(ensure=True)
    assert config["terragrunt_dir"] == "/srv/maas-nodes"
    assert config["paths"] == {"terragrunt_dir": "/ignored"}
    assert config["juju_model"] == "custom-model"
    assert config["testenv"] == config_module.DEFAULT_TESTENV_DEFAULTS


def test_load_cephtools_config_top_level(state_home: Path) -> None:
    state_home.mkdir(parents=True, exist_ok=True)
    config_path = state_home / "cephtools.yaml"
    config_path.write_text("terragrunt_dir: /srv/custom\n")

    config = load_cephtools_config(ensure=True)
    assert config["terragrunt_dir"] == "/srv/custom"
    assert config["juju_model"] == config_module.DEFAULT_JUJU_MODEL
    assert config["testenv"] == config_module.DEFAULT_TESTENV_DEFAULTS
