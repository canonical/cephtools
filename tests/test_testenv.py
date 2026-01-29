from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest
from click import ClickException

import jubilant

from cephtools import testenv


@pytest.fixture
def state_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / "state"
    monkeypatch.setenv("CEPHTOOLS_STATE_HOME", str(home))
    return home


def test_get_lxd_vm_host_id(monkeypatch):
    def fake_run(cmd, check=True, shell=False, quiet=False):
        assert "vm-hosts read" in cmd

        class Result:
            stdout = json.dumps([{"name": "local-lxd", "id": 123}])

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    host_id = testenv._get_lxd_vm_host_id("admin", "local-lxd")
    assert host_id == "123"


def test_get_lxd_vm_host_id_missing(monkeypatch):
    def fake_run(cmd, check=True, shell=False, quiet=False):
        class Result:
            stdout = json.dumps([{"name": "other-host", "id": 1}])

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    with pytest.raises(ClickException):
        testenv._get_lxd_vm_host_id("admin", "local-lxd")


def test_register_lxd_vmhost_impl_skips_existing(monkeypatch):
    calls: list[str] = []
    echoes: list[str] = []

    monkeypatch.setattr(testenv, "_get_lxd_vm_host_id", lambda *_: "123")

    def fake_run(cmd, check=True, shell=False, quiet=False):
        calls.append(cmd)

        class Result:
            stdout = ""

        return Result()

    def fake_echo(message, **kwargs):
        echoes.append(message)

    monkeypatch.setattr(testenv, "run", fake_run)
    monkeypatch.setattr(testenv.click, "echo", fake_echo)

    testenv.register_lxd_vmhost_impl("admin", "local-lxd", "10.0.0.1", "secret")

    assert calls == []
    assert echoes and "skipping create" in echoes[0]


def test_register_lxd_vmhost_impl_creates_when_missing(monkeypatch):
    commands: list[str] = []

    def missing_host(*args, **kwargs):
        raise ClickException("not found")

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(cmd)

        class Result:
            stdout = ""

        return Result()

    monkeypatch.setattr(testenv, "_get_lxd_vm_host_id", missing_host)
    monkeypatch.setattr(testenv, "run", fake_run)

    testenv.register_lxd_vmhost_impl("admin", "local-lxd", "10.0.0.1", "secret")

    assert any("vm-hosts create type=lxd" in cmd for cmd in commands)


def test_get_vm_host_architectures(monkeypatch):
    def fake_run(cmd, check=True, shell=False, quiet=False):
        assert "vm-hosts read" in cmd

        class Result:
            stdout = json.dumps(
                [
                    {
                        "name": "local-lxd",
                        "architectures": [
                            testenv.REQUIRED_BOOT_ARCHITECTURE,
                            "arm64/generic",
                        ],
                    }
                ]
            )

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    architectures = testenv._get_vm_host_architectures("admin", "local-lxd")
    assert architectures == [
        testenv.REQUIRED_BOOT_ARCHITECTURE,
        "arm64/generic",
    ]


def test_wait_for_vm_host_architecture_success(monkeypatch):
    calls = {"count": 0}

    def fake_get_arches(admin, vmhost):
        calls["count"] += 1
        if calls["count"] < 3:
            return []
        return [testenv.REQUIRED_BOOT_ARCHITECTURE]

    now = {"value": 0.0}

    def fake_monotonic():
        return now["value"]

    def fake_sleep(seconds: float):
        now["value"] += seconds

    monkeypatch.setattr(testenv, "_get_vm_host_architectures", fake_get_arches)
    monkeypatch.setattr(testenv.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(testenv.time, "sleep", fake_sleep)

    testenv._wait_for_vm_host_architecture(
        "admin",
        "local-lxd",
        testenv.REQUIRED_BOOT_ARCHITECTURE,
        timeout=30,
        interval=5,
    )

    assert calls["count"] >= 3


def test_wait_for_vm_host_architecture_timeout(monkeypatch):
    def fake_get_arches(admin, vmhost):
        return []

    now = {"value": 0.0}

    def fake_monotonic():
        return now["value"]

    def fake_sleep(seconds: float):
        now["value"] += seconds

    monkeypatch.setattr(testenv, "_get_vm_host_architectures", fake_get_arches)
    monkeypatch.setattr(testenv.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(testenv.time, "sleep", fake_sleep)

    with pytest.raises(ClickException):
        testenv._wait_for_vm_host_architecture(
            "admin",
            "local-lxd",
            testenv.REQUIRED_BOOT_ARCHITECTURE,
            timeout=12,
            interval=4,
        )


def test_extract_arches():
    resources = [
        {"type": "Syncing", "architecture": "amd64/unfinished"},
        {
            "type": "Synced",
            "architecture": "amd64/ga-24.04",
            "subarches": "generic, hwe-24.04",
        },
        {"type": "Synced", "architecture": None},
    ]

    arches = testenv.extract_arches(resources)

    assert "amd64/ga-24.04" in arches
    assert "amd64/hwe-24.04" in arches
    assert "amd64/generic" in arches
    assert "amd64/unfinished" not in arches


def test_create_nodes_impl_invokes_terragrunt(
    monkeypatch, tmp_path: Path, state_home: Path
):
    calls: list[str] = []
    machine_updates: list[str] = []
    apply_calls: list[str] = []

    terragrunt_dir = tmp_path / "maas-nodes"
    terragrunt_dir.mkdir(parents=True)
    monkeypatch.setenv("CEPHTOOLS_TERRAGRUNT_DIR", str(terragrunt_dir))

    def fake_run(cmd, check=True, shell=False, quiet=False):
        if "vm-hosts read" in cmd:

            class Result:
                stdout = json.dumps([{"name": "local-lxd", "id": 321}])

            return Result()

        if "terragrunt output -json" in cmd:
            calls.append(cmd)

            class Result:
                stdout = json.dumps({"vm_hostnames": {"value": ["ceph-01"]}})

            return Result()

        if cmd == "maas admin tag machines cephtools":
            calls.append(cmd)

            class Result:
                stdout = json.dumps(
                    [
                        {
                            "hostname": "ceph-01",
                            "system_id": "node-1",
                            "status_name": "Deployed",
                        }
                    ]
                )

            return Result()

        if cmd == "maas admin machines read":
            calls.append(cmd)

            class Result:
                stdout = json.dumps([{"hostname": "ceph-01", "system_id": "node-1"}])

            return Result()

        if cmd.startswith("maas admin block-devices read"):
            calls.append(cmd)

            class Result:
                stdout = json.dumps(
                    [
                        {"id": 0, "used_for": "GPT partitioned"},
                        {"id": 1, "used_for": "Unused", "tags": []},
                    ]
                )

            return Result()

        if cmd.startswith("maas admin machine release"):
            calls.append(cmd)

            class Result:
                stdout = ""

            return Result()

        if cmd == "maas admin tags read":
            calls.append(cmd)

            class Result:
                stdout = "[]"

            return Result()

        if cmd.startswith("maas admin tags create"):
            calls.append(cmd)

            class Result:
                stdout = ""

            return Result()

        if cmd.startswith("maas admin block-device add-tag"):
            calls.append(cmd)

            class Result:
                stdout = ""

            return Result()

        if cmd.startswith("maas admin tag update-nodes"):
            machine_updates.append(cmd)

            class Result:
                stdout = ""

            return Result()

        if "terragrunt apply" in cmd:
            apply_calls.append(cmd)

        calls.append(cmd)

        class Result:
            stdout = ""

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)
    state_home.mkdir(parents=True, exist_ok=True)

    (state_home / "cloud.yaml").write_text(
        "clouds:\n"
        "  maas-cloud:\n"
        "    type: maas\n"
        "    auth-types: [oauth1]\n"
        "    endpoint: http://10.0.0.1:5240/MAAS\n"
    )
    (state_home / "cred.yaml").write_text(
        "credentials:\n"
        "  maas-cloud:\n"
        "    admin:\n"
        "      auth-type: oauth1\n"
        "      maas-oauth: KEY:VALUE\n"
    )
    (state_home / "network.yaml").write_text(
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
        "  rack_sysid: rack-1\n"
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
        "    rack_sysid: rack-2\n"
        "    space_id: 13\n"
    )

    testenv._create_nodes_impl(
        {"admin": "admin", "vmhost": "local-lxd"},
        vm_data_disk_size=64,
        vm_data_disk_count=2,
        vm_count=5,
    )

    assert apply_calls, "Terragrunt apply not invoked"
    apply_command = apply_calls[0]
    assert str(terragrunt_dir) in apply_command
    assert "-parallelism=1" in apply_command
    assert "-var" not in apply_command

    assert any("terragrunt output -json" in command for command in calls), (
        "Terragrunt output not inspected"
    )
    assert "maas admin tags create name=cephtools" in calls
    assert "maas admin block-devices read node-1" in calls
    assert "maas admin block-device add-tag node-1 1 tag=osd" in calls
    assert machine_updates == ["maas admin tag update-nodes cephtools add=node-1"]

    inputs_path = terragrunt_dir / "ensure-nodes.hcl"
    assert inputs_path.exists()
    contents = inputs_path.read_text()
    assert 'maas_api_url = "http://10.0.0.1:5240/MAAS"' in contents
    assert 'maas_api_key = "KEY:VALUE"' in contents
    assert "vm_data_disk_size = 64" in contents
    assert "vm_data_disk_count = 2" in contents
    assert "vm_count = 5" in contents
    assert 'primary_subnet_cidr = "10.0.0.0/24"' in contents
    assert 'external_subnet_cidr = "10.10.0.0/24"' in contents
    assert stat.S_IMODE(inputs_path.stat().st_mode) == 0o600


def test_destroy_nodes_impl_runs_terragrunt(monkeypatch, tmp_path: Path):
    terragrunt_dir = tmp_path / "maas-nodes"
    terragrunt_dir.mkdir(parents=True)
    inputs_path = terragrunt_dir / testenv.ENSURE_NODES_INPUT_FILENAME
    inputs_path.write_text("inputs = {}\n")

    monkeypatch.setenv("CEPHTOOLS_TERRAGRUNT_DIR", str(terragrunt_dir))

    commands: list[str] = []

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(cmd)

        class Result:
            stdout = ""

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    testenv._destroy_nodes_impl()

    assert commands, "terragrunt destroy was not invoked"
    assert any("terragrunt destroy" in cmd for cmd in commands)
    assert all(str(terragrunt_dir) in cmd for cmd in commands if "terragrunt" in cmd)


def test_destroy_nodes_impl_requires_inputs_file(monkeypatch, tmp_path: Path):
    terragrunt_dir = tmp_path / "maas-nodes"
    terragrunt_dir.mkdir(parents=True)
    monkeypatch.setenv("CEPHTOOLS_TERRAGRUNT_DIR", str(terragrunt_dir))

    with pytest.raises(ClickException):
        testenv._destroy_nodes_impl()


def test_resolve_terragrunt_dir_from_config(
    monkeypatch, tmp_path: Path, state_home: Path
):
    monkeypatch.delenv("CEPHTOOLS_TERRAGRUNT_DIR", raising=False)
    state_home.mkdir(parents=True, exist_ok=True)
    preferred_dir = tmp_path / "maas-nodes-config"
    preferred_dir.mkdir()
    (state_home / "cephtools.yaml").write_text(f"terragrunt_dir: {preferred_dir}\n")

    resolved = testenv._resolve_terragrunt_dir()
    assert resolved == preferred_dir


def test_resolve_terragrunt_dir_from_terraform_root(
    monkeypatch, tmp_path: Path, state_home: Path
):
    monkeypatch.delenv("CEPHTOOLS_TERRAGRUNT_DIR", raising=False)
    state_home.mkdir(parents=True, exist_ok=True)
    terraform_root = tmp_path / "terraform-root"
    terragrunt_dir = terraform_root / "maas-nodes"
    terragrunt_dir.mkdir(parents=True)
    (state_home / "cephtools.yaml").write_text(f"terraform_root: {terraform_root}\n")

    resolved = testenv._resolve_terragrunt_dir()
    assert resolved == terragrunt_dir


def test_ensure_juju_model_creates_and_sets_constraints(monkeypatch):
    calls: list[tuple] = []
    models_payload = {"models": []}

    class FakeJuju:
        def __init__(self, model: str | None = None, **_: object) -> None:
            self.model = model

        def cli(self, *args: str, include_model: bool = True, **__: object) -> str:
            calls.append(("cli", self.model, args, include_model))
            if args and args[0] == "models":
                return json.dumps(models_payload)
            return ""

        def add_model(self, model: str, **kwargs: object) -> None:
            self.model = model
            calls.append(("add_model", model, kwargs))

    monkeypatch.setattr(
        testenv.jubilant, "Juju", lambda *args, **kwargs: FakeJuju(*args, **kwargs)
    )

    testenv._ensure_juju_model("cephtools", constraint="tags=cephtools")

    assert calls == [
        (
            "cli",
            None,
            ("models", "--format", "json", "--controller", testenv.MAAS_CONTROLLER),
            False,
        ),
        ("add_model", "cephtools", {"controller": testenv.MAAS_CONTROLLER}),
        (
            "cli",
            f"{testenv.MAAS_CONTROLLER}:cephtools",
            ("set-model-constraints", "tags=cephtools"),
            True,
        ),
    ]


def test_ensure_juju_model_skips_existing(monkeypatch):
    calls: list[tuple] = []
    models_payload = {"models": [{"name": "cephtools"}]}

    class FakeJuju:
        def __init__(self, model: str | None = None, **_: object) -> None:
            self.model = model

        def cli(self, *args: str, include_model: bool = True, **__: object) -> str:
            calls.append(("cli", self.model, args, include_model))
            if args and args[0] == "models":
                return json.dumps(models_payload)
            return ""

        def add_model(self, model: str, **kwargs: object) -> None:
            self.model = model
            calls.append(("add_model", model, kwargs))

    monkeypatch.setattr(
        testenv.jubilant, "Juju", lambda *args, **kwargs: FakeJuju(*args, **kwargs)
    )

    testenv._ensure_juju_model("cephtools", constraint="tags=cephtools")

    assert calls == [
        (
            "cli",
            None,
            ("models", "--format", "json", "--controller", testenv.MAAS_CONTROLLER),
            False,
        ),
        (
            "cli",
            f"{testenv.MAAS_CONTROLLER}:cephtools",
            ("set-model-constraints", "tags=cephtools"),
            True,
        ),
    ]


class DummyJuju:
    def __init__(
        self,
        *,
        clouds: dict[str, dict] | None = None,
        credentials: dict[str, dict] | None = None,
        controllers: dict[str, dict] | None = None,
        clouds_payload: dict | None = None,
    ) -> None:
        self.clouds = dict(clouds or {})
        self.credentials = dict(credentials or {})
        self.controllers = dict(controllers or {})
        self.clouds_payload = clouds_payload
        self.add_cloud_calls = 0
        self.add_credential_calls = 0
        self.bootstrap_calls: list[tuple[str, str]] = []
        self.switch_calls: list[str] = []
        self.add_cloud_error: jubilant.CLIError | None = None
        self.add_credential_error: jubilant.CLIError | None = None

    def cli(self, command: str, *args: str, include_model: bool = True):
        if command == "clouds":
            if self.clouds_payload is not None:
                return json.dumps(self.clouds_payload)
            return json.dumps({"clouds": self.clouds})
        if command == "credentials":
            return json.dumps({"credentials": self.credentials})
        if command == "controllers":
            return json.dumps({"controllers": self.controllers})
        if command == "add-cloud":
            self.add_cloud_calls += 1
            if not args:
                raise AssertionError("Missing cloud name for add-cloud")
            if self.add_cloud_error is not None:
                raise self.add_cloud_error
            cloud_name = args[0]
            self.clouds.setdefault(cloud_name, {"type": "maas"})
            return ""
        if command == "add-credential":
            self.add_credential_calls += 1
            if not args:
                raise AssertionError("Missing cloud name for add-credential")
            if self.add_credential_error is not None:
                raise self.add_credential_error
            cloud_name = args[0]
            self.credentials.setdefault(cloud_name, {})["admin"] = {
                "auth-type": "oauth1"
            }
            return ""
        if command == "switch":
            if not args:
                raise AssertionError("Missing controller name for switch")
            self.switch_calls.append(args[0])
            return ""
        raise AssertionError(f"Unexpected juju command: {command}")

    def bootstrap(
        self,
        cloud: str,
        controller: str,
        *,
        bootstrap_constraints: dict | None = None,
        config: dict | None = None,
    ):
        self.bootstrap_calls.append((cloud, controller))
        self.controllers = {
            controller: {"controller-machines": {"Total": 1}},
        }


def test_juju_onboard_bootstraps_when_missing(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
):
    state_home.mkdir(parents=True, exist_ok=True)
    testenv.write_cloud_yaml("10.0.0.1")
    testenv.write_cred_yaml("test-key")

    juju = DummyJuju()
    monkeypatch.setattr(testenv.jubilant, "Juju", lambda *args, **kwargs: juju)
    monkeypatch.setattr(testenv.time, "sleep", lambda *args, **kwargs: None)

    bootstrapped = testenv.juju_onboard()

    assert bootstrapped is True
    assert juju.add_cloud_calls == 1
    assert juju.add_credential_calls == 1
    assert juju.bootstrap_calls == [("maas-cloud", "maas-controller")]
    assert juju.switch_calls == ["maas-controller"]
    assert "maas-controller" in juju.controllers


def test_juju_onboard_is_repeatable(monkeypatch: pytest.MonkeyPatch, state_home: Path):
    state_home.mkdir(parents=True, exist_ok=True)
    testenv.write_cloud_yaml("10.0.0.1")
    testenv.write_cred_yaml("test-key")

    juju = DummyJuju(
        clouds={"maas-cloud": {"type": "maas"}},
        credentials={"maas-cloud": {"admin": {"auth-type": "oauth1"}}},
        controllers={"maas-controller": {"controller-machines": {"Total": 1}}},
    )
    monkeypatch.setattr(testenv.jubilant, "Juju", lambda *args, **kwargs: juju)
    monkeypatch.setattr(testenv.time, "sleep", lambda *args, **kwargs: None)

    bootstrapped = testenv.juju_onboard()

    assert bootstrapped is False
    assert juju.add_cloud_calls == 0
    assert juju.add_credential_calls == 0
    assert juju.bootstrap_calls == []
    assert juju.switch_calls == ["maas-controller"]


def test_juju_onboard_detects_existing_cloud_mapping(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
):
    state_home.mkdir(parents=True, exist_ok=True)
    testenv.write_cloud_yaml("10.0.0.1")
    testenv.write_cred_yaml("test-key")

    juju = DummyJuju(
        clouds={},
        credentials={"maas-cloud": {"admin": {"auth-type": "oauth1"}}},
        controllers={"maas-controller": {"controller-machines": {"Total": 1}}},
        clouds_payload={
            "localhost": {"type": "lxd"},
            "maas-cloud": {"type": "maas"},
        },
    )
    monkeypatch.setattr(testenv.jubilant, "Juju", lambda *args, **kwargs: juju)
    monkeypatch.setattr(testenv.time, "sleep", lambda *args, **kwargs: None)

    bootstrapped = testenv.juju_onboard()

    assert bootstrapped is False
    assert juju.add_cloud_calls == 0
    assert juju.add_credential_calls == 0
    assert juju.bootstrap_calls == []
    assert juju.switch_calls == ["maas-controller"]


def test_juju_onboard_handles_cloud_exists_error(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
):
    state_home.mkdir(parents=True, exist_ok=True)
    testenv.write_cloud_yaml("10.0.0.1")
    testenv.write_cred_yaml("test-key")

    juju = DummyJuju(
        credentials={"maas-cloud": {"admin": {"auth-type": "oauth1"}}},
        controllers={"maas-controller": {"controller-machines": {"Total": 1}}},
        clouds_payload={"clouds": {}},
    )
    juju.add_cloud_error = jubilant.CLIError(
        1,
        ["juju", "add-cloud"],
        stderr='ERROR local cloud "maas-cloud" already exists',
    )
    monkeypatch.setattr(testenv.jubilant, "Juju", lambda *args, **kwargs: juju)
    monkeypatch.setattr(testenv.time, "sleep", lambda *args, **kwargs: None)

    bootstrapped = testenv.juju_onboard()

    assert bootstrapped is False
    assert juju.add_cloud_calls == 1
    assert juju.add_credential_calls == 0
    assert juju.bootstrap_calls == []
    assert juju.switch_calls == ["maas-controller"]
