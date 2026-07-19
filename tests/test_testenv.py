from __future__ import annotations

import json
import stat
import subprocess
from pathlib import Path

import pytest
from click import ClickException
from click.testing import CliRunner

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

    with pytest.raises(testenv.VMHostNotFound):
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

    testenv.register_lxd_vmhost_impl("admin", "local-lxd", "10.0.0.1")

    assert calls == []
    assert echoes and "skipping create" in echoes[0]


def test_register_lxd_vmhost_impl_uses_unlogged_one_time_password(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[tuple[object, bool, bool]] = []
    secret = "one-time-trust-password"

    def missing_host(*args, **kwargs):
        raise testenv.VMHostNotFound("not found")

    def fake_run(cmd, check=True, shell=False, quiet=False):
        calls.append((cmd, check, quiet))
        if not quiet:
            print(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(testenv, "_get_lxd_vm_host_id", missing_host)
    monkeypatch.setattr(testenv, "run", fake_run)
    monkeypatch.setattr(testenv.secrets, "token_urlsafe", lambda size: secret)

    testenv.register_lxd_vmhost_impl("admin", "local-lxd", "10.0.0.1")

    assert calls[0] == (
        ["lxc", "config", "set", "core.trust_password", secret],
        False,
        True,
    )
    enrollment_call = next(call for call in calls if isinstance(call[0], str))
    assert f'password="{secret}"' in enrollment_call[0]
    assert "--debug" not in enrollment_call[0]
    assert "|| true" not in enrollment_call[0]
    assert enrollment_call[2] is True
    assert calls[-1] == (
        ["lxc", "config", "unset", "core.trust_password"],
        True,
        True,
    )
    captured = capsys.readouterr()
    assert secret not in captured.out
    assert secret not in captured.err


def test_register_lxd_vmhost_impl_unsets_password_after_failure(monkeypatch):
    commands: list[tuple[object, bool, bool]] = []

    monkeypatch.setattr(
        testenv,
        "_get_lxd_vm_host_id",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            testenv.VMHostNotFound("not found")
        ),
    )
    monkeypatch.setattr(
        testenv.secrets, "token_urlsafe", lambda size: "one-time-trust-password"
    )

    def fake_run(command, check=True, quiet=False, **_kwargs):
        commands.append((command, check, quiet))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(testenv, "run", fake_run)
    monkeypatch.setattr(
        testenv,
        "_run_maas_cli",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("enroll failed")),
    )

    with pytest.raises(RuntimeError, match="enroll failed"):
        testenv.register_lxd_vmhost_impl("admin", "local-lxd", "10.0.0.1")

    assert commands == [
        (
            [
                "lxc",
                "config",
                "set",
                "core.trust_password",
                "one-time-trust-password",
            ],
            False,
            True,
        ),
        (["lxc", "config", "unset", "core.trust_password"], True, True),
    ]


def test_register_lxd_vmhost_impl_fails_when_success_cleanup_fails(monkeypatch):
    monkeypatch.setattr(
        testenv,
        "_get_lxd_vm_host_id",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            testenv.VMHostNotFound("not found")
        ),
    )
    monkeypatch.setattr(testenv.secrets, "token_urlsafe", lambda size: "secret")
    run_calls = 0

    def fake_run(*args, **_kwargs):
        nonlocal run_calls
        run_calls += 1
        if run_calls == 2:
            raise RuntimeError("cleanup failed")
        return subprocess.CompletedProcess(args[0], 0, stdout="", stderr="")

    monkeypatch.setattr(testenv, "run", fake_run)
    monkeypatch.setattr(
        testenv,
        "_run_maas_cli",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            "maas", 0, stdout="", stderr=""
        ),
    )

    with pytest.raises(RuntimeError, match="cleanup failed"):
        testenv.register_lxd_vmhost_impl("admin", "local-lxd", "10.0.0.1")


def test_register_lxd_vmhost_impl_surfaces_cleanup_failure(monkeypatch):
    monkeypatch.setattr(
        testenv,
        "_get_lxd_vm_host_id",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            testenv.VMHostNotFound("not found")
        ),
    )
    monkeypatch.setattr(testenv.secrets, "token_urlsafe", lambda size: "secret")
    run_calls = 0

    def fake_run(*args, **_kwargs):
        nonlocal run_calls
        run_calls += 1
        if run_calls == 2:
            raise RuntimeError("cleanup failed")
        return subprocess.CompletedProcess(args[0], 0, stdout="", stderr="")

    monkeypatch.setattr(testenv, "run", fake_run)
    monkeypatch.setattr(
        testenv,
        "_run_maas_cli",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("enroll failed")),
    )

    with pytest.raises(RuntimeError, match="enroll failed") as excinfo:
        testenv.register_lxd_vmhost_impl("admin", "local-lxd", "10.0.0.1")

    assert excinfo.value.__notes__ == [
        "Additionally failed to disable temporary LXD password trust."
    ]


def test_register_lxd_vmhost_impl_cleans_up_after_interrupt(monkeypatch):
    secret = "one-time-trust-password"
    commands: list[object] = []
    monkeypatch.setattr(
        testenv,
        "_get_lxd_vm_host_id",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            testenv.VMHostNotFound("not found")
        ),
    )
    monkeypatch.setattr(testenv.secrets, "token_urlsafe", lambda size: secret)

    def fake_run(command, **_kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(testenv, "run", fake_run)
    monkeypatch.setattr(
        testenv,
        "_run_maas_cli",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    with pytest.raises(KeyboardInterrupt):
        testenv.register_lxd_vmhost_impl("admin", "local-lxd", "10.0.0.1")

    assert commands[-1] == ["lxc", "config", "unset", "core.trust_password"]


def test_register_lxd_vmhost_impl_redacts_enrollment_failure(monkeypatch):
    secret = "one-time-trust-password"
    monkeypatch.setattr(
        testenv,
        "_get_lxd_vm_host_id",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            testenv.VMHostNotFound("not found")
        ),
    )
    monkeypatch.setattr(testenv.secrets, "token_urlsafe", lambda size: secret)
    monkeypatch.setattr(
        testenv,
        "run",
        lambda command, **_kwargs: subprocess.CompletedProcess(
            command, 0, stdout="", stderr=""
        ),
    )
    monkeypatch.setattr(
        testenv,
        "_run_maas_cli",
        lambda command, **_kwargs: subprocess.CompletedProcess(
            command, 1, stdout="", stderr=f"rejected password {secret}"
        ),
    )

    with pytest.raises(ClickException) as excinfo:
        testenv.register_lxd_vmhost_impl("admin", "local-lxd", "10.0.0.1")

    assert secret not in str(excinfo.value)
    assert "<redacted>" in str(excinfo.value)


def test_register_lxd_vmhost_impl_redacts_password_setup_exception(monkeypatch):
    secret = "one-time-trust-password"
    calls = 0
    monkeypatch.setattr(
        testenv,
        "_get_lxd_vm_host_id",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            testenv.VMHostNotFound("not found")
        ),
    )
    monkeypatch.setattr(testenv.secrets, "token_urlsafe", lambda size: secret)

    def fake_run(command, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise subprocess.CalledProcessError(1, command, stderr=f"bad {secret}")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(testenv, "run", fake_run)

    with pytest.raises(ClickException) as excinfo:
        testenv.register_lxd_vmhost_impl("admin", "local-lxd", "10.0.0.1")

    assert secret not in str(excinfo.value)
    assert calls == 2  # setup attempt followed by cleanup


def test_register_lxd_vmhost_impl_propagates_inspection_error(monkeypatch):
    calls: list[object] = []
    monkeypatch.setattr(
        testenv,
        "_get_lxd_vm_host_id",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ClickException("Failed to parse MAAS vm-hosts output as JSON.")
        ),
    )
    monkeypatch.setattr(testenv, "run", lambda *args, **kwargs: calls.append(args))

    with pytest.raises(ClickException, match="Failed to parse"):
        testenv.register_lxd_vmhost_impl("admin", "local-lxd", "10.0.0.1")

    assert calls == []


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


def test_install_maas_deb(monkeypatch):
    commands: list[object] = []

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(cmd)

        class Result:
            stdout = ""

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    testenv.install_maas_deb("3.7")

    assert commands == [
        [
            "sudo",
            "apt-get",
            "-y",
            "install",
            "software-properties-common",
            "postgresql",
        ],
        ["sudo", "apt-get", "-y", "remove", "systemd-timesyncd"],
        ["sudo", "apt-add-repository", "-y", "ppa:maas/3.7"],
        ["sudo", "apt-get", "update"],
        ["sudo", "apt-get", "-y", "install", "maas"],
    ]


def test_maas_init_impl_configures_postgres_backed_maas(monkeypatch):
    calls: list[object] = []

    monkeypatch.setattr(testenv, "_maas_is_initialized", lambda: False)
    monkeypatch.setattr(
        testenv,
        "_ensure_maas_postgres",
        lambda password: calls.append(("postgres", password)),
    )
    monkeypatch.setattr(
        testenv,
        "_configure_maas_region",
        lambda maas_url, db_password: calls.append(("region", maas_url, db_password)),
    )
    monkeypatch.setattr(
        testenv,
        "_ensure_maas_auth_ready",
        lambda: calls.append(("auth-ready",)),
    )
    monkeypatch.setattr(testenv, "_maas_admin_exists", lambda admin: False)
    monkeypatch.setattr(testenv.time, "sleep", lambda seconds: None)

    def fake_run(cmd, check=True, shell=False, quiet=False):
        calls.append(cmd)

        class Result:
            stdout = ""

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    testenv.maas_init_impl(
        "http://10.0.0.1:5240/MAAS",
        "admin",
        "secret",
        "ops@example.com",
    )

    assert calls == [
        ("postgres", "secret"),
        ("region", "http://10.0.0.1:5240/MAAS", "secret"),
        ("auth-ready",),
        [
            "sudo",
            "maas",
            "createadmin",
            "--username",
            "admin",
            "--password",
            "secret",
            "--email",
            "ops@example.com",
        ],
    ]


def test_configure_maas_region_restarts_temporal_services(monkeypatch):
    calls: list[object] = []

    def fake_run(cmd, check=True, shell=False, quiet=False):
        calls.append((cmd, check))

        class Result:
            stdout = ""

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    testenv._configure_maas_region("http://10.0.0.1:5240/MAAS", "secret")

    assert calls == [
        (
            [
                "sudo",
                "maas-region",
                "local_config_set",
                "--database-host",
                testenv.MAAS_DB_HOST,
                "--database-port",
                testenv.MAAS_DB_PORT,
                "--database-name",
                testenv.MAAS_DB_NAME,
                "--database-user",
                testenv.MAAS_DB_USER,
                "--database-pass",
                "secret",
                "--maas-url",
                "http://10.0.0.1:5240/MAAS",
            ],
            True,
        ),
        (["sudo", "maas-region", "dbupgrade"], True),
        (["sudo", "systemctl", "restart", "maas-regiond"], False),
        (["sudo", "systemctl", "restart", "maas-rackd"], False),
        (["sudo", "systemctl", "restart", "maas-apiserver"], False),
        (["sudo", "systemctl", "restart", "maas-http"], False),
        (["sudo", "systemctl", "restart", "maas-temporal"], False),
        (["sudo", "systemctl", "restart", "maas-temporal-worker"], False),
    ]


def test_lxd_init_impl_stops_and_restarts_bind9(monkeypatch):
    commands: list[object] = []
    echoes: list[str] = []
    ensured_networks: list[str] = []
    ensured_profile_networks: list[str] = []
    sleeps: list[float] = []
    waited: list[bool] = []
    init_calls: list[bool] = []

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append((cmd, check, shell))

        class Result:
            stdout = ""

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)
    monkeypatch.setattr(
        testenv, "_wait_for_bind9_shutdown", lambda: waited.append(True)
    )
    monkeypatch.setattr(
        testenv, "_run_lxd_minimal_init", lambda: init_calls.append(True)
    )
    monkeypatch.setattr(
        testenv,
        "ensure_lxd_network",
        lambda name, ipv4_address=None: ensured_networks.append(name),
    )
    monkeypatch.setattr(
        testenv,
        "ensure_lxd_default_profile_network",
        lambda name: ensured_profile_networks.append(name),
    )
    monkeypatch.setattr(testenv.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(
        testenv.click, "echo", lambda message, **kwargs: echoes.append(message)
    )

    testenv.lxd_init_impl("10.0.0.1", "lxdbr0")

    assert commands[0] == (["sudo", "systemctl", "stop", "bind9"], False, False)
    assert commands[1] == ("sudo snap set lxd daemon.user.group=adm", True, False)
    assert commands[2] == (
        ["sudo", "systemctl", "restart", "snap.lxd.daemon.service"],
        False,
        False,
    )
    assert commands[3] == ("sudo lxd waitready", True, False)
    assert commands[4] == (["lxc", "query", "/1.0"], True, False)
    assert commands[5] == (
        ["lxc", "config", "unset", "core.trust_password"],
        True,
        False,
    )
    assert commands[6] == (
        ["lxc", "config", "set", "core.https_address", ":8443"],
        True,
        False,
    )
    assert commands[7] == (["sudo", "systemctl", "start", "bind9"], False, False)
    assert not any(
        isinstance(command, list)
        and command[:4] == ["lxc", "config", "set", "core.trust_password"]
        for command, _check, _quiet in commands
    )
    assert waited == [True]
    assert init_calls == [True]
    assert ensured_networks == ["lxdbr0", testenv.EXT_LXD_NETWORK]
    assert ensured_profile_networks == ["lxdbr0"]
    assert sleeps == [2]
    assert echoes == [
        "Stopping bind9 temporarily so LXD bridge setup can claim port 53...",
        "Starting bind9 again after LXD bridge setup...",
    ]


def test_lxd_init_impl_restarts_bind9_on_failure(monkeypatch):
    commands: list[object] = []

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append((cmd, check, shell))
        if cmd == "sudo snap set lxd daemon.user.group=adm":
            raise RuntimeError("boom")

        class Result:
            stdout = ""

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)
    monkeypatch.setattr(testenv, "_wait_for_bind9_shutdown", lambda: None)
    monkeypatch.setattr(testenv, "_run_lxd_minimal_init", lambda: None)

    with pytest.raises(RuntimeError, match="boom"):
        testenv.lxd_init_impl("10.0.0.1", "lxdbr0")

    assert commands == [
        (["sudo", "systemctl", "stop", "bind9"], False, False),
        ("sudo snap set lxd daemon.user.group=adm", True, False),
        (["sudo", "systemctl", "start", "bind9"], False, False),
    ]


def test_wait_for_bind9_shutdown_waits_for_named_exit(monkeypatch):
    sleeps: list[float] = []
    now = {"value": 0.0}
    service_states = iter(["active", "inactive", "inactive"])
    named_processes = iter(["123 named\n", ""])

    def fake_run(cmd, check=True, shell=False, quiet=False):
        if cmd == ["systemctl", "is-active", "bind9"]:
            stdout = next(service_states)
        elif cmd == "pgrep -a -x named || true":
            stdout = next(named_processes)
        else:
            raise AssertionError(cmd)

        class Result:
            def __init__(self, stdout: str):
                self.stdout = stdout
                self.returncode = 0

        return Result(stdout)

    monkeypatch.setattr(testenv, "run", fake_run)
    monkeypatch.setattr(testenv.time, "monotonic", lambda: now["value"])
    monkeypatch.setattr(
        testenv.time,
        "sleep",
        lambda seconds: (
            sleeps.append(seconds),
            now.__setitem__("value", now["value"] + seconds),
        ),
    )

    testenv._wait_for_bind9_shutdown(timeout=5, interval=2)

    assert sleeps == [2]


def test_run_lxd_minimal_init_retries_after_failure(monkeypatch):
    init_attempts: list[int] = []
    diagnostics: list[bool] = []
    waited: list[bool] = []
    sleeps: list[float] = []
    echoes: list[str] = []

    def fake_run(cmd, check=True, shell=False, quiet=False):
        if cmd == ["sudo", "lxd", "init", "--minimal"]:
            init_attempts.append(len(init_attempts) + 1)
            if len(init_attempts) == 1:
                raise subprocess.CalledProcessError(1, cmd)

        class Result:
            stdout = ""
            returncode = 0

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)
    monkeypatch.setattr(testenv, "_lxd_is_minimally_initialized", lambda: False)
    monkeypatch.setattr(
        testenv, "_log_lxd_port_53_diagnostics", lambda: diagnostics.append(True)
    )
    monkeypatch.setattr(
        testenv, "_wait_for_bind9_shutdown", lambda: waited.append(True)
    )
    monkeypatch.setattr(testenv.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(
        testenv.click, "echo", lambda message, **kwargs: echoes.append(message)
    )

    testenv._run_lxd_minimal_init()

    assert init_attempts == [1, 2]
    assert diagnostics == [True]
    assert waited == [True]
    assert sleeps == [testenv.LXD_INIT_RETRY_DELAY_SECONDS]
    assert any("retrying once" in message for message in echoes)


def test_run_lxd_minimal_init_continues_on_partial_init(monkeypatch):
    init_attempts: list[int] = []
    diagnostics: list[bool] = []
    sleeps: list[float] = []
    echoes: list[str] = []

    def fake_run(cmd, check=True, shell=False, quiet=False):
        if cmd == ["sudo", "lxd", "init", "--minimal"]:
            init_attempts.append(1)
            raise subprocess.CalledProcessError(1, cmd)

        class Result:
            stdout = ""
            returncode = 0

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)
    monkeypatch.setattr(testenv, "_lxd_is_minimally_initialized", lambda: True)
    monkeypatch.setattr(
        testenv, "_log_lxd_port_53_diagnostics", lambda: diagnostics.append(True)
    )
    monkeypatch.setattr(testenv, "_wait_for_bind9_shutdown", lambda: None)
    monkeypatch.setattr(testenv.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(
        testenv.click, "echo", lambda message, **kwargs: echoes.append(message)
    )

    testenv._run_lxd_minimal_init()

    assert init_attempts == [1]
    assert diagnostics == [True]
    assert sleeps == []
    assert any("partially initialized" in message for message in echoes)


def test_bind9_ipv4_listen_addresses(monkeypatch):
    def fake_run(cmd, check=True, shell=False, quiet=False):
        assert cmd == ["ip", "-j", "-4", "addr", "show"]

        class Result:
            stdout = json.dumps(
                [
                    {
                        "ifname": "lo",
                        "addr_info": [{"family": "inet", "local": "127.0.0.1"}],
                    },
                    {
                        "ifname": "eno49",
                        "addr_info": [{"family": "inet", "local": "10.241.21.59"}],
                    },
                    {
                        "ifname": "lxdbr0",
                        "addr_info": [{"family": "inet", "local": "10.241.99.1"}],
                    },
                    {
                        "ifname": "ext",
                        "addr_info": [{"family": "inet", "local": "10.241.88.1"}],
                    },
                ]
            )

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    addresses = testenv._bind9_ipv4_listen_addresses()

    assert addresses == ["127.0.0.1", "10.241.21.59", "10.241.99.1", "10.241.88.1"]


def test_configure_maas_bind9_ipv4(monkeypatch):
    commands: list[object] = []
    echoes: list[str] = []

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(cmd)

        class Result:
            stdout = ""

        return Result()

    monkeypatch.setattr(
        testenv,
        "_bind9_ipv4_listen_addresses",
        lambda: ["127.0.0.1", "10.241.21.59", "10.241.99.1", "10.241.88.1"],
    )
    monkeypatch.setattr(testenv, "run", fake_run)
    monkeypatch.setattr(
        testenv.click, "echo", lambda message, **kwargs: echoes.append(message)
    )

    testenv.configure_maas_bind9_ipv4()

    assert echoes == [
        "Configuring MAAS bind9 IPv4 listen-on policy on detected addresses: 127.0.0.1, 10.241.21.59, 10.241.99.1, 10.241.88.1"
    ]
    assert len(commands) == 3
    assert isinstance(commands[0], str)
    assert (
        "listen-on { 127.0.0.1; 10.241.21.59; 10.241.99.1; 10.241.88.1; };"
        in commands[0]
    )
    assert commands[1] == ["sudo", "named-checkconf"]
    assert commands[2] == ["sudo", "systemctl", "reload", "bind9"]


def test_ensure_lxd_network_creates_without_dns_or_dhcp(monkeypatch):
    commands: list[object] = []

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(cmd)

        class Result:
            stdout = "[]" if cmd == "lxc query /1.0/networks" else ""

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    testenv.ensure_lxd_network("ext")

    assert commands == [
        "lxc query /1.0/networks",
        "lxc network create ext ipv4.address=auto ipv4.nat=true ipv4.dhcp=false ipv6.address=none ipv6.dhcp=false dns.mode=none raw.dnsmasq=port=0",
        ["lxc", "network", "set", "ext", "dns.mode=none"],
        ["lxc", "network", "set", "ext", "raw.dnsmasq=port=0"],
        ["lxc", "network", "set", "ext", "ipv4.dhcp=false"],
        ["lxc", "network", "set", "ext", "ipv6.dhcp=false"],
    ]


def test_ensure_lxd_network_updates_existing_network(monkeypatch):
    commands: list[object] = []

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(cmd)

        class Result:
            stdout = json.dumps(["/1.0/networks/lxdbr0"])

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    testenv.ensure_lxd_network("lxdbr0")

    assert commands == [
        "lxc query /1.0/networks",
        ["lxc", "network", "set", "lxdbr0", "dns.mode=none"],
        ["lxc", "network", "set", "lxdbr0", "raw.dnsmasq=port=0"],
        ["lxc", "network", "set", "lxdbr0", "ipv4.dhcp=false"],
        ["lxc", "network", "set", "lxdbr0", "ipv6.dhcp=false"],
    ]


def test_ensure_lxd_host_network_creates_with_dns_and_dhcp(monkeypatch):
    commands: list[object] = []

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(cmd)

        class Result:
            stdout = "[]" if cmd == "lxc query /1.0/networks" else ""

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    testenv.ensure_lxd_host_network("lxdbr0")

    assert commands == [
        "lxc query /1.0/networks",
        "lxc network create lxdbr0 ipv4.address=auto ipv4.nat=true ipv4.dhcp=true ipv6.address=none ipv6.dhcp=false dns.mode=managed",
        ["lxc", "network", "set", "lxdbr0", "dns.mode=managed"],
        ["lxc", "network", "set", "lxdbr0", "ipv4.dhcp=true"],
        ["lxc", "network", "set", "lxdbr0", "ipv6.dhcp=false"],
        ["lxc", "network", "unset", "lxdbr0", "raw.dnsmasq"],
    ]


def test_ensure_lxd_host_network_clears_stale_dnsmasq_override(monkeypatch):
    commands: list[object] = []

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(cmd)

        class Result:
            stdout = json.dumps(["/1.0/networks/lxdbr0"])

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    testenv.ensure_lxd_host_network("lxdbr0")

    assert commands == [
        "lxc query /1.0/networks",
        ["lxc", "network", "set", "lxdbr0", "dns.mode=managed"],
        ["lxc", "network", "set", "lxdbr0", "ipv4.dhcp=true"],
        ["lxc", "network", "set", "lxdbr0", "ipv6.dhcp=false"],
        ["lxc", "network", "unset", "lxdbr0", "raw.dnsmasq"],
    ]


def test_render_maas_vm_bootstrap_script_uses_maas_snap():
    script = testenv.render_maas_vm_bootstrap_script(
        "http://10.0.0.2:5240/MAAS",
        "admin",
        "secret",
        "admin@example.com",
        "3.7",
    )

    assert "snap install maas-test-db" in script
    assert "snap install maas --channel=3.7/stable" in script
    assert "maas init region+rack --database-uri maas-test-db:///" in script
    assert "maas createadmin --username admin" in script


def test_lxd_init_vm_impl_does_not_touch_bind9(monkeypatch):
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr(
        testenv,
        "_configure_lxd_common",
        lambda: calls.append(("common", None)),
    )
    monkeypatch.setattr(
        testenv,
        "ensure_lxd_host_network",
        lambda name: calls.append(("host-network", name)),
    )
    monkeypatch.setattr(
        testenv,
        "ensure_lxd_default_profile_network",
        lambda name: calls.append(("profile", name)),
    )
    monkeypatch.setattr(
        testenv,
        "ensure_lxd_maas_network",
        lambda name: calls.append(("maas-network", name)),
    )
    monkeypatch.setattr(
        testenv,
        "ensure_lxd_maas_project",
        lambda project, network: calls.append(("project", (project, network))),
    )
    monkeypatch.setattr(
        testenv.time, "sleep", lambda seconds: calls.append(("sleep", seconds))
    )

    testenv.lxd_init_vm_impl("lxdbr0", "maasbr0", "maas")

    assert calls == [
        ("common", None),
        ("host-network", "lxdbr0"),
        ("profile", "lxdbr0"),
        ("maas-network", "maasbr0"),
        ("project", ("maas", "maasbr0")),
        ("sleep", 2),
    ]


def test_lxd_init_lxd_impl_creates_ext_as_host_network(monkeypatch):
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr(
        testenv,
        "_configure_lxd_common",
        lambda: calls.append(("common", None)),
    )
    monkeypatch.setattr(
        testenv,
        "ensure_lxd_host_network",
        lambda name: calls.append(("host-network", name)),
    )
    monkeypatch.setattr(
        testenv,
        "ensure_lxd_default_profile_network",
        lambda name: calls.append(("profile", name)),
    )
    monkeypatch.setattr(
        testenv.time, "sleep", lambda seconds: calls.append(("sleep", seconds))
    )

    testenv.lxd_init_lxd_impl("lxdbr0")

    assert calls == [
        ("common", None),
        ("host-network", "lxdbr0"),
        ("profile", "lxdbr0"),
        ("host-network", testenv.EXT_LXD_NETWORK),
        ("sleep", 2),
    ]


def test_ensure_lxd_default_profile_network_adds_eth0(monkeypatch):
    commands: list[object] = []

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(cmd)

        class Result:
            stdout = json.dumps({"devices": {"root": {"type": "disk"}}})

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    testenv.ensure_lxd_default_profile_network("lxdbr0")

    assert commands == [
        "lxc query /1.0/profiles/default",
        [
            "lxc",
            "profile",
            "device",
            "add",
            "default",
            "eth0",
            "nic",
            "network=lxdbr0",
            "name=eth0",
        ],
    ]


def test_ensure_lxd_default_profile_network_updates_existing_eth0(monkeypatch):
    commands: list[object] = []

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(cmd)

        class Result:
            stdout = json.dumps(
                {
                    "devices": {
                        "eth0": {"type": "nic", "network": "oldnet", "name": "eth0"}
                    }
                }
            )

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    testenv.ensure_lxd_default_profile_network("lxdbr0")

    assert commands == [
        "lxc query /1.0/profiles/default",
        [
            "lxc",
            "profile",
            "device",
            "set",
            "default",
            "eth0",
            "network=lxdbr0",
            "name=eth0",
        ],
    ]


def test_verify_maas_checks_systemd_services(monkeypatch):
    commands: list[object] = []

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(cmd)

        class Result:
            stdout = ""
            returncode = 0

        if cmd == ["sudo", "systemctl", "is-active", "--quiet", "maas-regiond"]:
            Result.returncode = 0
        elif cmd == ["sudo", "systemctl", "is-active", "--quiet", "maas-rackd"]:
            Result.returncode = 0
        elif cmd == "maas admin boot-resources read":
            Result.stdout = "[]"
        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    testenv.verify_maas("admin")

    assert commands == [
        ["sudo", "systemctl", "is-active", "--quiet", "maas-regiond"],
        ["sudo", "systemctl", "is-active", "--quiet", "maas-rackd"],
        "maas admin boot-resources read",
    ]


def test_verify_maas_raises_when_service_inactive(monkeypatch):
    def fake_run(cmd, check=True, shell=False, quiet=False):
        class Result:
            stdout = ""
            returncode = 0

        if cmd == ["sudo", "systemctl", "is-active", "--quiet", "maas-regiond"]:
            Result.returncode = 0
        elif cmd == ["sudo", "systemctl", "is-active", "--quiet", "maas-rackd"]:
            Result.returncode = 3
        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    with pytest.raises(RuntimeError):
        testenv.verify_maas("admin")


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


def test_dns_preflight_restarts_resolver(monkeypatch):
    commands: list[str] = []

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(str(cmd))

        class Result:
            stdout = ""

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)
    monkeypatch.setattr(testenv, "_resolve_hostname", lambda host: True)

    testenv.dns_preflight(hosts=("registry.terraform.io",), timeout=1, interval=1)

    assert "sudo resolvectl flush-caches || true" in commands
    assert "sudo systemctl restart systemd-resolved || true" in commands


def test_dns_preflight_raises_when_unresolved(monkeypatch):
    commands: list[str] = []
    now = {"value": 0.0}

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(str(cmd))

        class Result:
            stdout = ""

        return Result()

    def fake_monotonic():
        return now["value"]

    def fake_sleep(seconds: float):
        now["value"] += seconds

    monkeypatch.setattr(testenv, "run", fake_run)
    monkeypatch.setattr(testenv, "_resolve_hostname", lambda host: False)
    monkeypatch.setattr(testenv.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(testenv.time, "sleep", fake_sleep)

    with pytest.raises(ClickException, match="unresolved hosts: registry.terraform.io"):
        testenv.dns_preflight(hosts=("registry.terraform.io",), timeout=2, interval=1)

    assert "resolvectl status || true" in commands


def _fake_lxd_network(network_name: str):
    if network_name == "lxdbr0":
        return "10.10.0.0/24", "10.10.0.1"
    if network_name == testenv.EXT_LXD_NETWORK:
        return "10.20.0.0/24", "10.20.0.1"
    raise AssertionError(network_name)


def _run_configure_lxd_juju_network(monkeypatch, spaces_json: str):
    calls: list[object] = []

    def fake_run(cmd, check=True, shell=False, quiet=False):
        calls.append((cmd, check))

        class Result:
            stdout = spaces_json if cmd[:2] == ["juju", "spaces"] else ""
            returncode = 0

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)
    monkeypatch.setattr(testenv, "lxd_network_cidr_and_gateway", _fake_lxd_network)

    testenv.configure_lxd_juju_network(
        controller=testenv.LXD_CONTROLLER, model="cephtools", lxdbridge="lxdbr0"
    )
    return calls


def test_configure_lxd_juju_network_writes_reduced_network_yaml(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
) -> None:
    # ext subnet not yet assigned to the external space -> move-to-space runs.
    calls = _run_configure_lxd_juju_network(monkeypatch, spaces_json="{}")

    model_ref = f"{testenv.LXD_CONTROLLER}:cephtools"
    assert calls == [
        (["juju", "reload-spaces", "-m", model_ref], True),
        (["juju", "add-space", testenv.EXTERNAL_SPACE_NAME, "-m", model_ref], False),
        (["juju", "spaces", "--format", "json", "-m", model_ref], True),
        (
            [
                "juju",
                "move-to-space",
                testenv.EXTERNAL_SPACE_NAME,
                "10.20.0.0/24",
                "-m",
                model_ref,
            ],
            True,
        ),
    ]
    assert (state_home / "network.yaml").read_text() == (
        "network:\n"
        "  substrate: lxd\n"
        "  bridge: lxdbr0\n"
        "  cidr: 10.10.0.0/24\n"
        "  gateway: 10.10.0.1\n"
        "  external:\n"
        "    bridge: ext\n"
        "    cidr: 10.20.0.0/24\n"
        "    gateway: 10.20.0.1\n"
        "    space: external\n"
    )


def test_configure_lxd_juju_network_is_idempotent_when_subnet_already_moved(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
) -> None:
    # ext subnet already in the external space -> move-to-space is skipped, so a
    # repeated install/configure-network run does not abort.
    spaces_json = json.dumps(
        {
            "spaces": [
                {"name": "alpha", "subnets": {"10.10.0.0/24": {}}},
                {
                    "name": testenv.EXTERNAL_SPACE_NAME,
                    "subnets": {"10.20.0.0/24": {}},
                },
            ]
        }
    )
    calls = _run_configure_lxd_juju_network(monkeypatch, spaces_json=spaces_json)

    move_calls = [
        cmd for (cmd, _check) in calls if cmd[:2] == ["juju", "move-to-space"]
    ]
    assert move_calls == []
    assert (state_home / "network.yaml").exists()


def test_create_lxd_nodes_impl_adds_machines_and_block_volumes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[object] = []

    monkeypatch.setattr(testenv, "_juju_machine_instance_ids", lambda *_: {})
    monkeypatch.setattr(
        testenv,
        "_wait_for_lxd_juju_machines",
        lambda *_args, **_kwargs: {"0": "juju-vm-0", "1": "juju-vm-1"},
    )
    monkeypatch.setattr(testenv, "_default_lxd_storage_pool", lambda: "default")
    monkeypatch.setattr(testenv, "_lxd_storage_volume_exists", lambda *_args: False)
    monkeypatch.setattr(testenv, "_lxd_instance_device_names", lambda *_args: set())

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(cmd)

        class Result:
            stdout = ""
            returncode = 0

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    testenv._create_lxd_nodes_impl(
        {"substrate": testenv.SUBSTRATE_LXD},
        vm_data_disk_size=8,
        vm_data_disk_count=2,
        vm_count=2,
    )

    assert commands[0] == [
        "juju",
        "add-machine",
        "-n",
        "2",
        "--constraints",
        "virt-type=virtual-machine",
        "-m",
        f"{testenv.LXD_CONTROLLER}:{testenv.CEPHTOOLS_MODEL}",
    ]
    assert [
        "lxc",
        "storage",
        "volume",
        "create",
        "default",
        "cephtools-0-osd-0",
        "--type=block",
        "size=8GiB",
    ] in commands
    assert [
        "lxc",
        "config",
        "device",
        "add",
        "juju-vm-1",
        "osd-1",
        "disk",
        "pool=default",
        "source=cephtools-1-osd-1",
    ] in commands


def test_destroy_lxd_nodes_impl_detaches_disks_removes_volumes_and_machines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[object] = []

    monkeypatch.setattr(
        testenv,
        "_juju_machine_instance_ids",
        lambda *_args: {"1": "juju-vm-1", "0": "juju-vm-0"},
    )
    monkeypatch.setattr(
        testenv,
        "_lxd_instance_device_names",
        lambda instance: {"root", "osd-1", "osd-0"}
        if instance == "juju-vm-0"
        else {"root", "osd-0"},
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_lxd_osd_volumes",
        lambda: testenv.CleanupPhaseResult("delete LXD OSD volumes", "ok", "removed"),
    )

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(cmd)

        class Result:
            stdout = ""
            returncode = 0

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    testenv._destroy_lxd_nodes_impl({"substrate": testenv.SUBSTRATE_LXD})

    assert ["lxc", "config", "device", "remove", "juju-vm-0", "osd-0"] in commands
    assert ["lxc", "config", "device", "remove", "juju-vm-1", "osd-0"] in commands
    assert commands[-1] == [
        "juju",
        "remove-machine",
        "0",
        "1",
        "--force",
        "--no-wait",
        "--no-prompt",
        "-m",
        f"{testenv.LXD_CONTROLLER}:{testenv.CEPHTOOLS_MODEL}",
    ]


def test_cleanup_lxd_osd_volumes_deletes_only_exact_generated_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    valid_name = testenv._lxd_osd_volume_name("12", 3)
    volumes = [
        {"name": valid_name, "type": "custom", "content_type": "block"},
        {"name": "cephtools-backup", "type": "custom", "content_type": "block"},
        {"name": "cephtools-12-data-3", "type": "custom", "content_type": "block"},
        {"name": "cephtools-host-osd-3", "type": "custom", "content_type": "block"},
        {"name": "cephtools-12-osd-disk", "type": "custom", "content_type": "block"},
        {"name": "cephtools-12-osd-3-copy", "type": "custom", "content_type": "block"},
        {"name": "other-12-osd-3", "type": "custom", "content_type": "block"},
        {"name": "cephtools-13-osd-4", "type": "custom", "content_type": "filesystem"},
        {"name": "cephtools-14-osd-5", "type": "container", "content_type": "block"},
    ]
    commands: list[list[str]] = []

    def fake_run(command, **_kwargs):
        commands.append(command)
        if command[:5] == ["lxc", "storage", "volume", "list", "default"]:
            return subprocess.CompletedProcess(
                command, 0, stdout=json.dumps(volumes), stderr=""
            )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(testenv.shutil, "which", lambda name: "/snap/bin/lxc")
    monkeypatch.setattr(testenv, "_default_lxd_storage_pool", lambda: "default")
    monkeypatch.setattr(testenv, "run", fake_run)

    result = testenv._cleanup_lxd_osd_volumes()

    assert result.outcome == "ok"
    assert result.detail == f"deleted {valid_name}"
    assert commands[1:] == [
        ["lxc", "storage", "volume", "delete", "default", f"custom/{valid_name}"]
    ]


def test_cleanup_lxd_osd_volumes_skips_when_no_exact_names_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    volumes = [
        {"name": "cephtools-backup", "type": "custom", "content_type": "block"},
        {"name": "cephtools-0-osd-x", "type": "custom", "content_type": "block"},
        {"name": "cephtools-0-osd-0", "type": "custom", "content_type": "filesystem"},
    ]
    commands: list[list[str]] = []

    def fake_run(command, **_kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(
            command, 0, stdout=json.dumps(volumes), stderr=""
        )

    monkeypatch.setattr(testenv.shutil, "which", lambda name: "/snap/bin/lxc")
    monkeypatch.setattr(testenv, "_default_lxd_storage_pool", lambda: "default")
    monkeypatch.setattr(testenv, "run", fake_run)

    result = testenv._cleanup_lxd_osd_volumes()

    assert result.outcome == "skipped"
    assert result.detail == "no matching LXD OSD volumes"
    assert len(commands) == 1


def test_cleanup_lxd_osd_volumes_dry_run_describes_exact_pattern() -> None:
    result = testenv._cleanup_lxd_osd_volumes(dry_run=True)

    assert result.outcome == "ok"
    assert "cephtools-<machine-id>-osd-<disk-index>" in result.detail
    assert "prefixed" not in result.detail


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


def test_resolve_terragrunt_dir_from_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    preferred_dir = tmp_path / "maas-nodes"
    preferred_dir.mkdir()
    monkeypatch.setenv("CEPHTOOLS_TERRAGRUNT_DIR", str(preferred_dir))

    resolved = testenv._resolve_terragrunt_dir()
    assert resolved == preferred_dir


def test_resolve_terragrunt_dir_from_terraform_root_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("CEPHTOOLS_TERRAGRUNT_DIR", raising=False)
    terraform_root = tmp_path / "terraform-root"
    terragrunt_dir = terraform_root / "maas-nodes"
    terragrunt_dir.mkdir(parents=True)
    monkeypatch.setenv("CEPHTOOLS_TERRAFORM_ROOT", str(terraform_root))

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


def test_ensure_juju_model_accepts_controller_and_constraint(monkeypatch):
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
            calls.append(("add_model", model, kwargs))

    monkeypatch.setattr(
        testenv.jubilant, "Juju", lambda *args, **kwargs: FakeJuju(*args, **kwargs)
    )

    testenv._ensure_juju_model(
        "cephtools",
        controller=testenv.LXD_CONTROLLER,
        constraint="virt-type=virtual-machine",
    )

    assert calls == [
        (
            "cli",
            None,
            ("models", "--format", "json", "--controller", testenv.LXD_CONTROLLER),
            False,
        ),
        ("add_model", "cephtools", {"controller": testenv.LXD_CONTROLLER}),
        (
            "cli",
            f"{testenv.LXD_CONTROLLER}:cephtools",
            ("set-model-constraints", "virt-type=virtual-machine"),
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
        self.bootstrap_kwargs: list[dict[str, object]] = []
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
        self.bootstrap_kwargs.append(
            {"bootstrap_constraints": bootstrap_constraints, "config": config}
        )
        self.controllers = {
            controller: {"controller-machines": {"Total": 1}},
        }


@pytest.mark.parametrize(
    ("substrate", "expected_calls"),
    [
        (
            testenv.SUBSTRATE_LXD,
            ["snap:lxd:False", "snap:terraform:True", "snap:juju:False", "lxd-ready"],
        ),
        (
            testenv.SUBSTRATE_MAAS_VM,
            ["snap:lxd:False", "snap:terraform:True", "terragrunt", "lxd-ready"],
        ),
        (
            testenv.SUBSTRATE_MAAS_HOST,
            [
                f"maas:{testenv.DEFAULT_MAAS_VERSION}",
                "snap:lxd:False",
                "snap:terraform:True",
                "terragrunt",
                "lxd-ready",
            ],
        ),
    ],
)
def test_install_deps_installs_terraform_for_every_substrate(
    monkeypatch: pytest.MonkeyPatch,
    substrate: str,
    expected_calls: list[str],
) -> None:
    calls: list[str] = []
    runner = CliRunner()

    monkeypatch.setattr(
        testenv,
        "install_maas_deb",
        lambda version: calls.append(f"maas:{version}"),
    )
    monkeypatch.setattr(
        testenv,
        "ensure_snap",
        lambda name, classic=False: calls.append(f"snap:{name}:{classic}"),
    )
    monkeypatch.setattr(
        testenv, "ensure_terragrunt", lambda: calls.append("terragrunt")
    )
    monkeypatch.setattr(testenv, "lxd_ready", lambda: calls.append("lxd-ready"))

    result = runner.invoke(testenv.cli, ["--substrate", substrate, "install-deps"])

    assert result.exit_code == 0
    assert calls == expected_calls


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


def test_juju_onboard_lxd_bootstraps_localhost(
    monkeypatch: pytest.MonkeyPatch, state_home: Path
):
    state_home.mkdir(parents=True, exist_ok=True)
    juju = DummyJuju()
    monkeypatch.setattr(testenv.jubilant, "Juju", lambda *args, **kwargs: juju)
    monkeypatch.setattr(testenv.time, "sleep", lambda *args, **kwargs: None)

    bootstrapped = testenv.juju_onboard(testenv.SUBSTRATE_LXD)

    assert bootstrapped is True
    assert juju.add_cloud_calls == 0
    assert juju.add_credential_calls == 0
    assert juju.bootstrap_calls == [("localhost", testenv.LXD_CONTROLLER)]
    assert juju.bootstrap_kwargs == [
        {
            "bootstrap_constraints": {"virt-type": "virtual-machine"},
            "config": None,
        }
    ]
    assert juju.switch_calls == [testenv.LXD_CONTROLLER]


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


def test_cleanup_destroy_nodes_skips_missing_inputs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    terragrunt_dir = tmp_path / "maas-nodes"
    terragrunt_dir.mkdir(parents=True)
    monkeypatch.setenv("CEPHTOOLS_TERRAGRUNT_DIR", str(terragrunt_dir))
    monkeypatch.setattr(
        testenv,
        "run",
        lambda *args, **kwargs: pytest.fail("run should not be called"),
    )

    result = testenv._cleanup_destroy_nodes()

    assert result.outcome == "skipped"
    assert "ensure-nodes.hcl" in result.detail


def test_cleanup_destroy_nodes_skips_missing_terragrunt_dir(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CEPHTOOLS_TERRAGRUNT_DIR", raising=False)
    monkeypatch.setattr(
        testenv,
        "_resolve_terragrunt_dir",
        lambda: (_ for _ in ()).throw(
            ClickException("Unable to locate terragrunt configuration directory.")
        ),
    )

    result = testenv._cleanup_destroy_nodes()

    assert result.outcome == "skipped"
    assert "no Terragrunt-managed nodes to destroy" in result.detail


def test_cleanup_kill_controller_when_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[object] = []

    monkeypatch.setattr(testenv.shutil, "which", lambda name: "/bin/true")
    monkeypatch.setattr(testenv.jubilant, "Juju", lambda *args, **kwargs: object())
    monkeypatch.setattr(testenv, "_juju_controller_exists", lambda juju, name: True)

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(cmd)

        class Result:
            stdout = ""

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    result = testenv._cleanup_kill_controller(testenv.MAAS_CONTROLLER)

    assert result.outcome == "ok"
    assert commands == [
        [
            "juju",
            "kill-controller",
            testenv.MAAS_CONTROLLER,
            "--no-prompt",
            "--timeout",
            "2m",
        ]
    ]


def test_cleanup_kill_controller_skips_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(testenv.shutil, "which", lambda name: "/bin/true")
    monkeypatch.setattr(testenv.jubilant, "Juju", lambda *args, **kwargs: object())
    monkeypatch.setattr(testenv, "_juju_controller_exists", lambda juju, name: False)
    monkeypatch.setattr(
        testenv,
        "run",
        lambda *args, **kwargs: pytest.fail("run should not be called"),
    )

    result = testenv._cleanup_kill_controller(testenv.MAAS_CONTROLLER)

    assert result.outcome == "skipped"
    assert testenv.MAAS_CONTROLLER in result.detail


def test_cleanup_delete_vm_host_when_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[object] = []

    monkeypatch.setattr(testenv.shutil, "which", lambda name: "/bin/true")
    monkeypatch.setattr(testenv, "_get_lxd_vm_host_id", lambda admin, vmhost: "321")

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(cmd)

        class Result:
            stdout = ""

        return Result()

    monkeypatch.setattr(testenv, "run", fake_run)

    result = testenv._cleanup_delete_vm_host("admin", "local-lxd")

    assert result.outcome == "ok"
    assert commands == ["maas admin vm-host delete 321"]


def test_cleanup_delete_vm_host_skips_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_vmhost(admin: str, vmhost: str) -> str:
        raise ClickException("VM host 'local-lxd' not found in MAAS vm-hosts output.")

    monkeypatch.setattr(testenv.shutil, "which", lambda name: "/bin/true")
    monkeypatch.setattr(testenv, "_get_lxd_vm_host_id", missing_vmhost)
    monkeypatch.setattr(
        testenv,
        "run",
        lambda *args, **kwargs: pytest.fail("run should not be called"),
    )

    result = testenv._cleanup_delete_vm_host("admin", "local-lxd")

    assert result.outcome == "skipped"
    assert "local-lxd" in result.detail


def test_cleanup_delete_vm_host_reports_lookup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken_lookup(admin: str, vmhost: str) -> str:
        raise subprocess.CalledProcessError(
            1,
            ["maas", admin, "vm-hosts", "read"],
            stderr="auth failed",
        )

    monkeypatch.setattr(testenv.shutil, "which", lambda name: "/bin/true")
    monkeypatch.setattr(testenv, "_get_lxd_vm_host_id", broken_lookup)

    result = testenv._cleanup_delete_vm_host("admin", "local-lxd")

    assert result.outcome == "failed"
    assert "auth failed" in result.detail


def test_cleanup_delete_known_lxd_instances_skips_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[object] = []

    monkeypatch.setattr(testenv.shutil, "which", lambda name: "/bin/true")

    def fake_run(cmd, check=True, shell=False, quiet=False):
        commands.append(cmd)

        class Result:
            def __init__(self, stdout: str = "", stderr: str = "", returncode: int = 0):
                self.stdout = stdout
                self.stderr = stderr
                self.returncode = returncode

        if cmd == ["lxc", "info", testenv.WARMUP_VM_NAME]:
            return Result(returncode=1, stderr="Error: Instance not found")
        raise AssertionError(cmd)

    monkeypatch.setattr(testenv, "run", fake_run)

    result = testenv._cleanup_delete_known_lxd_instances()

    assert result.outcome == "skipped"
    assert commands == [["lxc", "info", testenv.WARMUP_VM_NAME]]


def test_cleanup_delete_known_lxd_instances_reports_inspection_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(testenv.shutil, "which", lambda name: "/bin/true")

    def fake_run(cmd, check=True, shell=False, quiet=False):
        class Result:
            def __init__(self, stdout: str = "", stderr: str = "", returncode: int = 0):
                self.stdout = stdout
                self.stderr = stderr
                self.returncode = returncode

        if cmd == ["lxc", "info", testenv.WARMUP_VM_NAME]:
            return Result(returncode=1, stderr="Error: Failed to connect to LXD")
        raise AssertionError(cmd)

    monkeypatch.setattr(testenv, "run", fake_run)

    result = testenv._cleanup_delete_known_lxd_instances()

    assert result.outcome == "failed"
    assert "Failed to inspect" in result.detail


def test_cleanup_remove_state_files_ignores_legacy_cephtools_config(
    state_home: Path,
) -> None:
    state_home.mkdir(parents=True, exist_ok=True)
    for filename in (*testenv.TESTENV_STATE_FILENAMES, "cephtools.yaml"):
        (state_home / filename).write_text(f"{filename}\n")

    result = testenv._cleanup_remove_state_files()

    assert result.outcome == "ok"
    for filename in testenv.TESTENV_STATE_FILENAMES:
        assert not (state_home / filename).exists()
    assert (state_home / "cephtools.yaml").exists()


def test_cleanup_remove_state_files_skips_missing(state_home: Path) -> None:
    state_home.mkdir(parents=True, exist_ok=True)

    result = testenv._cleanup_remove_state_files()

    assert result.outcome == "skipped"


def test_cleanup_remove_terragrunt_inputs_when_present(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    terragrunt_dir = tmp_path / "maas-nodes"
    terragrunt_dir.mkdir(parents=True)
    inputs_path = terragrunt_dir / testenv.ENSURE_NODES_INPUT_FILENAME
    inputs_path.write_text("inputs = {}\n")
    monkeypatch.setenv("CEPHTOOLS_TERRAGRUNT_DIR", str(terragrunt_dir))

    result = testenv._cleanup_remove_terragrunt_inputs()

    assert result.outcome == "ok"
    assert not inputs_path.exists()


def test_cleanup_remove_terragrunt_inputs_skips_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    terragrunt_dir = tmp_path / "maas-nodes"
    terragrunt_dir.mkdir(parents=True)
    monkeypatch.setenv("CEPHTOOLS_TERRAGRUNT_DIR", str(terragrunt_dir))

    result = testenv._cleanup_remove_terragrunt_inputs()

    assert result.outcome == "skipped"


def test_cleanup_remove_terragrunt_inputs_skips_missing_terragrunt_dir(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CEPHTOOLS_TERRAGRUNT_DIR", raising=False)
    monkeypatch.setattr(
        testenv,
        "_resolve_terragrunt_dir",
        lambda: (_ for _ in ()).throw(
            ClickException("Unable to locate terragrunt configuration directory.")
        ),
    )

    result = testenv._cleanup_remove_terragrunt_inputs()

    assert result.outcome == "skipped"
    assert "no Terragrunt inputs to remove" in result.detail


def test_cleanup_cli_dry_run_does_not_invoke_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = CliRunner()
    monkeypatch.setattr(testenv, "primary_ip", lambda: "10.0.0.1")
    monkeypatch.setattr(
        testenv,
        "run",
        lambda *args, **kwargs: pytest.fail("run should not be called in dry-run"),
    )

    result = runner.invoke(testenv.cli, ["cleanup", "--dry-run"])

    assert result.exit_code == 0
    assert "Cleanup summary:" in result.output
    assert "dry-run" in result.output.lower()


def test_cleanup_cli_keep_flags_skip_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    calls: list[str] = []

    monkeypatch.setattr(testenv, "primary_ip", lambda: "10.0.0.1")
    monkeypatch.setattr(
        testenv,
        "_cleanup_destroy_nodes",
        lambda *args, **kwargs: calls.append("nodes"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_kill_controller",
        lambda *args, **kwargs: calls.append("controller"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_delete_vm_host",
        lambda *args, **kwargs: calls.append("vm-host"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_delete_known_lxd_instances",
        lambda *args, **kwargs: calls.append("lxd"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_state_files",
        lambda *args, **kwargs: calls.append("state-files"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_terragrunt_inputs",
        lambda *args, **kwargs: calls.append("terragrunt-inputs"),
    )

    result = runner.invoke(
        testenv.cli,
        [
            "cleanup",
            "--keep-nodes",
            "--keep-controller",
            "--keep-vm-host",
            "--keep-lxd-instances",
            "--keep-state",
        ],
    )

    assert result.exit_code == 0
    assert calls == []
    assert result.output.count("skipped") >= 5


def test_cleanup_cli_keep_nodes_preserves_terragrunt_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = CliRunner()
    calls: list[str] = []

    monkeypatch.setattr(testenv, "primary_ip", lambda: "10.0.0.1")
    monkeypatch.setattr(
        testenv,
        "_cleanup_kill_controller",
        lambda *_args, **_kwargs: pytest.fail(
            "controller cleanup should be implied off by --keep-nodes"
        ),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_delete_vm_host",
        lambda admin, vmhost: testenv.CleanupPhaseResult(
            f"delete vm host {vmhost}", "skipped", "absent"
        ),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_delete_known_lxd_instances",
        lambda: testenv.CleanupPhaseResult(
            "delete known LXD instances", "skipped", "absent"
        ),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_state_files",
        lambda: calls.append("state-files")
        or testenv.CleanupPhaseResult("remove state files", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_terragrunt_inputs",
        lambda: calls.append("terragrunt-inputs")
        or testenv.CleanupPhaseResult("remove terragrunt inputs", "ok", "removed"),
    )

    result = runner.invoke(
        testenv.cli, ["--substrate", "maas-host", "cleanup", "--keep-nodes"]
    )

    assert result.exit_code == 0
    assert calls == ["state-files"]
    assert (
        "kill controller maas-controller: skipped (preserved by --keep-nodes)"
        in result.output
    )
    assert "remove terragrunt inputs: skipped" in result.output
    assert "preserved while nodes are kept" in result.output


def test_cleanup_cli_keep_nodes_implies_keep_controller_for_lxd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = CliRunner()
    calls: list[str] = []
    monkeypatch.setattr(testenv, "primary_ip", lambda: "10.0.0.1")
    monkeypatch.setattr(
        testenv,
        "_cleanup_kill_controller",
        lambda *_args, **_kwargs: pytest.fail(
            "controller cleanup should be implied off by --keep-nodes"
        ),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_lxd_osd_volumes",
        lambda *_args, **_kwargs: pytest.fail(
            "OSD volume cleanup should be skipped by --keep-nodes"
        ),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_delete_known_lxd_instances",
        lambda: calls.append("lxd-instances")
        or testenv.CleanupPhaseResult(
            "delete known LXD instances", "skipped", "absent"
        ),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_state_files",
        lambda: calls.append("state-files")
        or testenv.CleanupPhaseResult("remove state files", "ok", "removed"),
    )

    result = runner.invoke(testenv.cli, ["cleanup", "--keep-nodes"])

    assert result.exit_code == 0
    assert calls == ["lxd-instances", "state-files"]
    assert (
        "kill controller lxd-controller: skipped (preserved by --keep-nodes)"
        in result.output
    )
    assert (
        "delete LXD OSD volumes: skipped (preserved by --keep-nodes)" in result.output
    )


def test_cleanup_cli_explicit_keep_controller_remains_distinct_for_lxd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = CliRunner()
    monkeypatch.setattr(testenv, "primary_ip", lambda: "10.0.0.1")
    monkeypatch.setattr(
        testenv,
        "_cleanup_kill_controller",
        lambda *_args, **_kwargs: pytest.fail(
            "controller cleanup should be skipped by --keep-controller"
        ),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_lxd_osd_volumes",
        lambda *_args, **_kwargs: pytest.fail(
            "OSD volume cleanup should be skipped while controller is kept"
        ),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_delete_known_lxd_instances",
        lambda: testenv.CleanupPhaseResult(
            "delete known LXD instances", "skipped", "absent"
        ),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_state_files",
        lambda: testenv.CleanupPhaseResult("remove state files", "ok", "removed"),
    )

    result = runner.invoke(testenv.cli, ["cleanup", "--keep-controller"])

    assert result.exit_code == 0
    assert (
        "kill controller lxd-controller: skipped (preserved by --keep-controller)"
        in result.output
    )
    assert (
        "delete LXD OSD volumes: skipped (preserved while controller is kept)"
        in result.output
    )


def test_cleanup_cli_best_effort_reports_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = CliRunner()
    calls: list[str] = []

    monkeypatch.setattr(testenv, "primary_ip", lambda: "10.0.0.1")
    monkeypatch.setattr(
        testenv,
        "_cleanup_destroy_nodes",
        lambda: calls.append("nodes")
        or testenv.CleanupPhaseResult("destroy nodes", "failed", "boom"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_kill_controller",
        lambda controller_name: calls.append("controller")
        or testenv.CleanupPhaseResult(
            f"kill controller {controller_name}", "ok", "removed"
        ),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_delete_vm_host",
        lambda admin, vmhost: calls.append("vm-host")
        or testenv.CleanupPhaseResult(f"delete vm host {vmhost}", "skipped", "absent"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_delete_known_lxd_instances",
        lambda: calls.append("lxd")
        or testenv.CleanupPhaseResult("delete known LXD instances", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_state_files",
        lambda: calls.append("state-files")
        or testenv.CleanupPhaseResult("remove state files", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_terragrunt_inputs",
        lambda: calls.append("terragrunt-inputs")
        or testenv.CleanupPhaseResult("remove terragrunt inputs", "ok", "removed"),
    )

    result = runner.invoke(testenv.cli, ["--substrate", "maas-host", "cleanup"])

    assert calls == [
        "nodes",
        "controller",
        "vm-host",
        "lxd",
        "state-files",
    ]
    assert result.exit_code == 1
    assert "destroy nodes: failed" in result.output
    assert "kill controller" in result.output
    assert "remove terragrunt inputs: skipped" in result.output
    assert (
        "preserved because node cleanup did not complete successfully" in result.output
    )


def test_cleanup_cli_purge_installed_rejects_keep_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = CliRunner()
    monkeypatch.setattr(testenv, "primary_ip", lambda: "10.0.0.1")

    result = runner.invoke(
        testenv.cli,
        ["cleanup", "--purge-installed", "--keep-state"],
    )

    assert result.exit_code == 1
    assert "--purge-installed cannot be combined" in result.output
    assert "--keep-state" in result.output


def test_cleanup_cli_purge_installed_runs_extended_phases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = CliRunner()
    calls: list[str] = []

    monkeypatch.setattr(testenv, "primary_ip", lambda: "10.0.0.1")
    monkeypatch.setattr(
        testenv,
        "_cleanup_destroy_nodes",
        lambda: calls.append("nodes")
        or testenv.CleanupPhaseResult("destroy nodes", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_kill_controller",
        lambda controller_name: calls.append("controller")
        or testenv.CleanupPhaseResult(
            f"kill controller {controller_name}", "ok", "removed"
        ),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_delete_vm_host",
        lambda admin, vmhost: calls.append("vm-host")
        or testenv.CleanupPhaseResult(f"delete vm host {vmhost}", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_delete_known_lxd_instances",
        lambda: calls.append("delete-lxd-instances")
        or testenv.CleanupPhaseResult("delete known LXD instances", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_state_files",
        lambda: calls.append("state-files")
        or testenv.CleanupPhaseResult("remove state files", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_terragrunt_inputs",
        lambda: calls.append("terragrunt-inputs")
        or testenv.CleanupPhaseResult("remove terragrunt inputs", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_snap",
        lambda name: calls.append(f"snap:{name}")
        or testenv.CleanupPhaseResult(f"remove snap {name}", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_user_paths",
        lambda phase, paths: calls.append("juju-state")
        or testenv.CleanupPhaseResult(phase, "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_purge_apt_packages",
        lambda phase, prefixes=(), exact_names=(): calls.append(phase)
        or testenv.CleanupPhaseResult(phase, "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_apt_autoremove",
        lambda: calls.append("apt-autoremove")
        or testenv.CleanupPhaseResult("apt autoremove --purge", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_maas_ppa_sources",
        lambda: calls.append("maas-ppa")
        or testenv.CleanupPhaseResult("remove MAAS apt sources", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_apt_update",
        lambda: calls.append("apt-update")
        or testenv.CleanupPhaseResult("apt update", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_restore_systemd_timesyncd",
        lambda: calls.append("timesyncd")
        or testenv.CleanupPhaseResult("restore systemd-timesyncd", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_root_paths",
        lambda phase, paths: calls.append(phase)
        or testenv.CleanupPhaseResult(phase, "ok", "removed"),
    )

    result = runner.invoke(
        testenv.cli, ["--substrate", "maas-host", "cleanup", "--purge-installed"]
    )

    assert result.exit_code == 0
    assert calls == [
        "nodes",
        "controller",
        "vm-host",
        "delete-lxd-instances",
        "state-files",
        "terragrunt-inputs",
        "snap:juju",
        "juju-state",
        "purge MAAS apt packages",
        "purge PostgreSQL apt packages",
        "purge testenv helper apt packages",
        "apt-autoremove",
        "maas-ppa",
        "apt-update",
        "timesyncd",
        "snap:lxd",
        "snap:terraform",
        "remove Terragrunt binary",
        "remove residual toolchain directories",
    ]


def test_cleanup_kill_controller_skips_when_juju_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        testenv.shutil, "which", lambda name: None if name == "juju" else "/bin/true"
    )

    result = testenv._cleanup_kill_controller("maas-controller")

    assert result.outcome == "skipped"
    assert result.detail == "juju command not found"


def test_cleanup_delete_vm_host_skips_when_maas_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        testenv.shutil, "which", lambda name: None if name == "maas" else "/bin/true"
    )

    result = testenv._cleanup_delete_vm_host("admin", "local-lxd")

    assert result.outcome == "skipped"
    assert result.detail == "maas command not found"


def test_cleanup_delete_known_lxd_instances_skips_when_lxc_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        testenv.shutil, "which", lambda name: None if name == "lxc" else "/bin/true"
    )

    result = testenv._cleanup_delete_known_lxd_instances()

    assert result.outcome == "skipped"
    assert result.detail == "lxc command not found"


def test_cleanup_cli_purge_installed_removes_terragrunt_inputs_after_node_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = CliRunner()
    calls: list[str] = []

    monkeypatch.setattr(testenv, "primary_ip", lambda: "10.0.0.1")
    monkeypatch.setattr(
        testenv,
        "_cleanup_destroy_nodes",
        lambda: calls.append("nodes")
        or testenv.CleanupPhaseResult("destroy nodes", "failed", "boom"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_kill_controller",
        lambda controller_name: testenv.CleanupPhaseResult(
            f"kill controller {controller_name}", "ok", "removed"
        ),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_delete_vm_host",
        lambda admin, vmhost: testenv.CleanupPhaseResult(
            f"delete vm host {vmhost}", "ok", "removed"
        ),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_delete_known_lxd_instances",
        lambda: testenv.CleanupPhaseResult(
            "delete known LXD instances", "ok", "removed"
        ),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_state_files",
        lambda: testenv.CleanupPhaseResult("remove state files", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_terragrunt_inputs",
        lambda: calls.append("terragrunt-inputs")
        or testenv.CleanupPhaseResult("remove terragrunt inputs", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_snap",
        lambda name: testenv.CleanupPhaseResult(f"remove snap {name}", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_user_paths",
        lambda phase, paths: testenv.CleanupPhaseResult(phase, "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_purge_apt_packages",
        lambda phase, prefixes=(), exact_names=(): testenv.CleanupPhaseResult(
            phase, "ok", "removed"
        ),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_apt_autoremove",
        lambda: testenv.CleanupPhaseResult("apt autoremove --purge", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_maas_ppa_sources",
        lambda: testenv.CleanupPhaseResult("remove MAAS apt sources", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_apt_update",
        lambda: testenv.CleanupPhaseResult("apt update", "ok", "removed"),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_restore_systemd_timesyncd",
        lambda: testenv.CleanupPhaseResult(
            "restore systemd-timesyncd", "ok", "removed"
        ),
    )
    monkeypatch.setattr(
        testenv,
        "_cleanup_remove_root_paths",
        lambda phase, paths: testenv.CleanupPhaseResult(phase, "ok", "removed"),
    )

    result = runner.invoke(
        testenv.cli, ["--substrate", "maas-host", "cleanup", "--purge-installed"]
    )

    assert result.exit_code == 1
    assert calls == ["nodes", "terragrunt-inputs"]


def test_lxd_init_cli_uses_fixed_disposable_lab_conventions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = CliRunner()
    calls: list[tuple[object, ...]] = []
    monkeypatch.setattr(testenv, "primary_ip", lambda: "10.0.0.1")
    monkeypatch.setattr(
        testenv,
        "lxd_init_lxd_impl",
        lambda bridge: calls.append(("init", bridge)),
    )
    monkeypatch.setattr(
        testenv,
        "verify_lxd",
        lambda bridge: calls.append(("verify", bridge)),
    )

    result = runner.invoke(testenv.cli, ["lxd-init"])

    assert result.exit_code == 0
    assert calls == [
        ("init", testenv.LXD_BRIDGE),
        ("verify", testenv.LXD_BRIDGE),
    ]


def test_register_vm_host_cli_wires_maas_host_conventions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = CliRunner()
    calls: list[tuple[object, ...]] = []
    monkeypatch.setattr(testenv, "primary_ip", lambda: "10.0.0.10")
    monkeypatch.setattr(
        testenv,
        "register_lxd_vmhost_impl",
        lambda admin, vmhost, ip, **kwargs: calls.append(
            ("register", admin, vmhost, ip, kwargs)
        ),
    )
    monkeypatch.setattr(
        testenv,
        "import_boot_resources",
        lambda admin, **kwargs: calls.append(("import", admin, kwargs)),
    )
    monkeypatch.setattr(
        testenv,
        "_wait_for_vm_host_architecture",
        lambda admin, vmhost, arch, **kwargs: calls.append(
            ("wait", admin, vmhost, arch, kwargs)
        ),
    )

    result = runner.invoke(
        testenv.cli, ["--substrate", "maas-host", "register-vm-host"]
    )

    assert result.exit_code == 0
    assert calls[0] == (
        "register",
        testenv.MAAS_ADMIN,
        testenv.MAAS_VM_HOST,
        "10.0.0.10",
        {"project": "default", "maas_vm_name": None},
    )
    assert calls[1] == ("import", testenv.MAAS_ADMIN, {"maas_vm_name": None})
    assert calls[2] == (
        "wait",
        testenv.MAAS_ADMIN,
        testenv.MAAS_VM_HOST,
        testenv.REQUIRED_BOOT_ARCHITECTURE,
        {"maas_vm_name": None},
    )


def test_register_vm_host_cli_wires_maas_vm_conventions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = CliRunner()
    calls: list[tuple[object, ...]] = []
    bridges: list[str] = []
    monkeypatch.setattr(testenv, "primary_ip", lambda: "10.0.0.10")

    def fake_network(bridge: str) -> tuple[str, str]:
        bridges.append(bridge)
        return "10.20.0.0/24", "10.20.0.1"

    monkeypatch.setattr(testenv, "lxd_network_cidr_and_gateway", fake_network)
    monkeypatch.setattr(
        testenv,
        "register_lxd_vmhost_impl",
        lambda admin, vmhost, ip, **kwargs: calls.append(
            ("register", admin, vmhost, ip, kwargs)
        ),
    )
    monkeypatch.setattr(testenv, "import_boot_resources", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        testenv, "_wait_for_vm_host_architecture", lambda *args, **kwargs: None
    )

    result = runner.invoke(testenv.cli, ["--substrate", "maas-vm", "register-vm-host"])

    assert result.exit_code == 0
    assert bridges == [testenv.MAAS_LXD_BRIDGE]
    assert calls == [
        (
            "register",
            testenv.MAAS_ADMIN,
            testenv.MAAS_VM_HOST,
            "10.20.0.1",
            {
                "project": testenv.MAAS_LXD_PROJECT,
                "maas_vm_name": testenv.MAAS_VM_NAME,
            },
        )
    ]


def test_register_vm_host_cli_is_noop_for_lxd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = CliRunner()
    monkeypatch.setattr(testenv, "primary_ip", lambda: "10.0.0.10")
    monkeypatch.setattr(
        testenv,
        "register_lxd_vmhost_impl",
        lambda *args, **kwargs: pytest.fail("registration should be skipped"),
    )

    result = runner.invoke(testenv.cli, ["register-vm-host"])

    assert result.exit_code == 0
    assert "skipped for --substrate lxd" in result.output


# ---------------------------------------------------------------------------
# juju_warmup
# ---------------------------------------------------------------------------


class _RunResult:
    def __init__(self, stdout: str = "", returncode: int = 0):
        self.stdout = stdout
        self.stderr = ""
        self.returncode = returncode


def _warmup_run_recorder(calls, *, status_payloads):
    """Fake testenv.run that drives juju_warmup through to completion.

    For each base, add-model/add-machine/set-model-constraints/destroy-model
    return success, and `juju status --format json` returns a payload whose
    machine 0 becomes "started" after one poll.
    """

    def fake_run(cmd, *args, **kwargs):
        calls.append(list(cmd))

        if cmd[:2] == ["juju", "destroy-model"]:
            return _RunResult()
        if cmd[:2] == ["juju", "add-model"]:
            return _RunResult()
        if cmd[:2] == ["juju", "set-model-constraints"]:
            return _RunResult()
        if cmd[:2] == ["juju", "add-machine"]:
            return _RunResult()
        if cmd[:2] == ["juju", "status"]:
            return _RunResult(stdout=status_payloads.pop(0))
        raise AssertionError(f"unexpected run: {cmd}")

    return fake_run


def test_juju_warmup_caches_each_base_and_destroys_models(monkeypatch):
    calls: list[list[str]] = []
    started = json.dumps({"machines": {"0": {"machine-status": {"status": "started"}}}})
    # One "started" status payload per base (the warmup polls exactly once).
    payloads = [started, started, started]
    monkeypatch.setattr(
        testenv, "run", _warmup_run_recorder(calls, status_payloads=payloads)
    )
    monkeypatch.setattr(testenv.time, "sleep", lambda *a, **k: None)

    testenv.juju_warmup()

    # For each base: destroy(cleanup) + add-model + set-constraints + add-machine
    # + destroy(final). Status polls happen via `run` too.
    destroys = [c for c in calls if c[:2] == ["juju", "destroy-model"]]
    add_models = [c for c in calls if c[:2] == ["juju", "add-model"]]
    add_machines = [c for c in calls if c[:2] == ["juju", "add-machine"]]
    assert len(add_models) == 3
    assert len(add_machines) == 3
    # one cleanup destroy before + one final destroy after, per base
    assert len(destroys) == 6

    # Each add-machine targets its base and the warmup model on lxd-controller.
    for base, cmd in zip(
        ("ubuntu@22.04", "ubuntu@24.04", "ubuntu@26.04"), add_machines
    ):
        assert "--base" in cmd
        assert base in cmd
        assert "lxd-controller:warmup-ubuntu-" in " ".join(cmd)


def test_juju_warmup_is_best_effort_on_add_model_failure(monkeypatch):
    """A failed add-model must warn and continue, never raise."""

    def fake_run(cmd, *args, **kwargs):
        if cmd[:2] == ["juju", "add-model"]:
            return _RunResult(stdout="boom", returncode=2)
        return _RunResult()

    monkeypatch.setattr(testenv, "run", fake_run)
    monkeypatch.setattr(testenv.time, "sleep", lambda *a, **k: None)

    # Must not raise.
    testenv.juju_warmup()


def test_juju_warmup_is_best_effort_on_timeout(monkeypatch):
    """A machine that never starts must warn and continue, never raise."""

    def fake_run(cmd, *args, **kwargs):
        if cmd[:2] == ["juju", "status"]:
            # machine stuck pending forever
            return _RunResult(
                stdout=json.dumps(
                    {"machines": {"0": {"machine-status": {"status": "pending"}}}}
                )
            )
        return _RunResult()

    monkeypatch.setattr(testenv, "run", fake_run)
    monkeypatch.setattr(testenv.time, "sleep", lambda *a, **k: None)

    # Short timeout so the test doesn't wait the full default. Must not raise;
    # the timed-out base's model is still destroyed via the finally block.
    testenv.juju_warmup(timeout_seconds=0, poll_interval_seconds=0)


def test_juju_warmup_model_name_is_valid():
    assert testenv._warmup_model_name("ubuntu@24.04") == "warmup-ubuntu-24-04"
    assert testenv._warmup_model_name("ubuntu@26.04") == "warmup-ubuntu-26-04"


def test_install_runs_juju_warmup_for_lxd_substrate(monkeypatch):
    """The LXD install sequence must include a juju warmup step."""
    runner = CliRunner()

    # Short-circuit every install step so we can assert the step list runs.
    monkeypatch.setattr(testenv, "install_fault_handlers", lambda name: None)
    monkeypatch.setattr(testenv, "install_deps", lambda *a, **k: None)
    monkeypatch.setattr(testenv, "lxd_init_cmd", lambda *a, **k: None)
    monkeypatch.setattr(testenv, "juju_init", lambda *a, **k: None)
    monkeypatch.setattr(testenv, "configure_network", lambda *a, **k: None)
    monkeypatch.setattr(testenv, "_ensure_model_for_substrate", lambda *a, **k: None)
    warmed: list[bool] = []
    monkeypatch.setattr(testenv, "juju_warmup", lambda *a, **k: warmed.append(True))
    monkeypatch.setattr(testenv, "mark_complete", lambda: None)

    result = runner.invoke(testenv.cli, ["--substrate", "lxd", "install"])

    assert result.exit_code == 0, result.output
    assert warmed == [True]
    assert "Warming up Juju VM images" in result.output


def test_install_skips_juju_warmup_for_maas_host_substrate(monkeypatch):
    """MAAS-host install must not run the LXD-only juju warmup."""
    runner = CliRunner()

    monkeypatch.setattr(testenv, "install_fault_handlers", lambda name: None)
    monkeypatch.setattr(testenv, "install_deps", lambda *a, **k: None)
    monkeypatch.setattr(testenv, "lxd_init_cmd", lambda *a, **k: None)
    monkeypatch.setattr(testenv, "maas_init_cmd", lambda *a, **k: None)
    monkeypatch.setattr(testenv, "register_vm_host", lambda *a, **k: None)
    monkeypatch.setattr(testenv, "configure_network", lambda *a, **k: None)
    monkeypatch.setattr(testenv, "juju_init", lambda *a, **k: None)
    monkeypatch.setattr(testenv, "_ensure_model_for_substrate", lambda *a, **k: None)
    monkeypatch.setattr(
        testenv,
        "juju_warmup",
        lambda *a, **k: pytest.fail("warmup must not run for MAAS"),
    )
    monkeypatch.setattr(testenv, "mark_complete", lambda: None)
    # MAAS juju_onboard path touches state files; stub jubilant to avoid that.
    monkeypatch.setattr(testenv, "juju_onboard", lambda *a, **k: True)

    result = runner.invoke(testenv.cli, ["--substrate", "maas-host", "install"])

    assert result.exit_code == 0, result.output
    assert "Warming up Juju VM images" not in result.output
