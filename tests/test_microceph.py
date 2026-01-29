from __future__ import annotations

import subprocess

import click
import pytest
from click.testing import CliRunner

from cephtools import microceph


def test_resolve_nodes_from_status(monkeypatch: pytest.MonkeyPatch) -> None:
    config = {"juju_model": "ceph-model"}
    monkeypatch.setattr(microceph, "load_cephtools_config", lambda ensure=True: config)

    captured: dict[str, object] = {}

    def fake_application_machines(model: str, application: str) -> tuple[int, ...]:
        captured["model"] = model
        captured["application"] = application
        return (3, 4)

    monkeypatch.setattr(microceph, "application_machines", fake_application_machines)

    nodes = microceph._resolve_nodes()

    assert nodes == (3, 4)
    assert captured == {"model": "ceph-model", "application": "microceph"}


def test_resolve_nodes_override(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_load_config(**_: object) -> None:
        raise AssertionError("load_cephtools_config should not be called")

    monkeypatch.setattr(microceph, "load_cephtools_config", fail_load_config)

    def fail_application_machines(*_: object, **__: object) -> tuple[int, ...]:
        raise AssertionError("application_machines should not be called")

    monkeypatch.setattr(microceph, "application_machines", fail_application_machines)

    nodes = microceph._resolve_nodes(nodes_override=("1", 2))

    assert nodes == (1, 2)


def test_add_disks_cli_invokes_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    nodes = (1, 2)

    monkeypatch.setattr(
        microceph,
        "_resolve_nodes",
        lambda nodes_override=None: nodes if nodes_override is None else tuple(),
    )

    captured: dict[str, object] = {}

    def fake_runner(
        resolved_nodes,
        factory,
        *,
        use_sudo: bool,
        dry_run: bool,
    ):
        captured["nodes"] = tuple(resolved_nodes)
        captured["use_sudo"] = use_sudo
        captured["dry_run"] = dry_run
        captured["commands"] = [tuple(factory(node)) for node in resolved_nodes]
        return []

    monkeypatch.setattr(microceph, "_run_on_all_nodes", fake_runner)

    result = runner.invoke(
        microceph.cli,
        [
            "disk",
            "add",
            "/dev/disk/by-id/{host}-data",
            "--no-sudo",
        ],
    )

    assert result.exit_code == 0
    assert captured["nodes"] == nodes
    assert captured["use_sudo"] is False
    assert captured["dry_run"] is False
    assert captured["commands"] == [
        ("microceph", "disk", "add", "/dev/disk/by-id/{host}-data"),
        ("microceph", "disk", "add", "/dev/disk/by-id/{host}-data"),
    ]


def test_add_disks_cli_passes_node_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    nodes = (99,)
    captured: dict[str, object] = {}

    def fake_loader(nodes_override=None):
        captured["nodes_override"] = nodes_override
        return nodes

    monkeypatch.setattr(microceph, "_resolve_nodes", fake_loader)
    monkeypatch.setattr(microceph, "_run_on_all_nodes", lambda *a, **k: [])

    result = runner.invoke(
        microceph.cli,
        [
            "disk",
            "add",
            "/dev/sdb",
            "--nodes",
            "1",
            "--nodes",
            "2",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert captured["nodes_override"] == ("1", "2")


def test_run_on_all_nodes_reports_failures(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    nodes = (1, 2)

    def fake_ssh_run(command):
        target = command[2]
        if target == "2":
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="boom")
        return subprocess.CompletedProcess(command, 0, stdout="ok\n", stderr="")

    monkeypatch.setattr(microceph, "_ssh_run", fake_ssh_run)

    with pytest.raises(click.ClickException) as exc_info:
        microceph._run_on_all_nodes(
            nodes,
            lambda node: ["microceph", "disk", "add", f"/dev/{node}"],
            use_sudo=False,
        )

    out, err = capsys.readouterr()
    assert "[1]" in out
    assert "[2]" in out
    assert "Command failed on one or more nodes" in str(exc_info.value)
    assert "2" in str(exc_info.value)
