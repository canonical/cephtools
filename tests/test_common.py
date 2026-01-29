from __future__ import annotations

import shlex
import subprocess
from typing import Any

import pytest

from cephtools import common


def test_run_executes_sequence_command() -> None:
    code = "print('hello from run', end='')"
    result = common.run(["python3", "-c", code])
    assert result.stdout == "hello from run"


def test_run_executes_string_command() -> None:
    code = "print('hello again', end='')"
    command = f"python3 -c {shlex.quote(code)}"
    result = common.run(command)
    assert result.stdout == "hello again"


@pytest.mark.parametrize(
    ("existing", "expected_installs"),
    [
        ("Name Version Rev Tracking Publisher Notes\nsnapx 1 1 stable dev test\n", 0),
        ("Name Version Rev Tracking Publisher Notes\n", 1),
    ],
)
def test_ensure_snap_installs_when_missing(
    existing: str, expected_installs: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[Any, dict[str, Any]]] = []

    def fake_run(command: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((command, kwargs))
        if command == ["snap", "list"]:
            return subprocess.CompletedProcess(command, 0, stdout=existing)
        if isinstance(command, list) and command[:3] == ["sudo", "snap", "install"]:
            return subprocess.CompletedProcess(command, 0, stdout="")
        raise AssertionError(f"unexpected command: {command!r}")

    monkeypatch.setattr(common, "run", fake_run)
    common.ensure_snap("snapx")

    install_calls = [
        call
        for call in calls
        if isinstance(call[0], list) and call[0][:3] == ["sudo", "snap", "install"]
    ]
    assert len(install_calls) == expected_installs
