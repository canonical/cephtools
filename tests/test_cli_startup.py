from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


def _run_cli(args: list[str], state_home: Path) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["CEPHTOOLS_STATE_HOME"] = str(state_home)
    return subprocess.run(
        [sys.executable, "-c", "from cephtools.main import cli; cli()", *args],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


@pytest.mark.parametrize(
    "args", [["--help"], ["testenv", "--help"], ["testenv", "job", "--help"]]
)
def test_help_does_not_create_state(tmp_path: Path, args: list[str]) -> None:
    state_home = tmp_path / "state"

    result = _run_cli(args, state_home)

    assert result.returncode == 0, result.stderr
    assert "Usage:" in result.stdout
    assert not state_home.exists()


@pytest.mark.parametrize(
    "args", [["--help"], ["testenv", "--help"], ["testenv", "job", "--help"]]
)
def test_help_ignores_legacy_config(tmp_path: Path, args: list[str]) -> None:
    state_home = tmp_path / "state"
    state_home.mkdir()
    legacy_config = state_home / "cephtools.yaml"
    legacy_content = "this is deliberately not valid: [yaml"
    legacy_config.write_text(legacy_content)
    result = _run_cli(args, state_home)

    assert result.returncode == 0, result.stderr
    assert "Usage:" in result.stdout
    assert legacy_config.read_text() == legacy_content
    assert sorted(path.name for path in state_home.iterdir()) == ["cephtools.yaml"]


def test_job_protocol_does_not_create_state(tmp_path: Path) -> None:
    state_home = tmp_path / "state"

    result = _run_cli(["testenv", "job", "protocol"], state_home)

    assert result.returncode == 0, result.stderr
    assert result.stdout == "1\n"
    assert not state_home.exists()
