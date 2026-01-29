from __future__ import annotations

from pathlib import Path

import pytest
from click import ClickException

from cephtools import terraform


@pytest.fixture
def state_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / "state"
    monkeypatch.setenv("CEPHTOOLS_STATE_HOME", str(home))
    return home


def test_find_terraform_root_from_config(monkeypatch, tmp_path: Path, state_home: Path):
    state_home.mkdir(parents=True, exist_ok=True)
    configured_root = tmp_path / "configured-terraform"
    configured_root.mkdir()
    (state_home / "cephtools.yaml").write_text(f"terraform_root: {configured_root}\n")

    resolved = terraform.find_terraform_root()
    assert resolved == configured_root


def test_find_terraform_root_handles_missing(monkeypatch):
    monkeypatch.setattr(
        terraform, "terraform_root_candidates", lambda: [Path("/does/not/exist")]
    )

    with pytest.raises(ClickException):
        terraform.find_terraform_root()

    assert terraform.find_terraform_root(raise_if_missing=False) is None


def test_resolve_plan_dir_prefers_env_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, state_home: Path
) -> None:
    env_root = tmp_path / "env-root"
    plan_dir = env_root / "microceph"
    plan_dir.mkdir(parents=True)
    (plan_dir / "terragrunt.hcl").write_text("")

    monkeypatch.setenv("CEPHTOOLS_TERRAFORM_ROOT", str(env_root))

    resolved = terraform.resolve_plan_dir("microceph", plan_relative=Path("microceph"))
    assert resolved == plan_dir


def test_resolve_plan_dir_uses_config_path(
    tmp_path: Path, state_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_root = tmp_path / "configured"
    plan_dir = config_root / "microceph"
    plan_dir.mkdir(parents=True)
    (plan_dir / "terragrunt.hcl").write_text("")

    state_home.mkdir(parents=True, exist_ok=True)
    (state_home / "cephtools.yaml").write_text(f"terraform_root: {config_root}\n")

    # Clear env override if present
    monkeypatch.delenv("CEPHTOOLS_TERRAFORM_ROOT", raising=False)

    resolved = terraform.resolve_plan_dir("microceph", plan_relative=Path("microceph"))
    assert resolved == plan_dir
