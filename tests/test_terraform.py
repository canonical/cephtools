from __future__ import annotations

from pathlib import Path

import pytest
from click import ClickException

from cephtools import terraform


def test_find_terraform_root_from_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    configured_root = tmp_path / "configured-terraform"
    configured_root.mkdir()
    monkeypatch.setenv("CEPHTOOLS_TERRAFORM_ROOT", str(configured_root))

    resolved = terraform.find_terraform_root()
    assert resolved == configured_root


def test_find_terraform_root_handles_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        terraform, "terraform_root_candidates", lambda: [Path("/does/not/exist")]
    )

    with pytest.raises(ClickException):
        terraform.find_terraform_root()

    assert terraform.find_terraform_root(raise_if_missing=False) is None


def test_resolve_plan_dir_prefers_env_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    env_root = tmp_path / "env-root"
    plan_dir = env_root / "microceph"
    plan_dir.mkdir(parents=True)
    (plan_dir / "terragrunt.hcl").write_text("")

    monkeypatch.setenv("CEPHTOOLS_TERRAFORM_ROOT", str(env_root))

    resolved = terraform.resolve_plan_dir("microceph", plan_relative=Path("microceph"))
    assert resolved == plan_dir
