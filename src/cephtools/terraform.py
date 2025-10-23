from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

import click

from cephtools.config import DEFAULT_TERRAFORM_ROOT, load_cephtools_config


def _extract_config_path(config: dict[str, object], key: str) -> str | None:
    raw_value: object = config.get(key)
    if raw_value is None:
        paths_section = config.get("paths")
        if isinstance(paths_section, dict):
            raw_value = paths_section.get(key)
    if raw_value is None:
        return None
    if not isinstance(raw_value, str):
        raise click.ClickException(
            f"Configuration value '{key}' must be a string path."
        )
    return raw_value


def terraform_root_candidates() -> list[Path]:
    """
    Return possible terraform root directories ordered by preference.
    """
    candidates: list[Path] = []

    env_root = os.environ.get("CEPHTOOLS_TERRAFORM_ROOT")
    if env_root:
        candidates.append(Path(env_root).expanduser())

    config = load_cephtools_config(ensure=True)
    config_path = _extract_config_path(config, "terraform_root") if config else None
    if config_path:
        candidates.append(Path(config_path).expanduser())
    else:
        candidates.append(DEFAULT_TERRAFORM_ROOT)

    cwd = Path.cwd()
    parents: Iterable[Path] = (cwd, *cwd.parents)
    candidates.extend(parent / "terraform" for parent in parents)

    package_root = Path(__file__).resolve().parents[2] / "terraform"
    candidates.append(package_root)

    default_root = DEFAULT_TERRAFORM_ROOT
    if default_root not in candidates:
        candidates.append(default_root)

    return candidates


def find_terraform_root(*, raise_if_missing: bool = True) -> Path | None:
    """
    Return the first existing terraform root directory from the candidate list.

    When raise_if_missing is False, the function returns None if no candidates
    exist. Otherwise, a ClickException is raised detailing checked locations.
    """
    seen: set[Path] = set()
    for candidate in terraform_root_candidates():
        resolved = candidate.expanduser().resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.is_dir():
            return resolved

    if raise_if_missing:
        attempted = "\n  - ".join(str(path) for path in seen) or "<none>"
        raise click.ClickException(
            "Unable to locate terraform root directory.\n"
            "Checked the following locations:\n"
            f"  - {attempted}\n"
            "Set 'terraform_root' in cephtools.yaml to override."
        )

    return None


def resolve_plan_dir(plan: str, *, plan_relative: Path | None = None) -> Path:
    """
    Locate a Terragrunt plan directory by plan name.

    The directory must contain a terragrunt.hcl file. plan_relative provides an
    optional explicit relative path from the terraform root; when omitted the
    plan name is used.
    """
    checked: list[Path] = []
    seen: set[Path] = set()
    plan_path = Path(plan)

    def candidate_paths(base: Path) -> Iterable[Path]:
        yield base
        if plan_relative is not None:
            yield base / plan_relative
        if plan_relative is None or plan_relative != plan_path:
            yield base / plan_path

    for root_candidate in terraform_root_candidates():
        for candidate in candidate_paths(root_candidate.expanduser()):
            try:
                resolved = candidate.resolve()
            except FileNotFoundError:
                resolved = candidate
            if resolved in seen:
                continue
            seen.add(resolved)
            checked.append(resolved)
            if resolved.is_dir() and (resolved / "terragrunt.hcl").exists():
                return resolved

    locations = "\n  - ".join(str(path) for path in checked) or "<none>"
    raise click.ClickException(
        f"Terragrunt plan directory not found for '{plan}'. "
        "Checked locations:\n"
        f"  - {locations}\n"
        "Set CEPHTOOLS_TERRAFORM_ROOT or update terraform_root in the config."
    )
