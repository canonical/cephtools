from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Iterable

import click

from cephtools.config import (
    DEFAULT_TERRAFORM_ROOT,
    ensure_cephtools_config,
    read_cephtools_config,
)
from cephtools.testflinger import machine_ids

PLAN_RELATIVE = {
    "microceph": Path("microceph"),
}


def _build_var_args(variables: dict[str, str | list[str]]) -> list[str]:
    args: list[str] = []
    for key, value in variables.items():
        if isinstance(value, list):
            encoded = json.dumps(value)
        else:
            encoded = value
        args.extend(["-var", f"{key}={encoded}"])
    return args


def _run_terragrunt(
    plan_dir: Path,
    var_args: Iterable[str],
) -> None:
    if not plan_dir.exists():
        raise click.ClickException(f"Terragrunt plan directory not found: {plan_dir}")

    cmd = ["terragrunt", "apply", "-auto-approve", *var_args]
    try:
        subprocess.run(cmd, cwd=str(plan_dir), check=True)
    except FileNotFoundError as exc:  # pragma: no cover - depends on environment
        raise click.ClickException("terragrunt is not installed or not in PATH.") from exc
    except subprocess.CalledProcessError as exc:
        raise click.ClickException(
            f"terragrunt apply failed with exit code {exc.returncode}."
        ) from exc


def _resolve_plan_dir(plan: str) -> Path:
    rel_path = PLAN_RELATIVE[plan]
    ensure_cephtools_config()

    base_dirs: list[Path] = []
    env_root = os.environ.get("CEPHTOOLS_TERRAFORM_ROOT")
    if env_root:
        base_dirs.append(Path(env_root).expanduser())

    config = read_cephtools_config()
    raw_root = config.get("terraform_root")
    if raw_root is None:
        terraform_root = DEFAULT_TERRAFORM_ROOT
    elif isinstance(raw_root, str):
        terraform_root = Path(raw_root).expanduser()
    else:
        raise click.ClickException(
            "Configuration value 'terraform_root' must be a string path."
        )
    base_dirs.append(terraform_root)

    cwd_root = Path.cwd()
    for parent in (cwd_root,) + tuple(cwd_root.parents):
        base_dirs.append(parent / "terraform")

    package_root = Path(__file__).resolve().parents[2] / "terraform"
    base_dirs.append(package_root)

    checked: list[Path] = []

    def is_plan_dir(path: Path) -> bool:
        return path.is_dir() and (path / "terragrunt.hcl").exists()

    seen: set[Path] = set()
    for base in base_dirs:
        base = base.expanduser()
        try:
            resolved_base = base.resolve()
        except FileNotFoundError:
            resolved_base = base
        if resolved_base in seen:
            continue
        seen.add(resolved_base)

        for candidate in (
            resolved_base,
            resolved_base / rel_path,
            resolved_base / plan,
        ):
            checked.append(candidate)
            if is_plan_dir(candidate):
                return candidate.resolve()

    locations = "\n  - ".join(str(p) for p in checked) or "<none>"
    raise click.ClickException(
        f"Terragrunt plan directory not found for '{plan}'. "
        "Checked locations:\n"
        f"  - {locations}\n"
        "Set CEPHTOOLS_TERRAFORM_ROOT or update terraform_root in the config."
    )


@click.group(help="Juju orchestration helpers.")
def cli() -> None:
    """Root group for Juju helpers."""


@cli.command("deploy")
@click.option(
    "--plan",
    type=click.Choice(sorted(PLAN_RELATIVE)),
    required=True,
    help="Terragrunt plan to execute.",
)
@click.option(
    "--model",
    required=True,
    help="Juju model to deploy into.",
)
@click.option(
    "--units",
    type=int,
    default=3,
    show_default=True,
    help="Number of units to deploy.",
)
@click.option(
    "--offset",
    type=int,
    default=0,
    show_default=True,
    help="Starting index when selecting machine placements.",
)
def deploy(plan: str, model: str, units: int, offset: int) -> None:
    """Deploy a Juju application managed by Terragrunt."""
    if units <= 0:
        raise click.ClickException("--units must be a positive integer.")
    if offset < 0:
        raise click.ClickException("--offset must be zero or a positive integer.")

    placements = machine_ids(units, offset=offset)
    if len(placements) < units:
        raise click.ClickException(
            f"Requested {units} placements but only found {len(placements)} machines."
        )

    click.echo(
        f"Deploying plan '{plan}' to model '{model}' with placements: {', '.join(placements)}"
    )

    variables = {
        "model": model,
        "units": str(units),
        "placements": placements,
    }
    var_args = _build_var_args(variables)
    plan_dir = _resolve_plan_dir(plan)
    _run_terragrunt(plan_dir, var_args)

    click.echo("Terragrunt apply completed successfully.")
