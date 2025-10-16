from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Iterable

import click

from cephtools.testflinger import machine_ids

PLAN_PATHS = {
    "microceph": Path(__file__).resolve().parents[2] / "terraform" / "microceph",
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


@click.group(help="Juju orchestration helpers.")
def cli() -> None:
    """Root group for Juju helpers."""


@cli.command("deploy")
@click.option(
    "--plan",
    type=click.Choice(sorted(PLAN_PATHS)),
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
    plan_dir = PLAN_PATHS[plan]
    _run_terragrunt(plan_dir, var_args)

    click.echo("Terragrunt apply completed successfully.")
