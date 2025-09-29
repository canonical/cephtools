from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Iterable

import click
import jubilant
from jubilant import WaitError, all_active, any_error

from cephtools import terraform
from cephtools.config import load_cephtools_config

PLAN_RELATIVE = {
    "microceph": Path("microceph"),
}


def _build_var_args(variables: dict[str, object]) -> list[str]:
    args: list[str] = []
    for key, value in variables.items():
        if value is None:
            continue
        if isinstance(value, (list, dict)):
            encoded = json.dumps(value)
        else:
            encoded = str(value)
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
    except FileNotFoundError as exc:
        raise click.ClickException("terragrunt is not installed or not in PATH.") from exc
    except subprocess.CalledProcessError as exc:
        raise click.ClickException(
            f"terragrunt apply failed with exit code {exc.returncode}."
        ) from exc


def _model_metadata(model: str) -> tuple[str, str]:
    """Return the model UUID and owner for the specified Juju model."""
    juju = jubilant.Juju()
    try:
        output = juju.cli(
            "show-model",
            model,
            "--format",
            "json",
            include_model=False,
        )
    except jubilant.CLIError as exc:
        stderr = (exc.stderr or "").strip()
        if not stderr:
            stderr = "unknown error"
        raise click.ClickException(
            f"Failed to describe model '{model}': {stderr}"
        ) from exc

    try:
        payload = json.loads(output or "{}")
    except json.JSONDecodeError as exc:
        raise click.ClickException(
            f"Failed to decode Juju show-model output for '{model}'."
        ) from exc

    model_data = payload.get(model)
    if not isinstance(model_data, dict):
        raise click.ClickException(
            f"Model '{model}' not found in Juju show-model output."
        )

    uuid_value = model_data.get("model-uuid")
    if not isinstance(uuid_value, str):
        raise click.ClickException(
            f"Unable to determine UUID for model '{model}'."
        )

    owner_value = model_data.get("owner")
    if not isinstance(owner_value, str):
        raise click.ClickException(
            f"Unable to determine owner for model '{model}'."
        )

    owner = owner_value.split("@", 1)[0]
    return uuid_value, owner


def _wait_for_model_active(model: str, *, timeout: int) -> None:
    if timeout <= 0:
        raise click.ClickException("--wait-timeout must be a positive integer.")

    juju = jubilant.Juju(model=f"{MAAS_CONTROLLER}:{model}")
    click.echo(
        f"Waiting for applications in model '{model}' to become active "
        f"(timeout {timeout}s)..."
    )
    try:
        juju.wait(all_active, error=any_error, timeout=timeout)
    except TimeoutError as exc:
        raise click.ClickException(
            f"Timed out waiting for applications in model '{model}' "
            f"to become active after {timeout} seconds."
        ) from exc
    except WaitError as exc:
        raise click.ClickException(
            f"Encountered an error state while waiting for model '{model}': {exc}"
        ) from exc


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
    help="Juju model to deploy into (defaults to config juju_model).",
)
@click.option(
    "--units",
    type=int,
    default=3,
    show_default=True,
    help="Number of units to deploy.",
)
@click.option(
    "--charm-channel",
    help="Charm channel to deploy the microceph charm from.",
)
@click.option(
    "--charm-revision",
    type=int,
    help="Specific charm revision to deploy.",
)
@click.option(
    "--base",
    help="Base to deploy the microceph charm with.",
)
@click.option(
    "--snap-channel",
    help="Snap channel for the microceph workload.",
)
@click.option(
    "--wait/--no-wait",
    default=True,
    show_default=True,
    help="Wait for applications in the target model to become active.",
)
@click.option(
    "--wait-timeout",
    type=int,
    default=3600,
    show_default=True,
    help="Seconds to wait for readiness when --wait is enabled.",
)
def deploy(
    plan: str,
    model: str | None,
    units: int,
    charm_channel: str | None,
    charm_revision: int | None,
    base: str | None,
    snap_channel: str | None,
    wait: bool,
    wait_timeout: int,
) -> None:
    """Deploy a Juju application managed by Terragrunt."""
    if units <= 0:
        raise click.ClickException("--units must be a positive integer.")

    if model is None:
        config = load_cephtools_config(ensure=True)
        model = config.get("juju_model")
        if not model:
            raise click.ClickException(
                "Unable to determine model: set --model or configure juju_model in cephtools.yaml."
            )

    model_uuid, model_owner = _model_metadata(model)

    click.echo(
        f"Deploying plan '{plan}' to model '{model}' "
        f"(uuid={model_uuid}, owner={model_owner})"
    )

    variables: dict[str, object] = {
        "model_uuid": model_uuid,
        "units": units,
    }
    if charm_channel:
        variables["charm_microceph_channel"] = charm_channel
    if charm_revision is not None:
        variables["charm_microceph_revision"] = charm_revision
    if base:
        variables["base"] = base
    if snap_channel:
        variables["snap_channel"] = snap_channel

    var_args = _build_var_args(variables)
    plan_dir = terraform.resolve_plan_dir(plan, plan_relative=PLAN_RELATIVE[plan])
    _run_terragrunt(plan_dir, var_args)

    click.echo("Terragrunt apply completed successfully.")
    if wait:
        _wait_for_model_active(model, timeout=wait_timeout)
