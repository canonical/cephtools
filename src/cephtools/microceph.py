from __future__ import annotations

import shlex
import subprocess
from typing import Callable, Iterable, Sequence

import click

from cephtools.config import load_cephtools_config
from cephtools.juju_utils import application_machines


def _resolve_nodes(nodes_override: Iterable[str] | None = None) -> tuple[int, ...]:
    """Determine MicroCeph nodes from CLI overrides or Juju status."""
    if nodes_override:
        nodes = tuple(_validate_node_value(node, origin="--nodes") for node in nodes_override)
        if not nodes:
            raise click.ClickException("At least one node must be provided via --nodes.")
        return nodes

    config = load_cephtools_config(ensure=True)
    model_value = config.get("juju_model")
    if not isinstance(model_value, str) or not model_value:
        raise click.ClickException(
            "Unable to determine Juju model. Configure 'juju_model' or provide --nodes."
        )

    nodes = application_machines(model_value, "microceph")
    if not nodes:
        raise click.ClickException(
            "No microceph units found in Juju status. Ensure the application is deployed or provide --nodes."
        )

    return nodes


def _validate_node_value(node: object, *, origin: str) -> int:
    if isinstance(node, str):
        node = node.strip()
        if not node:
            raise click.ClickException(f"{origin} entries must be non-empty integers.")
        if not node.isdigit():
            raise click.ClickException(f"{origin} entries must be integers, got {node!r}.")
        value = int(node, 10)
    elif isinstance(node, int):
        value = node
    else:
        raise click.ClickException(f"{origin} entries must be integers or numeric strings.")

    if value < 0:
        raise click.ClickException(f"{origin} entries must be non-negative integers.")

    return value


def _ssh_run(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


CommandFactory = Callable[[int], Sequence[str]]


def _run_on_all_nodes(
    nodes: Sequence[int],
    command_factory: CommandFactory,
    *,
    use_sudo: bool = True,
    dry_run: bool = False,
) -> None:
    if not nodes:
        raise click.ClickException("No nodes available to run the command.")

    failures: list[tuple[int, int, str]] = []

    for node in nodes:
        remote_command = list(command_factory(node))
        if not remote_command:
            raise click.ClickException("Command factory must return at least one argument per node.")

        display_command = ["sudo", *remote_command] if use_sudo else list(remote_command)
        quoted = " ".join(shlex.quote(part) for part in display_command)
        click.echo(f"[{node}] {quoted}")

        if dry_run:
            continue

        ssh_command = ["juju", "ssh", str(node), *display_command]
        completed = _ssh_run(ssh_command)

        if completed.stdout:
            click.echo(completed.stdout, nl=False)
        if completed.stderr:
            click.echo(completed.stderr, err=True, nl=False)

        if completed.returncode != 0:
            failures.append((node, completed.returncode, completed.stderr.strip()))

    if failures:
        error_lines = [
            f"- {node}: exit code {returncode}"
            + (f" (stderr: {stderr})" if stderr else "")
            for node, returncode, stderr in failures
        ]
        raise click.ClickException(
            "Command failed on one or more nodes:\n" + "\n".join(error_lines)
        )


@click.group(help="MicroCeph cluster helpers.")
def cli() -> None:
    """Root command group for MicroCeph utilities."""


@cli.group(help="MicroCeph disk management helpers.")
def disk() -> None:
    """Disk operations for MicroCeph units."""


@disk.command("add")
@click.argument("disk_args", nargs=-1, required=True)
@click.option(
    "--nodes",
    multiple=True,
    help="Override the configured node list (may be provided multiple times).",
)
@click.option(
    "--sudo/--no-sudo",
    default=True,
    show_default=True,
    help="Execute microceph commands with sudo on the remote hosts.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Only print the commands that would be executed without running them.",
)
def add_disk(
    disk_args: tuple[str, ...],
    nodes: tuple[str, ...],
    sudo: bool,
    dry_run: bool,
) -> None:
    """Add disks to every node in the MicroCeph cluster."""

    resolved_nodes = _resolve_nodes(nodes_override=nodes or None)

    def _command_factory(node: int) -> list[str]:
        return ["microceph", "disk", "add", *disk_args]

    _run_on_all_nodes(
        resolved_nodes,
        _command_factory,
        use_sudo=sudo,
        dry_run=dry_run,
    )
