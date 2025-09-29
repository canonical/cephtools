from __future__ import annotations

import json
import click
import jubilant

__all__ = ["application_machines"]


def _format_juju_error(exc: jubilant.CLIError) -> str:
    stderr = (exc.stderr or "").strip()
    if stderr:
        return stderr
    output = (getattr(exc, "output", "") or "").strip()
    if output:
        return output
    return f"exit code {getattr(exc, 'returncode', 'unknown')}"


def _coerce_machine(value: object) -> int:
    if isinstance(value, int):
        if value < 0:
            raise ValueError("machine id must be non-negative")
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or not stripped.isdigit():
            raise ValueError("machine id must be numeric")
        return int(stripped, 10)
    raise ValueError("unsupported machine id type")


def application_machines(model: str, application: str) -> tuple[int, ...]:
    """
    Return the machine numbers hosting the specified application in the given model.
    """
    juju = jubilant.Juju(model=model)
    try:
        status_output = juju.cli("status", "--format", "json")
    except jubilant.CLIError as exc:
        message = _format_juju_error(exc)
        raise click.ClickException(f"Failed to fetch Juju status: {message}") from exc

    payload = json.loads(status_output or "{}")
    applications = payload.get("applications")
    app_entry = applications.get(application)
    if not isinstance(app_entry, dict):
        return ()

    units = app_entry.get("units")
    if not isinstance(units, dict):
        return ()

    machines: list[int] = []
    for unit_name, unit_data in units.items():
        machine_value = unit_data.get("machine")
        if machine_value is None:
            continue
        try:
            machine_id = _coerce_machine(machine_value)
        except ValueError:
            continue
        machines.append(machine_id)

    return tuple(sorted(set(machines)))
