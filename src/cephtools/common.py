from __future__ import annotations

import shlex
import subprocess
from collections.abc import Sequence
from typing import Union

Command = Union[str, Sequence[object]]


def _format_command(command: Command) -> str:
    """Return a human-friendly representation of a command for logs."""
    if isinstance(command, str):
        return command
    return " ".join(shlex.quote(str(part)) for part in command)


def run(
    command: Command,
    *,
    check: bool = True,
    shell: bool = False,
    quiet: bool = False,
) -> subprocess.CompletedProcess[str]:
    """
    Execute a command and return the completed process.

    The helper normalises different command formats and consistently captures
    stdout for downstream parsing.
    """
    if not quiet:
        print(f"+ {_format_command(command)}")

    if shell:
        if not isinstance(command, str):
            command = " ".join(str(part) for part in command)
        return subprocess.run(
            command,
            check=check,
            text=True,
            stdout=subprocess.PIPE,
            shell=True,
        )

    if isinstance(command, str):
        command = shlex.split(command)
    else:
        command = [str(part) for part in command]

    return subprocess.run(
        command,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        shell=False,
    )


def ensure_snap(
    name: str, channel: str | None = None, *, classic: bool = False
) -> None:
    """
    Ensure a snap is present, installing it if necessary.
    """
    out = run(["snap", "list"])
    lines = out.stdout.splitlines()
    for line in lines[1:]:
        columns = line.split()
        if columns and columns[0] == name:
            return

    command: list[str] = ["sudo", "snap", "install", name]
    if channel:
        command.append(f"--channel={channel}")
    if classic:
        command.append("--classic")

    run(command)
