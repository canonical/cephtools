from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import click
import yaml

STATE_ENV_VAR = "CEPHTOOLS_STATE_HOME"


def default_state_home() -> Path:
    """Return the configured state directory without creating it."""
    configured = os.getenv(STATE_ENV_VAR)
    if configured:
        return Path(configured).expanduser()
    preferred_root = Path("~/src/cephtools").expanduser()
    if preferred_root.exists():
        return (preferred_root / "state").expanduser()
    fallback_root = Path("~/cephtools").expanduser()
    return (fallback_root / "state").expanduser()


def ensure_state_dir() -> Path:
    """Ensure the state directory exists and return it."""
    state_dir = default_state_home()
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir


def load_nested_yaml(path: Path) -> dict[str, Any]:
    """Load YAML content from disk and require a mapping at the document root."""
    target = path.expanduser()
    try:
        raw = target.read_text()
    except FileNotFoundError as exc:  # pragma: no cover - defensive
        raise click.ClickException(f"Expected state file at {target}") from exc

    try:
        parsed = yaml.safe_load(raw)
        data = {} if parsed is None else parsed
    except yaml.YAMLError as exc:
        raise click.ClickException(f"Failed to parse YAML in {target}: {exc}") from exc

    if not isinstance(data, dict):
        raise click.ClickException(
            f"{target} has unexpected YAML structure (expected a mapping)."
        )
    return data


def get_state_file(name: str, *, ensure_parent: bool = True) -> Path:
    """
    Return the path to a file under the state directory.

    When ensure_parent is True (default) the parent directories are created.
    """
    base = ensure_state_dir() if ensure_parent else default_state_home()
    path = base / name
    if ensure_parent:
        path.parent.mkdir(parents=True, exist_ok=True)
    return path
