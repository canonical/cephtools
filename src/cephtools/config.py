from __future__ import annotations

from pathlib import Path
from typing import Any

import click
import yaml

from cephtools.state import get_state_file

CONFIG_FILENAME = "cephtools.yaml"

DEFAULT_TERRAFORM_ROOT = Path("~/src/cephtools/terraform").expanduser()


def default_testflinger_config_path() -> Path:
    return get_state_file("testflinger.yaml", ensure_parent=False)


DEFAULT_TESTFLINGER_CONFIG_PATH = default_testflinger_config_path()
DEFAULT_TESTFLINGER_RESERVE_FOR = 3600
DEFAULT_TESTFLINGER_DEPLOY_RESERVE_FOR = 7200

DEFAULT_JUJU_MODEL = "cephtools"

_TESTENV_DEFAULTS_FALLBACK: dict[str, str] = {
    "maas_ch": "3.6/stable",
    "admin": "admin",
    "admin_pw": "maaspass",
    "admin_mail": "admin@example.com",
    "lxdbridge": "lxdbr0",
    "vmhost": "local-lxd",
    "maas_tag": "cephtools",
}

DEFAULT_TESTENV_DEFAULTS = _TESTENV_DEFAULTS_FALLBACK.copy()


def load_nested_yaml(path: Path) -> dict[str, Any]:
    """Load YAML content from disk and ensure the result is a mapping."""
    target = path.expanduser()
    try:
        raw = target.read_text()
    except FileNotFoundError as exc:  # pragma: no cover - defensive
        raise click.ClickException(f"Expected configuration file at {target}") from exc

    try:
        data = yaml.safe_load(raw) or {}
    except yaml.YAMLError as exc:
        raise click.ClickException(f"Failed to parse YAML in {target}: {exc}") from exc

    if not isinstance(data, dict):
        raise click.ClickException(
            f"{target} has unexpected YAML structure (expected a mapping)."
        )

    return data


def _write_default_config(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    testenv_lines = [
        "  testenv:",
        *[f"    {key}: {value}" for key, value in _TESTENV_DEFAULTS_FALLBACK.items()],
    ]
    content = "\n".join(
        [
            "cephtools:",
            f"  terraform_root: {DEFAULT_TERRAFORM_ROOT}",
            f"  juju_model: {DEFAULT_JUJU_MODEL}",
            *testenv_lines,
            "",
        ]
    )
    path.write_text(content)


def _config_path() -> Path:
    return get_state_file(CONFIG_FILENAME)


def load_cephtools_config(
    path: Path | None = None, *, ensure: bool = False
) -> dict[str, Any]:
    target = (path or _config_path()).expanduser()
    if ensure and not target.exists():
        _write_default_config(target)
    if not target.exists():
        return {
            "terraform_root": str(DEFAULT_TERRAFORM_ROOT),
            "juju_model": DEFAULT_JUJU_MODEL,
            "testenv": _TESTENV_DEFAULTS_FALLBACK.copy(),
        }
    data = load_nested_yaml(target)
    section = data.get("cephtools")
    if section is None:
        section = data
    if not isinstance(section, dict):
        raise click.ClickException(
            f"{target} has unexpected structure for the 'cephtools' section."
        )

    juju_model = section.get("juju_model")
    if juju_model is None:
        section["juju_model"] = DEFAULT_JUJU_MODEL
    elif not isinstance(juju_model, str):
        raise click.ClickException(
            f"{target} has unexpected type for 'juju_model'; expected string."
        )

    testenv_section = section.get("testenv")
    if testenv_section is None:
        section["testenv"] = _TESTENV_DEFAULTS_FALLBACK.copy()
    elif not isinstance(testenv_section, dict):
        raise click.ClickException(
            f"{target} has unexpected structure for the 'testenv' section."
        )

    return section


def load_testenv_defaults(path: Path | None = None) -> dict[str, str]:
    """Return testenv defaults from the configuration, with legacy support."""
    config_section = load_cephtools_config(path, ensure=True)
    testenv_section = config_section.get("testenv")
    if testenv_section is None:
        return _TESTENV_DEFAULTS_FALLBACK.copy()
    if not isinstance(testenv_section, dict):
        raise click.ClickException(
            "The 'testenv' configuration section must be a mapping."
        )

    defaults = _TESTENV_DEFAULTS_FALLBACK.copy()
    for key, value in testenv_section.items():
        if value is None:
            continue
        if not isinstance(value, str):
            raise click.ClickException(
                f"Configuration value 'testenv.{key}' must be a string."
            )
        defaults[key] = value
    return defaults
