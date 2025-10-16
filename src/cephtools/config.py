from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import click

from cephtools.state import get_state_file

CONFIG_FILENAME = "cephtools.yaml"

DEFAULT_TERRAFORM_ROOT = Path("~/src/cephtools/terraform").expanduser()
def default_testflinger_config_path() -> Path:
    return get_state_file("testflinger.yaml", ensure_parent=False)


DEFAULT_TESTFLINGER_CONFIG_PATH = default_testflinger_config_path()
DEFAULT_TESTFLINGER_RESERVE_FOR = 3600
DEFAULT_TESTFLINGER_DEPLOY_RESERVE_FOR = 7200
DEFAULT_VMAAS_DEFAULTS = dict(
    maas_ch=os.getenv("MAAS_CH", "3.6/stable"),
    admin=os.getenv("MAAS_ADMIN", "admin"),
    admin_pw=os.getenv("MAAS_ADMIN_PW", "maaspass"),
    admin_mail=os.getenv("MAAS_ADMIN_MAIL", "admin@example.com"),
    lxdbridge=os.getenv("LXDBRIDGE", "lxdbr0"),
    vmhost=os.getenv("VMHOST", "local-lxd"),
)


def _coerce_yaml_scalar(value: str) -> str | list[str] | None:
    lower = value.lower()
    if lower in {"null", "none", "~"}:
        return None
    if (value.startswith("'") and value.endswith("'")) or (
        value.startswith('"') and value.endswith('"')
    ):
        value = value[1:-1]
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [
            item.strip().strip("'").strip('"')
            for item in inner.split(",")
            if item.strip()
        ]
    return value


def load_nested_yaml(path: Path) -> dict[str, Any]:
    target = path.expanduser()
    lines = target.read_text().splitlines()
    root: dict[str, Any] = {}
    stack: list[tuple[dict[str, Any], int]] = [(root, -1)]
    for raw_line in lines:
        if not raw_line.strip() or raw_line.strip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        if ":" not in raw_line:
            raise click.ClickException(
                f"Invalid YAML line in {target}: '{raw_line}'"
            )
        key, raw_value = raw_line.split(":", 1)
        key = key.strip()
        value = raw_value.strip()
        while stack and indent <= stack[-1][1]:
            stack.pop()
        parent = stack[-1][0]
        if value == "":
            new_mapping: dict[str, Any] = {}
            parent[key] = new_mapping
            stack.append((new_mapping, indent))
        else:
            parent[key] = _coerce_yaml_scalar(value)
    return root


def _write_default_config(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(
        [
            "cephtools:",
            f"  terraform_root: {DEFAULT_TERRAFORM_ROOT}",
            "",
        ]
    )
    path.write_text(content)


def _config_path() -> Path:
    return get_state_file(CONFIG_FILENAME)


def load_cephtools_config(path: Path | None = None, *, ensure: bool = False) -> dict[str, Any]:
    target = (path or _config_path()).expanduser()
    if ensure and not target.exists():
        _write_default_config(target)
    if not target.exists():
        return {"terraform_root": str(DEFAULT_TERRAFORM_ROOT)}
    data = load_nested_yaml(target)
    section = data.get("cephtools")
    if section is None:
        section = data
    if not isinstance(section, dict):
        raise click.ClickException(
            f"{target} has unexpected structure for the 'cephtools' section."
        )
    return section


def ensure_cephtools_config(path: Path | None = None) -> dict[str, Any]:
    return load_cephtools_config(path, ensure=True)


def read_cephtools_config(path: Path | None = None) -> dict[str, Any]:
    return load_cephtools_config(path, ensure=True)
