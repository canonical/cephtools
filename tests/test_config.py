from __future__ import annotations

from pathlib import Path

import pytest

from cephtools import config


@pytest.fixture
def state_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / "state"
    monkeypatch.setenv("CEPHTOOLS_STATE_HOME", str(home))
    return home


def test_load_cephtools_config_creates_file_when_ensured(
    state_home: Path,
) -> None:
    cfg_path = state_home / config.CONFIG_FILENAME
    assert not cfg_path.exists()

    data = config.load_cephtools_config(ensure=True)

    assert cfg_path.exists()
    assert data["terraform_root"] == str(config.DEFAULT_TERRAFORM_ROOT)
    assert data["juju_model"] == config.DEFAULT_JUJU_MODEL
    assert data["testenv"] == config.DEFAULT_TESTENV_DEFAULTS
    assert "testenv:" in cfg_path.read_text()


def test_load_cephtools_config_returns_defaults_without_file(
    state_home: Path,
) -> None:
    cfg_path = state_home / config.CONFIG_FILENAME
    assert not cfg_path.exists()

    data = config.load_cephtools_config()

    assert not cfg_path.exists()
    assert data["terraform_root"] == str(config.DEFAULT_TERRAFORM_ROOT)
    assert data["juju_model"] == config.DEFAULT_JUJU_MODEL
    assert data["testenv"] == config.DEFAULT_TESTENV_DEFAULTS


def test_load_testenv_defaults_overrides_from_config(state_home: Path) -> None:
    cfg_path = state_home / config.CONFIG_FILENAME
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        "\n".join(
            [
                "cephtools:",
                "  terraform_root: ~/custom",
                "  testenv:",
                '    maas_version: "3.8"',
                "    admin: admin",
                "    admin_pw: secret",
                "    admin_mail: ops@example.com",
                "    lxdbridge: br0",
                "    vmhost: lab-host",
                "    maas_tag: custom-tag",
                "",
            ]
        )
    )

    defaults = config.load_testenv_defaults()

    assert defaults == {
        "maas_version": "3.8",
        "admin": "admin",
        "admin_pw": "secret",
        "admin_mail": "ops@example.com",
        "lxdbridge": "br0",
        "vmhost": "lab-host",
        "maas_tag": "custom-tag",
    }


def test_load_testenv_defaults_rejects_legacy_maas_ch(state_home: Path) -> None:
    cfg_path = state_home / config.CONFIG_FILENAME
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        "\n".join(
            [
                "cephtools:",
                "  testenv:",
                "    maas_ch: 3.6/stable",
                "",
            ]
        )
    )

    with pytest.raises(config.click.ClickException) as excinfo:
        config.load_testenv_defaults()

    assert "no longer supported" in str(excinfo.value)


def test_load_testenv_defaults_requires_quoted_maas_version(
    state_home: Path,
) -> None:
    cfg_path = state_home / config.CONFIG_FILENAME
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        "\n".join(
            [
                "cephtools:",
                "  testenv:",
                "    maas_version: 3.10",
                "",
            ]
        )
    )

    with pytest.raises(config.click.ClickException) as excinfo:
        config.load_testenv_defaults()

    assert "must be a quoted string" in str(excinfo.value)


def test_write_default_config_quotes_maas_version(state_home: Path) -> None:
    cfg_path = state_home / config.CONFIG_FILENAME

    config.load_cephtools_config(ensure=True)

    assert 'maas_version: "3.7"' in cfg_path.read_text()
