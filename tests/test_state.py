from pathlib import Path

import click
import pytest

from cephtools.state import load_nested_yaml


@pytest.mark.parametrize("content", ["[]\n", "false\n", "0\n", '"text"\n'])
def test_load_nested_yaml_rejects_non_mapping_roots(
    tmp_path: Path, content: str
) -> None:
    path = tmp_path / "state.yaml"
    path.write_text(content)

    with pytest.raises(click.ClickException, match="expected a mapping"):
        load_nested_yaml(path)


def test_load_nested_yaml_treats_null_as_empty_mapping(tmp_path: Path) -> None:
    path = tmp_path / "state.yaml"
    path.write_text("null\n")

    assert load_nested_yaml(path) == {}
