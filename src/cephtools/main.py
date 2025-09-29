import click

from cephtools.config import load_cephtools_config
from cephtools.reltool import charm_rel, list_prs
from cephtools.testflinger import cli as testflinger_cli
from cephtools.vmaas import cli as vmaas_cli
from cephtools.juju import cli as juju_cli
from cephtools.microceph import cli as microceph_cli

load_cephtools_config(ensure=True)


@click.group()
def cli():
    """cephtools main entrypoint."""


cli.add_command(list_prs)
cli.add_command(charm_rel)
cli.add_command(vmaas_cli, name="vmaas")
cli.add_command(testflinger_cli, name="testflinger")
cli.add_command(juju_cli, name="juju")
cli.add_command(microceph_cli, name="microceph")


if __name__ == "__main__":
    cli()
