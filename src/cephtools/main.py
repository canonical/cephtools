import click

from cephtools.reltool import charm_rel, list_prs
from cephtools.testflinger import cli as testflinger_cli
from cephtools.vmaas import cli as vmaas_cli


@click.group()
def cli():
    """cephtools main entrypoint."""


cli.add_command(list_prs)
cli.add_command(charm_rel)
cli.add_command(vmaas_cli, name="vmaas")
cli.add_command(testflinger_cli, name="testflinger")


if __name__ == "__main__":
    cli()
