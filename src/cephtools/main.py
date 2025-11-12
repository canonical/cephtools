import click

from cephtools.config import load_cephtools_config
from cephtools.reltool import charm_rel, list_prs
from cephtools.testenv import cli as testenv_cli
from cephtools.testflinger import cli as testflinger_cli
from cephtools.microceph import cli as microceph_cli
from cephtools.terraform import cli as terraform_cli

load_cephtools_config(ensure=True)


@click.group()
def cli():
    """cephtools main entrypoint."""


cli.add_command(list_prs)
cli.add_command(charm_rel)
cli.add_command(testenv_cli, name="testenv")
cli.add_command(testflinger_cli, name="testflinger")
cli.add_command(microceph_cli, name="microceph")
cli.add_command(terraform_cli, name="terraform")


if __name__ == "__main__":
    cli()
