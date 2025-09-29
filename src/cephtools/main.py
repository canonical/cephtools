import click

from cephtools.reltool import list_prs, charm_rel
from cephtools.vmaas import cli as vmaas_cli


@click.group()
def cli():
    """cephtools main entrypoint."""


cli.add_command(list_prs)
cli.add_command(charm_rel)
cli.add_command(vmaas_cli, name="vmaas")


if __name__ == "__main__":
    cli()
