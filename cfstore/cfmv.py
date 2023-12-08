import click
import timer

from cfstore.config import CFSconfig
from cfstore.plugins.et_main import et_main
from cfstore.plugins.jasmin import Jasmin
from cfstore.plugins.posix import RemotePosix


def safe_cli():
    """
    Traps all ValueErrors which bubble up from the things click calls, and
    removes the stack trace ...
    """
    try:
        cli()
    except ValueError as e:
        click.echo(e)


@click.group()
@click.pass_context
def cli(ctx):
    """
    Provides the overall group context for command line arguments
    """
    ctx.ensure_object(dict)


@click.argument("collection")
@click.argument("source")
@click.argument("destination")
@click.pass_context
@cli.command()
def copy(ctx, collection, source, destination):
    """
    Copy collection of files from source to destination

    :param collection: Collection of files which are to be moved. Collection must exist at source.
    :param source: Source location of collection
    :param destination: Destination location for collection
    :return:
    """
    print("\nOk, we confess, we haven't got to this yet\n")
    raise NotImplementedError


@click.argument("collection")
@click.argument("source")
@click.argument("destination")
@click.pass_context
@cli.command()
def JDMA_Transfer(ctx, source, destination):
    """
    Copy collection of files from source to destination

    :param collection: Collection of files which are to be moved. Collection must exist at source.
    :param source: Source location of collection
    :param destination: Destination location for collection
    :return:
    """
    timer.initialise_timer()

    jasmin = Jasmin()

    # Copy subset of streams
    jasmin.copy_streams()

    # Generate a cfa
    jasmin.generate_cfa()

    # Migrate data to Elastic Tape
    jasmin.jdma_migrate()

    timer.finalise_timer()


if __name__ == "__main__":
    safe_cli()
