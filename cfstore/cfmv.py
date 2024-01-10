import click
import timer
import json

from cfstore.config import CFSconfig
from cfstore.plugins.et_main import et_main
from cfstore.plugins.posix import RemotePosix
from cfstore.plugins.transfer import Transfer
from cfstore.plugins.jdma import JDMAInterface, Jasmin

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
def transfer(ctx, collection, source, destination, transfer_method):
    """
    Copy collection of files from source to destination

    :param collection: LIST of files which are to be moved. Collection must exist at source.
    :param source: Source location of collection
    :param destination: Destination location for collection
    :return:
    """
    filelist = json.load(collection)

    if transfer_method=="JDMA":
        JDMA_Transfer(source,destination,filelist)
    if transfer_method=="DELETE_FROM_GWS":
        GWS_Delete(filelist)


@click.argument("collection")
@click.argument("source")
@click.argument("destination")
@click.pass_context
@cli.command()
def JDMA_Transfer(ctx, collection, destination):
    """
    Copy collection of files from source to destination

    :param collection: Collection of files which are to be moved. Collection must exist at source.
    :param source: Source location of collection
    :param destination: Destination location for collection
    :return:
    """
    timer.initialise_timer()

    jasmin = Jasmin()

    jdma = JDMAInterface(workspace=destination)

    # Copy subset of streams
    jasmin.copy_streams()

    # Update the CFA
    jasmin.update_cfa()

    # Migrate data to Elastic Tape
    jdma.submit_migrate(collection)

    timer.finalise_timer()


@click.argument("collection")
@click.argument("source")
@click.argument("destination")
@click.pass_context
@cli.command()
def GWS_Delete(ctx, filelist, location):
    """
    Copy collection of files from source to destination

    :param filelist: List of files which are to be moved. Files must exist at source.
    :param location: The remote location of the GWS as stored on CFStore
    :return:
    """
    timer.initialise_timer()

    state = CFSconfig()

    # SSH
    # Setup Remote Posix as normal
    x = RemotePosix(state.db, location)
    host, user = (
        state.get_location(location)["host"],
        state.get_location(location)["user"],
    )
    x.configure(host, user)

    jasmin = Jasmin()

    for file in filelist:
        # Update the CFA
        jasmin.delete_from_cfa(file)
        x.ssh.delete(file)
    timer.finalise_timer()


if __name__ == "__main__":
    safe_cli()
