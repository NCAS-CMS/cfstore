#!/usr/bin/env python3

import json

import click

from cfstore.config import CFSconfig
from cfstore.plugins.et_main import et_main
from cfstore.plugins.jdma import Jasmin, JDMAInterface
from cfstore.plugins.posix import RemotePosix
from cfstore.plugins.transfer import Transfer


def safe_cli():
    """
    Traps all ValueErrors which bubble up from the things click calls, and
    removes the stack trace ...
    """
    try:
        cli()
    except ValueError as e:
        click.echo(e)


STATE_FILE = ".cftape"


def _save(view_state):
    """Save view state if valid"""
    if not view_state["db"]:
        raise ValueError("Save option requires default database value")
    with open(STATE_FILE, "w") as f:
        json.dump(view_state, f)


def _load():
    """Load existing view state"""
    return CFSconfig()


def _set_context(ctx):
    """
    Set the config_state context
    Importantly, this sets the active collection
    """

    def doset(c):
        if c == "all":
            config_state["last_collection"] = ""
        else:
            config_state["last_collection"] = c

    config_state = _load()

    return config_state, config_state.db


@click.group()
@click.pass_context
def cli(ctx):
    """
    Provides the overall group context for command line arguments
    """
    ctx.ensure_object(dict)


@cli.command()
@click.pass_context
@click.argument("collection", nargs=1)
@click.argument("transfer_method", nargs=1)
@click.option("--source", default="None", help="the path of the source location")
@click.option("--destination", default="None", help="the path of the destination")
@click.option("--location", default="None", help="the location as stored by cfstore")
def transfer(ctx, collection, transfer_method, source, destination, location):
    """
    Copy collection of files from source to destination

    :param collection: File containing manifest of files to be transfered.
    :param transfer_method: Which method of transfer (currently JDMA for archiving or DELETE_FROM_GWS to remove from workspace)
    :param source: Source location of collection
    :param destination: Destination location for collection
    :return:
    """

    print(collection, location)

    if transfer_method == "JDMA":
        JDMA_Transfer(ctx, destination, collection)
    elif transfer_method == "DELETE_FROM_GWS":
        GWS_Delete(ctx, collection, location)
    else:
        print("Select one of JDMA or DELETE_FROM_GWS")


def JDMA_Transfer(ctx, collection, destination):
    """
    Copy collection of files from source to destination

    :param collection: Collection of files which are to be moved. Collection must exist at source.
    :param destination: Destination location for collection
    :return:
    """

    jasmin = Jasmin()

    jdma = JDMAInterface(workspace=destination)

    with open(collection) as f:
        filelist = json.load(f)
    # Copy subset of streams
    jasmin.copy_streams()

    # Update the CFA
    jasmin.update_cfa()

    # Migrate data to Elastic Tape
    jdma.submit_migrate(filelist)


def GWS_Delete(ctx, collection, location):
    """
    Copy collection of files from source to destination

    :param filelist: List of files which are to be moved. Files must exist at source.
    :param location: The remote location of the GWS as stored on CFStore
    :return:
    """

    state, db = _set_context(ctx)
    # SSH
    # Setup Remote Posix as normal
    x = RemotePosix(state.db, location)
    host, user = (
        state.get_location(location)["host"],
        state.get_location(location)["user"],
    )
    x.configure(host, user)

    with open(collection) as f:
        filelist = json.load(f)
    print(filelist)
    with open("json/cfadeletelist.json", "w+") as f:
        deletefile = json.dump(filelist, f)
    x.ssh.delete_from_master_cfa(
        "json/cfadeletelist.json",
    )
    totalfilesize = 0
    for file in filelist:
        # Update the CFA
        f = db.retrieve_file_if_present(file)
        if f:
            totalfilesize += f.size
    answer = input(
        f"This will open up {totalfilesize} worth of space on {location}. y/n"
    )
    if answer.lower() == "y":
        print("Clearing space")
        # for file in filelist:
        #   x.ssh.delete(file)
    elif answer.lower() == "n":
        print("No actions")
    else:
        print("Please enter y or n.")


if __name__ == "__main__":
    safe_cli()
