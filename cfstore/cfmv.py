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
@click.argument("transfer_method", nargs=1)
@click.option("--destination", default="None", help="the location of the gws directory where files will end up (only needed when moving)")
@click.option("--filemanifest", default="None", help="the location of the file manifest, containing the paths of which files are to transfered")
@click.option("--location", default="None", help="the location as stored by cfstore")
def transfer(ctx, transfer_method, filemanifest, destination, location):
    """
    Copy collection of files from source to destination

    :param transfer_method: Which method of transfer (currently JDMA for archiving or DELETE_FROM_GWS to remove from workspace)
    :param source: Source location of collection
    :param destination: Destination location for collection
    :return:
    """

    disc,tape,unavailable,other = get_file_json(filemanifest)
    print(disc,unavailable)
    if transfer_method == "JDMA":
        JDMA_Transfer(ctx, tape, location, destination)
    elif transfer_method == "GWS_DELETE":
        GWS_Delete(ctx, unavailable, location)
    elif transfer_method == "GWS_MOVE":
        GWS_Move(ctx, disc, location, destination)

    else:
        print("Select one of JDMA, GWS_MOVE or GWS_DELETE")


def get_file_json(filemanifest):
    print(filemanifest)
    with open(filemanifest) as f:
        jsonfiles = json.load(f)
    disc,tape,unavailable,other = jsonfiles["disc"][:-1],jsonfiles["tape"][:-1],jsonfiles["unavailable"][:-1],jsonfiles["other"][:-1]
    return disc,tape,unavailable,other

def JDMA_Transfer(ctx, filelist, location, destination):
    """
    Copy collection of files from source to destination

    :param collection: Collection of files which are to be moved. Collection must exist at source.
    :param destination: Destination location for collection
    :return:
    """

    jasmin = Jasmin()

    jdma = JDMAInterface(workspace=destination)


    # Copy subset of streams
    jasmin.copy_streams()

    # Update the CFA
    jasmin.update_cfa()

    # Migrate data to Elastic Tape
    jdma.submit_migrate(filelist)

def GWS_Move(ctx, filelist, location, destination):
    """
    Move a collection of files from one are on a gws to a new one
    """

    state, db = _set_context(ctx)
    x = RemotePosix(state.db, location)
    host, user = (
        state.get_location(location)["host"],
        state.get_location(location)["user"],
    )
    x.configure(host, user)

    for file in filelist:
        x.ssh.move_file(file[1], destination+"/"+file[0])
def GWS_Delete(ctx, filelist, location):
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

def update_cfa(filelist):
    pass

if __name__ == "__main__":
    safe_cli()
