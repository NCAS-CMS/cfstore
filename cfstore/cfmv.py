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
@click.argument("transfer_method", nargs=1, help="Select one of JDMA,GWS_DELETE or GWS_MOVE as the transfer operation")
@click.option("--destination", default="None", help="the location of the gws directory where files will end up (only needed when moving)")
@click.option("--filemanifest", default="None", help="filemanifest: A json file containing the list of files (format {disc:[list of files],tape:[list of files],unavailable:[list of files],other:[list of files])")
@click.option("--location", default="None", help="the location as stored by cfstore")
def transfer(ctx, transfer_method, filemanifest, destination, location):
    """
    Perform one of three transfer operations:
        JDMA: Transfer files from tape to disc using a JDMA transfer
        GWS_MOVE: Transfer a file from disc to a new location on disc
        GWS_DELETE: Remove a file from disc (will not delete from tape)
    """

    disc,tape,unavailable,other = get_file_json(filemanifest)
    print(disc,unavailable)
    if transfer_method == "JDMA":
        JDMA_Transfer(ctx, tape, location, destination)
    elif transfer_method == "GWS_DELETE":
        GWS_Delete(ctx, disc, location)
    elif transfer_method == "GWS_MOVE":
        GWS_Move(ctx, disc, location, destination)

    else:
        print("Select one of JDMA, GWS_MOVE or GWS_DELETE")


def get_file_json(filemanifest):
    """
    Loads a json file and seperates it into 4 lists

    -argument filemanifest: A json file containing the list of files (format {disc:[list of files],tape:[list of files],unavailable:[list of files],other:[list of files])
    """
    print(filemanifest)
    with open(filemanifest) as f:
        jsonfiles = json.load(f)
    disc,tape,unavailable,other = jsonfiles["disc"][:-1],jsonfiles["tape"][:-1],jsonfiles["unavailable"][:-1],jsonfiles["other"][:-1]
    return disc,tape,unavailable,other

def JDMA_Transfer(ctx, filelist, location, destination):
    """
    Copy list of files from source to destination

    -argument collection: Collection of files which are to be moved. Collection must exist at source.
    -argument destination: Destination location for collection
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
    Move a list of files from one are on a gws to a new one

    -argument filelist: List of files which are to be moved. Files must exist at source.
    -argument location: The remote location of the GWS as stored on CFStore
    -argument destination: Destination location for collection
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
    Delete a list of files from a groupworkspace

    -argument filelist: List of files which are to be moved. Files must exist at source.
    -argument location: The remote location of the GWS as stored on CFStore
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


    totalfilesize = 0
    for file in filelist:
        totalfilesize += file[2]
        print(file)

    answer = input(
        f"This will open up {totalfilesize} bytes worth of space on {location}. y/n"
    )
    if answer.lower() == "y":
        for file in filelist:
            print(file[1])
            x.ssh.delete(file[1])
    elif answer.lower() == "n":
        print("No actions")
    else:
        print("Please enter y or n.")

def update_cfa(filelist):
    pass

if __name__ == "__main__":
    safe_cli()
