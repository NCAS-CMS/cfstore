#!/usr/bin/env python3

import json
import os
import sys
import time

import cf
import click
import netCDF4
from netCDF4 import Dataset
import numpy as np
from rich.console import Console
from rich.markdown import Markdown

from cfstore.config import CFSconfig
from cfstore.plugins.posix import Posix, RemotePosix

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


def _set_context(ctx, collection):
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
    # coming in from context before particular option (e.g. ls)
    if ctx.obj["collection"]:
        doset(ctx.obj["collection"])
        config_state["last_collection"] = ctx.obj["collection"]
    # now override default with arguments to option (e.g. ls)
    if collection:
        doset(collection)

    return config_state, config_state.db


def _print(lines, prop=None):
    if lines:
        for line in lines:
            if prop:
                print(getattr(line, prop))
            else:
                print(line)


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
@click.option("--collection", default=None, help="Current collection (make default)")
@click.pass_context
def cli(ctx, collection):
    """
    Provides the overall group context for command line arguments
    """
    ctx.ensure_object(dict)
    ctx.obj["collection"] = collection


@cli.command()
@click.pass_context
@click.argument("file")
def updateRemoteFile(ctx, file):
    """
    Looks at a single file and updates the appropriate database items
    """
    pass

@cli.command()
@click.pass_context
def updateRemoteCFAfile(ctx, file):
    pass

@cli.command()
@click.pass_context
def updateRemoteDirectory(ctx, file):
    state = CFSconfig()
    view_state, db = _set_context(ctx, "all")
    fileStore = db.getFileStore()
    # SSH
    # Setup Remote Posix as normal
    x = RemotePosix(state.db, fileStore.location)
    host, user = (
        state.get_location(fileStore.location)["host"],
        state.get_location(fileStore.location)["user"],
    )

    for location in db.retrieve_locations:
        files = db.retrieve_files_in_location(location)
        
        locPosix = RemotePosix(state.db, location)
        remote_files = locPosix.ssh.check_and_update_directory(files)

@cli.command()
@click.pass_context
def updateCollection(ctx, collection):
    state = CFSconfig()
    view_state, db = _set_context(ctx, "all")
    fileStore = db.getFileStore()
    # SSH
    # Setup Remote Posix as normal
    x = RemotePosix(state.db, fileStore.location)
    host, user = (
        state.get_location(fileStore.location)["host"],
        state.get_location(fileStore.location)["user"],
    )

    for location in db.retrieve_locations:
        files = db.retrieve_files_in_location(location)
        
        locPosix = RemotePosix(state.db, location)
        remote_files = locPosix.ssh.check_and_update_files(files)

@cli.command()
@click.pass_context
def updateAll(ctx):
    state = CFSconfig()
    view_state, db = _set_context(ctx, "all")
    fileStore = db.getFileStore()
    # SSH
    # Setup Remote Posix as normal
    x = RemotePosix(state.db, fileStore.location)
    host, user = (
        state.get_location(fileStore.location)["host"],
        state.get_location(fileStore.location)["user"],
    )

    for location in db.retrieve_locations:
        files = db.retrieve_files_in_location(location)
        
        locPosix = RemotePosix(state.db, location)
        remote_files = locPosix.ssh.check_and_update_files(files)

@cli.command()
@click.pass_context
def locateCFAFileStore(ctx, file):
    view_state, db = _set_context(ctx, "all")
    fileStore = db.getFileStore()
    print(fileStore.location,"|",fileStore.path)

@cli.command()
@click.pass_context
@click.option("--copy", default=False, help="When True will not delete current CFA File Store and will copy it")
@click.option("--updatedblocation",default=True, help="When True will update the database to point towards the destination location")
@click.option("--tempfilelocation", default=None, help="When copying the file needs to be briefly uploaded locally then pushed. It's inefficient")
def moveCFAFileStore(ctx, copy, updatedblocation, tempfilelocation):
    state = CFSconfig()
    view_state, db = _set_context(ctx, "all")
    fileStore = db.getFileStore()
    # SSH
    # Setup Remote Posix as normal
    x = RemotePosix(state.db, fileStore.location)

    fileStore = db.retrieve_CFA_directory()
    fileStore.CFA = False
    newFileStore = db.make_directory(updatedblocation, fileStore.location, True)
    if copy:
        x.ssh.copy_file()
    else:
        x.ssh.move_file(fileStore.Path,updatedblocation)

@cli.command()
@click.pass_context
def backPropagate(ctx, cfa, jsonFile):
    growncfa = Dataset(
        cfa,
        "a",
        format="NETCDF4",
    )


    def back_prop(a, i, s):
        if i == 0:
            varval[i] = varval[i].replace("${unavailable}", s)
        elif "${unavailable}" in a[i]:
            varval[i] = varval[i].replace("${unavailable}", s)
            back_prop(a, i - 1, s)

    with open(jsonFile) as f:
        tapefiles = json.load(f)

    k = 1
    for varkey, varval in growncfa.variables.items():
        if varkey.startswith("cfa_file"):
            varshape = varval[..., 0].shape
            varval = varval[..., 0].flatten().tolist()
            for v in range(len(varval) - 1):
                k += 1
                val = varval[v]
                if val in tapefiles:
                    back_prop(varval, v, tapefiles[val])
            varval = np.reshape(varval, varshape)


@cli.command()
@click.pass_context
@click.option("--collection", default=None, help="Current collection (make default)")
@click.argument("directory")
@click.argument("location")
@click.argument("cfadirectory")
def update(ctx, collection, directory, location, cfadirectory):

    growncfa = Dataset(
        cfadirectory,
        "a",
        format="NETCDF4",
    )
    state, db = _set_context(ctx, collection)
    x = RemotePosix(state.db, location)
    host, user = (
        state.get_location(location)["host"],
        state.get_location(location)["user"],
    )
    x.configure(host, user)

    tapefiles = x.ssh.get_files_and_sizes(directory, subcollections=True)
    tapefiles.append(["/gws/nopw/j04/canari/copy_streams/cv827_1_mon__grid_T_195001-195001.nc", 493350999])
    for tf, size in tapefiles:
        name = os.path.basename(tf)
        path = os.path.abspath(tf)
        print(name, size)
        db_files = db.retrieve_files_by_name(name)
        print(db_files)
        if db_files:
            for df in db_files:
                print(df.path)
                df.name = df.name.replace("unavailable","disc")
                df.name = df.name.replace("tape","disc")
                df.path = path
                df.size = size
                df.save()

    tapefilenames = [row[0] for row in tapefiles]

    k = 1
    for varkey, varval in growncfa.variables.items():
        if varkey.startswith("cfa_file"):
            varshape = varval[..., 0].shape
            varval = varval[..., 0].flatten().tolist()
            for v in range(len(varval) - 1):
                k += 1
                val = varval[v]
                if val in tapefilenames:
                    varval[v] = varval[v].replace("${unavailable}", "${disc}")
            varval = np.reshape(varval, varshape)

        