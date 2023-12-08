import os
import stat
import time
from datetime import datetime
from pathlib import Path

import click

from cfstore.config import CFSconfig
from cfstore.plugins.et_main import et_main
from cfstore.plugins.posix import Posix, RemotePosix


class InputError(Exception):
    def __init__(self, message, help):
        self.help = "***\n" + help + "\n***"
        super().__init__(message)


def safe_cli():
    """
    Traps all ValueErrors which bubble up from the things click calls, and
    removes the stack trace ...
    """
    try:
        cli()
    except ValueError as e:
        click.echo(e)
    except FileNotFoundError as e:
        click.echo("FileNotFoundError: " + str(e))
    except InputError as e:
        click.echo(e)
        click.echo(e.help)


@click.group()
@click.pass_context
@click.argument("fstype")
def cli(ctx, fstype):
    """
    Provides the overall group context for command line arguments by
    defining the <fstype> (e.g. et,remote_posix etc) of the remote
    resource
    """
    ctx.ensure_object(dict)
    ctx.obj["fstype"] = fstype


@cli.command()
@click.pass_context
@click.argument("arg1", nargs=1)
@click.argument("argm", nargs=-1)
@click.option(
    "--description",
    default=None,
    help="(Optional) File in which a description for this collection can be found",
)
@click.option(
    "--regexselect",
    default=None,
    help="(Optional) Selects only a portion of files. Uses Regex.",
)
@click.option(
    "--subcollections",
    default=False,
    help="(Optional) When true, searches subcollections.",
)
def add(ctx, description, regexselect, subcollections, arg1, argm):
    """

    Add collection to the cfdb.

    Supports three location types: et, rp, and p (elastic tape, remote posix, posix).

    Usage:

    To add an elastic tape group workspace running inside STFC::

        cfin et add gwsname

    If you are outside of STFC you will need to run

        cfin et add gwsname ssh_host ssh_user

    where ssh_host and ssh_user should probably appear in your ssh config file

    No collection or description names are required because this will load a collection
    for each elastic tape batch, and you will need to add descriptions for each batch later.

    To add a directory_path at remote posix location with a particular collection_name::

        cfin rp add location directory_path_at_location collection_name --description=filename
        cfin rp add location directory_path_at_location collection_name

    If the description_file filename is provided, the file is opened and the contents
    are used for the collection description, otherwise the application opens
    your standard editor to use for description content.

    """
    state = CFSconfig()
    target = ctx.obj["fstype"]

    if target == "et":
        if len(argm) == 2:
            et_main(state.db, "init", arg1, argm[0], argm[1])
        elif len(argm) == 0:
            et_main(state.db, "init", arg1)
        else:
            raise InputError("InputError: Missing arguments", add.__doc__)
    else:
        if description:
            path = Path(description)
            assert path.exists()
            with open(path, "r") as f:
                description = f.read()
        else:
            # if len(argm) != 2:
            #    raise InputError('InputError: Missing arguments', add.__doc__)
            today = datetime.now().strftime("%H:%M:%S %d/%m/%Y")
            if target == "local" or target == "p":
                intro = (
                    f"\n#### Adding collection {argm[0]}\n\n(loaded locally,  at {today})\n\n"
                    + "[Enter description (text or markdown) (maybe using the above and deleting this line)]\n"
                )
            else:
                intro = (
                    f"\n#### Adding collection {argm[1]}\n\n(loaded from {argm[0]}, location {target},  at {today})\n\n"
                    + "[Enter description (text or markdown) (maybe using the above and deleting this line)]\n"
                )

            description = click.edit(intro)

        if target == "rp":
            location = arg1
            path, collection = argm
            x = RemotePosix(state.db, location)
            host, user = (
                state.get_location(location)["host"],
                state.get_location(location)["user"],
            )
            x.configure(host, user)
            x.add_collection(
                path,
                collection,
                description,
                subcollections=subcollections,
                regex=regexselect,
            )

        elif target == "local" or target == "p":
            location = "local"
            collection = arg1
            path = argm[0]
            x = Posix(state.db, collection)
            x.add_collection(
                path,
                collection,
                description,
                subcollections=subcollections,
                regex=regexselect,
            )
        else:
            raise ValueError(f"Unexpected location type {target}")
    state.save()


"""
#This with the right arguments can run scripts on Jasmin
#Most of the work is done in the arguments though
#So needs fiddling
@cli.command()
@click.pass_context
@click.argument('arg1', nargs=1)
@click.argument('argm', nargs=-1)
def getBMetadata(ctx, arg1, argm):
    state = CFSconfig()

    location = arg1
    remotepath, collection, scriptname = argm

    #Setup Remote Posix as normal
    x = RemotePosix(state.db, location)
    host, user = state.get_location(location)['host'], state.get_location(location)['user']
    x.configure(host, user)


    #Add collection, I guess?
#    x.add_collection(path, collection)

    #Set up something that runs on Jasmin
    # Connect to remote host
    x.getBMetadata(remotepath,collection, scriptname, False, False,None)

    # Setup sftp connection and transmit this script


    # Run the transmitted script remotely without args and show its output.
    # SSHClient.exec_command() returns the tuple (stdin,stdout,stderr)

    #Call add_variables_from_file for each file
    state.save()
"""


# This with the right arguments can run scripts on Jasmin
# Most of the work is done in the arguments though
# So needs fiddling
@cli.command()
@click.pass_context
@click.argument("arg1", nargs=1)
@click.argument("argm", nargs=-1)
@click.option(
    "--outputfilename", default="tempfile.cfa", help="the name of the output file"
)
def PopulateFromWorkspaceDirectoryByDirectory(
    ctx,
    arg1,
    argm,
    outputfilename,
):
    """
    Runs a remote script to aggregate metadata and store it in the database
    Format is:
        cfin rp getbmetadataclean sshlocation wheretopushscript remotedirectory collection
    Other scripts from /scripts/ folder can be used by setting --aggscript=scriptname (only name is needed, .py is optional)
    Other locations for scripts can be used by setting --scriptlocation=afolder
    """
    state = CFSconfig()
    print(arg1)
    print(argm)
    location = arg1
    pushdirectory, metadatadirectory, collection = argm
    scriptlocation = "~/cfstore/scripts/"

    aggscriptpath = "~/cfstore/scripts/aggregatebmetadata.py"

    # SSH
    # Setup Remote Posix as normal
    x = RemotePosix(state.db, location)
    host, user = (
        state.get_location(location)["host"],
        state.get_location(location)["user"],
    )
    x.configure(host, user)

    for root, dirs in os.walk(collection):
        # Add settings to script
        x.ssh.configureScript(aggscriptpath, (metadatadirectory, pushdirectory))
        aggscriptpath = scriptlocation + "aggscript.py"
        path = root + dirs
        aggscriptname = "aggscript.py"
        col = collection + path
        description = "Path " + path

        x.add_collection(
            path,
            col,
            description,
            subcollections=False,
            regex=None,
        )

        # Push Script(s)
        # x.ssh.pushScript(remotepath,col, scriptname)
        x.ssh.pushScript(pushdirectory, col, aggscriptpath)

        # Activate Remote Environments
        x.ssh.configureRemoteEnvironment()

        # Execute script to generate Aggregation File
        x.ssh.executeScript(pushdirectory, col, aggscriptname)

        # Retrieve JSON file
        x.ssh.get(
            pushdirectory + "/tempfile.cfa",
            "cfstore/json/" + outputfilename,
            delete=True,
        )

        # Clean-up remote files (At present clean-up means remove them)
        # This is actually an ongoing step done at the end of each remote transfer with excepts. It's more robust.

        # Update database with JSON
        x.aggregation_files_to_collection("cfstore/json/" + outputfilename, col)
        state.save()


# This with the right arguments can run scripts on Jasmin
# Most of the work is done in the arguments though
# So needs fiddling
@cli.command()
@click.pass_context
@click.argument("arg1", nargs=1)
@click.argument("argm", nargs=-1)
@click.option(
    "--subdirectories",
    default="False",
    help="If True searches through all subdirectories",
)
@click.option(
    "--outputfilename", default="tempfile.cfa", help="the name of the output file"
)
def PopulateFromWorkspaceDirectory(
    ctx,
    arg1,
    argm,
    subdirectories,
    outputfilename,
):
    """
    Runs a remote script to aggregate metadata and store it in the database
    Format is:
        cfin rp getbmetadataclean sshlocation wheretopushscript remotedirectory collection
    Other scripts from /scripts/ folder can be used by setting --aggscript=scriptname (only name is needed, .py is optional)
    Other locations for scripts can be used by setting --scriptlocation=afolder
    """
    state = CFSconfig()
    print(arg1)
    for m in argm:
        print(m)
    location = arg1
    pushdirectory, metadatadirectory, collection = argm
    scriptlocation = "cfstore/scripts/"

    aggscriptpath = "cfstore/scripts/aggregatebmetadata.py"

    print("Variables set")

    print("Setting up remote posix")
    # SSH
    # Setup Remote Posix as normal
    x = RemotePosix(state.db, location)
    host, user = (
        state.get_location(location)["host"],
        state.get_location(location)["user"],
    )
    x.configure(host, user)
    print("Posix set up")

    print("Configuring Aggregation Script")
    x.ssh.configureScript(aggscriptpath, (metadatadirectory, pushdirectory))
    print("Success!")

    print("Pushing script to", pushdirectory)
    # Push Script(s)
    # x.ssh.pushScript(remotepath,col, scriptname)
    x.ssh.pushScript(pushdirectory, collection, scriptlocation + "aggscript.py")
    print("Success!")

    print("Adding top level collection")
    col = x.add_collection(
        metadatadirectory,
        collection,
        "Top level directory",
        subcollections=False,
        regex=None,
    )
    print("Collection added")

    print("Configuring Remote Environment")
    # Activate Remote Environments
    x.ssh.configureRemoteEnvironment()
    print("Success!")

    print("Executing script")
    scriptstarttime = time.time()
    # Execute script to generate Aggregation File
    x.ssh.executeScript(pushdirectory, collection, "aggscript.py")
    print("Success!")
    scriptendtime = time.time()
    print(f"Aggregation script took {scriptendtime-scriptstarttime:.2f} seconds")
    print("Getting CFA")
    # Retrieve CFA file
    x.ssh.get(
        pushdirectory + "/tempfile.cfa", "cfstore/json/" + outputfilename, delete=False
    )

    # Clean-up remote files (At present clean-up means remove them)
    # This is actually an ongoing step done at the end of each remote transfer with excepts. It's more robust.
    print("Success!")

    print("Parsing files")
    print("walking from ", metadatadirectory)
    scriptstarttime = time.time()
    for dirName in x.ssh._sftp.listdir(metadatadirectory):
        parsefiles(
            x,
            state,
            metadatadirectory,
            pushdirectory,
            metadatadirectory + "/" + dirName,
            collection,
            scriptlocation,
            subdirectories,
            outputfilename,
            [collection],
        )
    scriptendtime = time.time()
    print(f"Walking directory took {scriptstarttime-scriptendtime:.2f} seconds")
    scriptstarttime = time.time()

    print("Adding metadata from aggregation files tp collection")
    # Update database with JSON
    x.aggregation_files_to_collection("cfstore/json/" + outputfilename, collection)
    state.save()
    scriptendtime = time.time()
    print(
        f"Aggregation metadata gathering took {scriptstarttime-scriptendtime:.2f} seconds"
    )


def parsefiles(
    x,
    state,
    metadatadirectory,
    pushdirectory,
    path,
    collection,
    scriptlocation,
    subdirectories,
    outputfilename,
    higherlevelcollections,
):
    print("Configuring")
    colname = collection + "-" + path
    description = "Path " + path

    print("Making Collection called", path)
    col = x.add_collection(
        path,
        colname,
        description,
        subcollections=False,
        regex=None,
    )
    print("Success!")

    col = x.db.retrieve_collection(colname)
    print("Adding entrenched hierarchical system")
    for relation in higherlevelcollections:
        col2 = x.db.retrieve_collection(relation)
        print(col.name, "is above", col2.name)
        x.db.add_relationships(col.name, col2.name, "above", "below")
    print("Success!")

    higherlevelcollections = higherlevelcollections.append(colname)
    for dirName in x.ssh._sftp.listdir(path):
        fileattr = x.ssh._sftp.lstat(path + "/" + dirName)
        if not stat.S_ISREG(fileattr.st_mode):
            try:
                parsefiles(
                    x,
                    state,
                    metadatadirectory,
                    pushdirectory,
                    path + "/" + dirName,
                    collection,
                    scriptlocation,
                    subdirectories,
                    outputfilename,
                    higherlevelcollections,
                )
            except:
                pass
    print("Success!")
    print("All done!")


@cli.command()
@click.pass_context
@click.argument("location")
@click.argument("host")
@click.argument("user")
@click.option(
    "--overwrite",
    default=False,
    help="(Optional) If true, will overwrites current location",
)
def setup(ctx, location, host, user, overwrite):
    """
    Add a new posix location
    """
    if overwrite == "True":
        overwrite = True
    else:
        overwrite = False

    state = CFSconfig()
    target = ctx.obj["fstype"]
    print(f"Atempting to setup:\n host:{host} \n location:{location}")
    if target == "rp":
        # check we don't already have one in config or database (we can worry about mismatches later)
        if location in state.interfaces and not overwrite:
            raise ValueError(f"Location {location} already exists in config file")

        state.db.create_location(location, overwrite=overwrite)
        state.add_location(target, location, user=user, host=host)

    elif target == "local" or target == "p":
        # check we don't already have one in config or database (we can worry about mismatches later)
        if location in state.interfaces and not overwrite:
            raise ValueError(f"Location {location} already exists in config file")
        state.db.create_location(location, overwrite=overwrite)
        state.add_location(target, location, user=user, host=host)
    else:
        raise ValueError(f"Unexpected location type {target}")

    state.save()


if __name__ == "__main__":
    safe_cli()


# This with the right arguments can run scripts on Jasmin
# Most of the work is done in the arguments though
# So needs fiddling
@cli.command()
@click.pass_context
@click.argument("arg1", nargs=1)
@click.argument("argm", nargs=-1)
@click.option(
    "--aggscriptname",
    default="aggregatebmetadata",
    help="(Optional) Lets you run a different script to the default",
)
@click.option(
    "--remotetempfilelocation",
    default="",
    help="(Optional) Sets where to put the remote temp file",
)
@click.option(
    "--scriptlocation", default="", help="(Optional) Sets where to find the script"
)
@click.option(
    "--outputfilename", default="tempfile.cfa", help="the name of the output file"
)
@click.option(
    "--outputfilelocation",
    default="~/cfstore/scripts/newfilebmetadata.json",
    help="Where to put the output file",
)
def PopulateFromWorkspace(
    ctx,
    arg1,
    argm,
    aggscriptname,
    remotetempfilelocation,
    scriptlocation,
    outputfilename,
    outputfilelocation,
):
    """
    Runs a remote script to aggregate metadata and store it in the database
    Format is:
        cfin rp getbmetadataclean sshlocation wheretopushscript remotedirectory collection
    Other scripts from /scripts/ folder can be used by setting --aggscript=scriptname (only name is needed, .py is optional)
    Other locations for scripts can be used by setting --scriptlocation=afolder
    """
    state = CFSconfig()
    print(arg1)
    print(argm)
    location = arg1
    pushdirectory, metadatadirectory, collection = argm
    if not aggscriptname.endswith(".py"):
        aggscriptname = aggscriptname + ".py"
    aggscriptpath = scriptlocation + aggscriptname

    # SSH
    # Setup Remote Posix as normal
    x = RemotePosix(state.db, location)
    host, user = (
        state.get_location(location)["host"],
        state.get_location(location)["user"],
    )
    x.configure(host, user)

    # Add settings to script
    x.ssh.configureScript(aggscriptpath, (metadatadirectory, pushdirectory))
    aggscriptpath = scriptlocation + "aggscript.py"
    aggscriptname = "aggscript.py"

    # Push Script(s)
    # x.ssh.pushScript(remotepath,collection, scriptname)
    x.ssh.pushScript(pushdirectory, collection, aggscriptpath)

    x.ssh.configureRemoteEnvironment()

    # Generate Aggregation File
    x.ssh.aggregateFiles(pushdirectory)

    # Generate JSON from Aggregation File
    x.ssh.executeScript(pushdirectory, collection, aggscriptname)

    # Retrieve JSON file
    x.ssh.get(
        pushdirectory + "/tempfile.cfa", "cfstore/json/" + outputfilename, delete=True
    )

    # Clean-up remote files (At present clean-up means remove them)
    # This is actually an ongoing step done at the end of each remote transfer with excepts. It's more robust.

    # Update database with JSON
    x.aggregation_files_to_collection("cfstore/json/" + outputfilename, collection)
    state.save()
