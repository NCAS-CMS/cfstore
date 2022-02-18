
from cfstore.plugins.et_main import et_main
from cfstore.plugins.posix import RemotePosix, Posix
from cfstore.config import CFSconfig
from cfstore import interface
from pathlib import Path
from datetime import datetime
import click


class InputError(Exception):
    def __init__(self, message, help):
        self.help = '***\n'+help+'\n***'
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
        click.echo('FileNotFoundError: ' + str(e))
    except InputError as e:
        click.echo(e)
        click.echo(e.help)


@click.group()
@click.pass_context
@click.argument('fstype')
def cli(ctx, fstype):
    """
    Provides the overall group context for command line arguments by
    defining the <fstype> (e.g. et,remote_posix etc) of the remote
    resource
    """
    ctx.ensure_object(dict)
    ctx.obj['fstype'] = fstype


@cli.command()
@click.pass_context
@click.argument('arg1', nargs=1)
@click.argument('argm', nargs=-1)
@click.option('--description', default=None, help='(Optional) File in which a description for this collection can be found')
def add(ctx, description, arg1, argm):
    """

    Add collection to the cfdb.

    Supports three location types: et, rp, and p (elastic tape, remote posix, posix).

    Usage:

    To add an elastic tape group workspace::

        cfin et add gwsname

    (no collection or description names are required because this will load a collection
    for each elastic tape batch, and you will need to add descriptions for each batch later.)

    To add a directory_path at remote posix location with a particular collection_name::

        cfin rp add location directory_path_at_location collection_name --description=filename
        cfin rp add location directory_path_at_location collection_name

    If the description_file filename is provided, the file is opened and the contents
    are used for the collection description, otherwise the application opens
    your standard editor to use for description content.

    """
    state = CFSconfig()
    target = ctx.obj['fstype']

    if target == 'et':
        et_main(state.db, 'init', arg1)
    else:

        if description:
            path = Path(description)
            assert path.exists()
            with open(path, 'r') as f:
                description = f.read()
        else:
            #if len(argm) != 2:
            #    raise InputError('InputError: Missing arguments', add.__doc__)
            today = datetime.now().strftime("%H:%M:%S %d/%m/%Y")
            intro = f'\n#### Collection {argm[1]}\n\n(loaded from {argm[0]}, location {arg1},  at {today})\n\n' + \
                    '[Enter description (text or markdown) (maybe using the above and deleting this line)]\n'
            description = click.edit(intro)

        if target == 'rp':
            location = arg1
            path, collection = argm
            x = RemotePosix(state.db, location)
            host, user = state.get_location(location)['host'], state.get_location(location)['user']
            x.configure(host, user)
            x.add_collection(path, collection, description)

        elif target == 'local' or target == 'p':
            location = arg1
            path, collection = argm
            x = Posix(state.db, location)
            x.add_collection(path, collection, description)
        else:
            raise ValueError(f'Unexpected location type {target}')

    state.save()


@cli.command()
@click.pass_context
@click.argument('location')
@click.argument('host')
@click.argument('user')
def setup(ctx, location, host, user):
    """
    Add a new posix location
    """

    state = CFSconfig()
    target = ctx.obj['fstype']

    if target == 'rp':
        # check we don't already have one in config or database (we can worry about mismatches later)
        if location in state.interfaces:
            raise ValueError(f'Location {location} already exists in config file')
        state.db.create_location(location)
        state.add_location(target, location, user=user, host=host)

    elif target == 'local' or target == 'p':
        # check we don't already have one in config or database (we can worry about mismatches later)
        if location in state.interfaces:
            raise ValueError(f'Location {location} already exists in config file')
        state.db.create_location(location)
        state.add_location(target, location, user=user, host=host)
    else:
        raise ValueError(f'Unexpected location type {target}')

    state.save()


if __name__ == "__main__":
    safe_cli()
