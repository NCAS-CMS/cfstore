from interface import CollectionDB
import os, json, sys
import click
from urllib.parse import urlparse

STATE_FILE = '.cftape'

def _save(view_state):
    """ Save view state if valid"""
    if not view_state['db']:
        raise ValueError('Save option requires default database value')
    with open(STATE_FILE,'w') as f:
        json.dump(view_state, f)


def _load():
    """ Load existing view state"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE,'r') as f:
            view_state = json.load(f)
        actual_file = _naked(view_state['db'])
        if os.path.exists(actual_file):
            return view_state
        else:
            raise ValueError(f"Configuration file {STATE_FILE} does not have valid database ({view_state['db']}")
    else:
        raise FileNotFoundError('No existing configuration file found')

def _set_context(ctx, collection):
    """
    Set the view_state context
    """
    try:
        view_state = _load()
        for k in ['db', 'collection']:
            if ctx.obj[k]:
                view_state[k] = ctx.obj[k]
    except ValueError:
        view_state = {'db': ctx.obj['db'], 'collection': None}
        if ctx.obj['collection']:
            view_state['collection'] = ctx.obj['collection']
    except FileNotFoundError:
        view_state = {k: ctx.obj[k] for k in ['db', 'collection']}

    # now override default with arguments to ls
    if collection:
        view_state['collection'] = collection

    db = CollectionDB()
    db.init(view_state['db'])
    if view_state['collection'] == 'all':
        view_state['collection'] = None

    return view_state, db



def _naked(db_name, display=False):
    """ Strip protocol gubbins from database connection string"""
    f = urlparse(db_name).path[1:]
    if display:
        print(f)
    return f

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
@click.option('--db', default=None, help='New default database (e.g sqlite///hiresgw.db) ')
@click.option('--collection', default=None, help='Current collection (make default)')
@click.pass_context
def cli(ctx, db, collection):
    """
    Provides the overall group context for command line arguments
    """
    ctx.ensure_object(dict)
    ctx.obj['db'] = db
    ctx.obj['collection'] = collection


@cli.command()
@click.pass_context
def save(ctx):
    """
    Save current db choice and last used collection (which becomes default).
    """
    view_state = {k: ctx.obj[k] for k in ['db', 'collection']}
    _save(view_state)


@cli.command()
@click.pass_context
@click.option('--collection', default=None, help='Required collection (use and make default)')
def ls(ctx, collection):
    """ 
    List collections (collections=None),
    or list files in a specific collection
    (which might be the last used one).
    """
    view_state, db = _set_context(ctx, collection)

    if view_state['collection']:
        files = db.get_files_in_collection(view_state['collection'])
        for f in files:
            print(f)
    else:
        collections = db.get_collections()
        _naked(view_state['db'], display=True)
        for c in collections:
            print(c)

    _save(view_state)


@cli.command()
@click.pass_context
@click.option('--collection', default=None, help='Look in collection (use and make default)')
@click.argument('match')
def find(ctx, match, collection):
    """
    Find files in collection (or entire database if --collection=all), which include MATCH
    anywhere in their path and filename.
    """
    view_state, db = _set_context(ctx, collection)
    collection = view_state['collection']
    if collection:
        files = db.get_files_in_collection(collection, match=match)
        for f in files:
            print(f)
    else:
        files = db.retrieve_files_which_match(match)
        for f in files:
            print(f)

    _save(view_state)


@cli.command()
@click.pass_context
@click.argument('collection')
@click.option('--description_file', default=None, help='(Optional) File in which a description for this collection can be found')
def organise(ctx, collection, description_file):
    """
    Take a list of files read from std input and move them into a collection with name COLLECTION.
    If COLLECTION doesn't exist, create it, otherwise add files into it.
    """
    view_state, db = _set_context(ctx, collection)

    if os.isatty(0):
        click.echo(ctx.get_help())
        return

    if description_file:
        with open(description_file, 'r') as f:
            description = f.readlines()
    else:
        description = None

    files = [f.strip() for f in sys.stdin.readlines()]
    db.organise(collection, files, description=description)

    _save(view_state)



if __name__ == "__main__":
    safe_cli()