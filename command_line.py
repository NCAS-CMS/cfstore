from interface import CollectionDB
import os, json
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
        actual_file = urlparse(view_state['db']).path[1:]
        print(actual_file)
        if os.path.exists(actual_file):
            return view_state
        else:
            raise ValueError(f"Configuration file {STATE_FILE} does not have valid database ({view_state['db']}")
    else:
        raise FileNotFoundError('No existing configuration file found')


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
@click.option('--db', default=None, help='New default database file')
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
    Save current db, overwriting existing state if desired
    """
    view_state = {k: ctx.obj[k] for k in ['db', 'collection']}
    _save(view_state)


@cli.command()
@click.pass_context
@click.option('--collection', default=None, help='Required collection(use and make default)')
def ls(ctx, collection):
    """ 
    List collections, or list files in collection
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
    print(view_state)
    db.init(view_state['db'])

    if view_state['collection']:
        files = db.get_files_in_collection(view_state['collection'])
        for f in files:
            print(f)
    else:
        collections = db.get_collections()
        for c in collections:
            print(c)

    _save(view_state)


if __name__ == "__main__":
    safe_cli()