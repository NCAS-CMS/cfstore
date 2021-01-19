#!/usr/bin/env python3

from cfstore.config import CFSconfig
import os, sys
import click


def _load():
    """ Load existing view state"""
    return CFSconfig()


def _set_context(ctx, collection):
    """
    Set the config_state context
    """
    config_state = _load()
    # coming in from context before particular option (e.g. ls)
    if ctx.obj['collection']:
        config_state['last_collection'] = ctx.obj['collection']
    # now override default with arguments to option (e.g. ls)
    if collection:
        if collection == 'all':
            config_state['last_collection'] = ''
        else:
            config_state['last_collection'] = collection

    return config_state, config_state.db


def _print(lines, prop=None):
    for line in lines:
        if prop:
            print(getattr(line,prop))
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
@click.option('--collection', default=None, help='Current collection (make default)')
@click.pass_context
def cli(ctx, collection):
    """
    Provides the overall group context for command line arguments
    """
    ctx.ensure_object(dict)
    ctx.obj['collection'] = collection


@cli.command()
@click.pass_context
def save(ctx):
    """
    Save current db choice and last used collection (which becomes default).
    """
    view_state = {k: ctx.obj[k] for k in ['db', 'collection']}
    view_state.save()


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

    if view_state.collection:
        files = db.retrieve_files_in_collection(view_state.collection)
        for f in files:
            print(f)
    else:
        collections = db.retrieve_collections()
        print(view_state.name)
        for c in collections:
            print(c)

    view_state.save()


@cli.command()
@click.pass_context
@click.option('--collection', default=None, help='Look in collection (use and make default)')
@click.argument('match')
def findf(ctx, match, collection):
    """
    Find files in collection (or entire database if --collection=all), which include MATCH
    anywhere in their path and filename.
    """
    view_state, db = _set_context(ctx, collection)
    collection = view_state.collection
    if collection:
        files = db.retrieve_files_in_collection(collection, match=match)
        for f in files:
            print(f)
    else:
        files = db.retrieve_files_which_match(match)
        for f in files:
            print(f)

    view_state.save()


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

    view_state.save()

@cli.command()
@click.pass_context
@click.argument('collection')
@click.argument('tagname')
def tag(ctx, collection, tagname):
    """
    Tag a COLLECTION with TAGNAME
    (and save collection as current default collection)
    """
    view_state, db = _set_context(ctx, collection)
    db.tag_collection(view_state.collection, tagname)
    view_state.save()

@cli.command()
@click.pass_context
@click.option('--match', default=None, help='return collections which with MATCH somewhere in the name or description')
@click.option('--tagname', default=None, help='return collections which have TAGNAME associated with them')
@click.option('--facet', nargs=2, type=str, help='return collection where facet  key=value --facet key value')
def findc(ctx, match, tagname, facet):
    """
    Find all collections which either have MATCH in their name, or
    are tagged with TAGNAME
    """
    view_state, db = _set_context(ctx, 'all')
    if facet == ():
        facet = None
    if [match, tagname, facet].count(None) < 2:
        click.echo(ctx.get_help())
    elif match:
        _print(db.retrieve_collections(contains=match), 'name')
    elif tagname:
        _print(db.retrieve_collections(tagname=tagname), 'name')
    elif facet:
        _print(db.retrieve_collections(facet=facet), 'name')

@cli.command()
@click.pass_context
@click.argument('key')
@click.argument('value')
@click.option('--collection', default=None, help='Collection to which to apply/remove facet')
@click.option('-r','--remove', is_flag=True, help="If present, remove property from collection")
def facet(ctx, key, value, collection, remove):
    """
    Add key,value to the properties (facets) of a collection
    (or remove if -r/--remove is present)
    As usual, do this with current default collection or be specific with
    --collection=collection
    """
    view_state, db = _set_context(ctx, collection)
    if not view_state.collection:
        raise ValueError('Cannot use facet without defining a collection')
    if remove:
        raise NotImplementedError

    c = db.retrieve_collection(view_state.collection)
    c[key] = value
    db.session.commit()
    view_state.save()


@cli.command()
@click.pass_context
@click.argument('col1')
@click.argument('link')
@click.argument('col2')
def linkto(ctx, col1, link, col2):
    """
    Add a one way connection between col1 and col2.
    e.g. col1 parent_of col2, would be
    linkto (col1, 'parent_of, col2)
    Makes no reciprocal links. This link can only
    be discovered from col1.
    """
    view_state, db = _set_context(ctx, col1)
    db.add_relationships(col1, col2, link, None)

@cli.command()
@click.pass_context
@click.argument('col1')
@click.argument('link')
@click.argument('col2')
def linkbetween(ctx, col1, link, col2):
    """
    Add a symmetric link between col1 and col2.
    e.g. linkbetween (col1, 'brother_of', col2) would
    result in being able to find all collections
    which are "brother_of" col2 (which would be col1), and
    vice versa.
    """
    view_state, db = _set_context(ctx, col1)
    db.add_relationship(col1, col2, link)


@cli.command()
@click.pass_context
@click.argument('link')
@click.option('--collection', default=None, help='Collection from which to find relationships')
def findr(ctx, link, collection):
    """
    Find all collections related to <collection> via the <link> relationship.
    e.g. findr parent_of
    would find all the related object collections for the subject/predicate/object
    relationship collection/parent_of/*
    """
    view_state, db = _set_context(ctx, collection)
    collection = view_state.collection
    _print(db.retrieve_related(collection, link), 'name')
    view_state.save()


if __name__ == "__main__":
    safe_cli()