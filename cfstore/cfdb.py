#!/usr/bin/env python3

import json
from unicodedata import name
from cfstore.config import CFSconfig
import os, sys
import click
from rich.console import Console
from rich.markdown import Markdown
import cf

STATE_FILE = '.cftape'


def _save(view_state):
    """ Save view state if valid"""
    if not view_state['db']:
        raise ValueError('Save option requires default database value')
    with open(STATE_FILE,'w') as f:
        json.dump(view_state, f)

def _load():
    """ Load existing view state"""
    return CFSconfig()


def _set_context(ctx, collection):
    """
    Set the config_state context
    Importantly, this sets the active collection
    """
    def doset(c):
        if c == 'all':
            config_state['last_collection'] = ''
        else:
            config_state['last_collection'] = c

    config_state = _load()
    # coming in from context before particular option (e.g. ls)
    if ctx.obj['collection']:
        doset(ctx.obj['collection'])
        config_state['last_collection'] = ctx.obj['collection']
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
@click.option('--collection', default=None, help='Required collection (use and make default)')
def setc(ctx,collection):
    """
    Set collection, or reset to default if --collection=all
    Usage: cfsdb setc --collection=<collection>
    """
    view_state, db = _set_context(ctx, collection)
    if collection!="all":
        try:
            db.retrieve_collection(collection)
        except ValueError as err:
            print(err, file=sys.stderr)
    print(f"Collection set to {collection}")
    view_state.save()

@cli.command()
@click.pass_context
@click.argument('aggfile')
@click.option('--collection', default=None, help='Where to send contents of aggregation file')
def aaftc(ctx,aggfile,collection):
    """
    (A)dd (A)ggregation (F)ile (T)o (C)ollection
    Takes in an aggregation file and a collection. Collection is not actually an optional input.
    The contents of the aggregation file are added to the collection.
    Usage: #FIXME
    """
    view_state, db = _set_context(ctx, collection)
    if collection!="all":
        try:
            c = db.retrieve_collection(collection)
        except ValueError as err:
            print(err, file=sys.stderr)
    aggfileobject = open(aggfile,'r')
    variables = cf.read(aggfile)
    for var in variables:
        print(var.get_filenames())
    db.add_variables_from_file(aggfile)

@cli.command()
@click.pass_context
@click.argument('key')
@click.argument('value')
@click.option('--verbosity', default=0, help='0 is just name, 2 is everything, 1 is id, name, size and domain')
def searchvariable(ctx,key,value,verbosity):
    """
    Search for collections with a variable
    Main keys are: long_name, standard_name, cfdm_size, cfdm_domain, cell_methods
    Other properties can also be searched
    Usage: cfsdb searchvariable <key> <value>
    """
    view_state, db = _set_context(ctx, "all")

    variables = db.retrieve_variable(key,value)
    for var in variables:
        #print(var.get_properties(verbosity))
        db.show_collections_with_variable(var)


@cli.command()
@click.pass_context
@click.option('--name_contains', default=None, help='Search by collection name')
@click.option('--description_contains', default=None, help='Search by text in description')
@click.option('--contains_file', default=None, help='Search for collections containing a file')
@click.option('--tagname', default=None, help='Search by tag')
@click.option('--facet', default=None, help='Search by facet')

def search_collections(ctx,name_contains, description_contains, contains_file, tagname, facet):
    """
    Search for collections with specific features
    The supported search properties are name_contains, description_contains, contains_file, tagname, facet
    """
    if not (name_contains or description_contains or contains_file or tagname or facet):
        print("You might want to put in some search options")
    view_state, db = _set_context(ctx, "all")
    collections = db.retrieve_collections(name_contains, description_contains, contains_file, tagname, facet)
    print("Your search of ")
    if name_contains:
        print(f"Name contains {name_contains}")
    if description_contains:
        print(f"Description contains {description_contains}")
    if contains_file:
        print(f"Collection contains file called {contains_file}")
    if tagname:
        print(f"Collection tagged as {tagname}")
    if facet:
        print(f"Collection has facet {facet}")
    if collections:
        print("Produces the following collections:")
        for col in collections:
            print("|",col.name)
    else:
        print("Found nothing!")

@cli.command()
@click.pass_context
@click.argument('key')
@click.argument('value')
@click.option('--verbosity', default=0, help='0 is just name, 2 is everything, 1 is id, name, size and domain')
def browsevariable(ctx,key,value,verbosity):
    """
    Iterative user input to build compound search
    Browse starts with initial key/value pair then iteratively take in additional key/value pairs gradually narrowing search
    Will not print collections without checking first - make sure the output is of a reasonable size
    """
    view_state, db = _set_context(ctx, "all")
    variables,query = db.retrieve_variable_query(key,value,[])
    loop = True
    while loop:
        print("There are ",len(variables),"results found.")
        print("Print them all or continue to browse\n")
        user_input = input("Input (p)rint or (b)rowse\n")
        print(user_input)
        if user_input == "p" or user_input == "print":
            user_input = input("Are you sure you want to print "+str(len(variables))+" items?\n")
            if user_input == "yes" or user_input == "y":
                for var in variables:
                    print(var.get_properties(verbosity))
                loop = False
            else:
                print("Nevermind, then\n")
        elif user_input == "b" or user_input == "browse":
            user_input = input("Input additional search in the format \"key,value\".\n")
            k,v=user_input.split(",")
            variables,query = db.retrieve_variable_query(k,v,query)


@cli.command()
@click.pass_context
@click.option('--collection', default=None, help='Required collection (use and make default)')
@click.option('--output', default="files",help="What information is printed (files, tags, facets, relationships, collections, variables or locations)")
def ls(ctx, collection, output):
    """ 
    List collections (collections=None),
    or list other objects in a specific collection
    (which might be the last used one).
    Usage: cfsdb ls --collection=<collection> --output= <files|tags|facets|relationships|collections|variables|locations>
    """
    view_state, db = _set_context(ctx, collection)
    output= output.lower()
    print(output)
    
    if view_state.collection:
        if output=="files":
            return_list = db.retrieve_files_in_collection(view_state.collection)
        
        if output=="tags":
            return_list = db.retrieve_collection(view_state.collection).tags
        
        if output=="facets":
            return_list = db.retrieve_collection(view_state.collection).properties
        
        if output=="relationships":
            return_list = db.retrieve_related(view_state.collection)

        if output=="collections":
            return_list = db.retrieve_collections()
            print(view_state.name)

        if output=="variables":
            return_list = db.retrieve_variable("all","")
            print(view_state.name)

        if output=="locations":
            state = _load()
            return_list = []
            loc_list = db.retrieve_locations()
            print(view_state.name)
            for c in loc_list:
                locationName = str(c)
                if locationName == collection:
                    return_list.append("Location details:")
                    return_list.append("Name:"+locationName)
                    return_list.append("Host:"+state.get_location(locationName)['host'])
                    return_list.append("User:"+state.get_location(locationName)['user'])
            if return_list==[]:
                if collection!=None:
                    print(f"Location {collection} not found, showing all locations")
                return_list = loc_list

        if output=="variables" or output=="var":
            try:
                for r in return_list:
                    print(r.get_properties(1))
            except:
                if output not in ["files","tags","facets","relationships","collections","locations"]:
                    print(f"Invalid output \"{output}\" selected - try files, tags, facets, relationships, collections or locations instead")
                else:
                    print("Return list cannot be printed")
        else:
            try:
                for r in return_list:
                    print(r)
            except:
                if output not in ["files","tags","facets","relationships","collections","locations"]:
                    print(f"Invalid output \"{output}\" selected - try files, tags, facets, relationships, collections or locations instead")
                else:
                    print("Return list cannot be printed")
    else:
        return_list = db.retrieve_collections()
        print(view_state.name)
        if output=="locations":
            return_list = db.retrieve_locations()
        try:
            for r in return_list:
                print(r)
        except:
            if output not in ["files","tags","facets","relationships","collections","locations"]:
                print(f"Invalid output \"{output}\" selected - try files, tags, facets, relationships, collections or locations instead")
            else:
                print("Return list failed to print")
    view_state.save()


@cli.command()
@click.pass_context
@click.option('--collection', default=None, help='Look in collection (use and make default)')
@click.argument('match')
def findf(ctx, match, collection):
    """
    Find files in collection (or entire database if --collection=all), which include MATCH
    anywhere in their path and filename.
    Usage: cfsdb findf <string to find> --collection=<collection>
    """
    view_state, db = _set_context(ctx, collection)
    collection = view_state.collection
    if collection:
        try:
            db.retrieve_collection(collection)
        except ValueError as err:
            print(err, file=sys.stderr)
        files = db.retrieve_files_in_collection(collection, match=match)
        if len(files)==0:
            print("No files found")
        for f in files:
            print(f)
    else:
        files = db.retrieve_files_which_match(match)
        for f in files:
            print(f)

    view_state.save()

@cli.command()
@click.pass_context
@click.option('--collection', default=None, help='Collection in which replicants are expected')
@click.argument('match', nargs=-1)
def findrx(ctx, collection, match):
    """

    Find all replicant files in a collection, optionally including MATCH
    anywhere in their path and filename.

    (The default collection must be set, or the --collection argument used.)
    (Depreciated, replaced by locate replicants)
    """
    view_state, db = _set_context(ctx, collection)
    collection = view_state.collection
    if match == ():
        match = None
    else:
        match = match[0]
    if collection:
        try:
            db.retrieve_collection(collection)
        except ValueError as err:
            print(err, file=sys.stderr)
        files = db.retrieve_files_in_collection(collection, replicants=True, match=match)
        for f in files:
            print(f)
    else:
        click.echo('Replicant file discovery requires a collection to be set')
        click.echo(ctx.get_help())

    view_state.save()

@cli.command()
@click.pass_context
@click.option('--match-full-path', default=False, help='Match full path, if False, match end of path')
@click.option('--strip-base', default='', help="String to remove from start of collection path")
@click.option('--collection', default=None, help='Collection in which replicants are expected')
@click.option('--match_entire_collection', default=False, help='If true, checks if there are any complete identical folders with all identical files')
@click.option('--checkby', default="name", help='Checks by name or filesize or both')
def locate_replicants(ctx, collection, strip_base, match_full_path,match_entire_collection,checkby):
    # this is using the capability from the interface locate replicants, so this docstring is duplicated from there
    """
    For all the files in a given collection, look for other
        files in other collections which may be replicants,
        but which have not been matched because they have no size, and/or
        their path stem differs.

        Optionally use strip_base to remove the leading path in the collection files which looking for match.
        This can be used with or without match_full_path.
        if match_full_path True (default), replicants must match the path given from the input collection
        whether stripped or no. If False, then matches require the path in the replicant to end with
        the same structure as provided from the collection (whether stripped or not).

        e.g. given

        input file /blah/path/filex and candidate replicant /foo/path/filex,
        if strip_base = blah and match_full_path = False, this will match,  all other combinations will not match.

        input file /blah/path/filex and candidate path/file will match with strip-base=/blah/ and any option
        for match_full_path.

        input file path/filex will match replicant /foo/path/filex with match_full_path=False

        In all cases file names must match.

        We normally assume that there we are looking in a large set of *other* files for matches into a smaller
        set of collection files. If the collection likely contains more files than exist in the set of others,
        then it might be worth using try_reverse_for_speed=True (default False) to speed things up.
    Usage: cfsdb locate-replicants --collection=<collection> --checkby=<name>
    See "Identifying Replicants.rst" for further usage information
    """
    view_state, db = _set_context(ctx, collection)

    candidates, possibles = db.locate_replicants(collection, strip_base=strip_base, match_full_path=match_full_path,check=checkby)
    full_match=[]
    not_full_match=[]
    if match_entire_collection:
        for c, p in zip(candidates, possibles):
            for x in p:
                for col in x.in_collections:
                    if col not in not_full_match:
                        full_match.append(col)
                for col in full_match:
                    if col not in x.in_collections:
                        full_match.delete(col)
                        not_full_match.append(col)
            print(c, [(x, x.replicas, x.in_collections) for x in p])

    no_replicant_found = True
    for c, p in zip(candidates, possibles):
        if (len(p))>1:
            no_replicant_found = False
            print("File:",c.name, "has the following replicas:")
            for x in p:
                print("Replica file", "\""+x.name+"\""," in the following collections:", [n.name for n in x.in_collections],"\n")
    if no_replicant_found:
        print("No replicants found")
    view_state.save()

@cli.command()
@click.pass_context
@click.argument('collection')
@click.option('--description_file', default=None, help='(Optional) File in which a description for this collection can be found')
def organise(ctx, collection, description_file):
    """
    Take a list of files move them into a collection with name COLLECTION
    If COLLECTION doesn't exist, create it. 
    If invoked from a terminal, provide an editor for entering files.
    Can also be invoked in a pipeline or using an input file (e.g. cfsdb organise yourc << YourFileListing)
    Files must exist in database before they can be organised.
    Usage: cfsdb organise <collectionname> --description_file=<file_location>
    """
    #FIXME This could probably do with a doc page
    view_state, db = _set_context(ctx, collection)

    if os.isatty(0):
        text = f"# (Don't remove this two line header)\n# Enter a list of files to be organised into {collection}:\n"
        text = click.edit(text)
        lines = text.split('\n')[2:]
    else:
        lines = sys.stdin.readlines()
    olines = [f.strip() for f in lines if f != ''] # clean for obvious UI issues
    lines = list(dict.fromkeys(olines))
    if len(lines)!=len(olines):
        print('WARNING: removed duplicates in file list!', file=sys.stderr)

    if description_file:
        with open(description_file, 'r') as f:
            description = f.readlines()
    else:
        description = None

    try:
        db.organise(collection, lines, description=description)
        view_state.save()
    except FileNotFoundError as err:
        print(err, file=sys.stderr)
        return 2

    

@cli.command()
@click.pass_context
@click.argument('collection')
@click.argument('tagname')
def tag(ctx, collection, tagname):
    """
    Tag a COLLECTION with TAGNAME
    (and save collection as current default collection)
    Usage: cfsdb tag <collection> <tagname>
    """
    view_state, db = _set_context(ctx, collection)
    db.tag_collection(view_state.collection, tagname)
    print("Tag",tagname,"added to",collection)
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
    Usage: cfsdb findc --match|tagname=<string>
    Alternate usage: cfsdb findc --facet <key> <value>
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
    Usage: cfsdb facet key value --collection=collection
    """
    view_state, db = _set_context(ctx, collection)
    if not view_state.collection:
        raise ValueError('Cannot use facet without defining a collection')

    c = db.retrieve_collection(view_state.collection)
 
    if remove:
        print(key,"/",c[key],"pair removed")
        del c[key]
    else:
        c[key] = value
        print(key,"/",value,"pair added")
    
    db.session.commit()
    view_state.save()


@cli.command()
@click.pass_context
@click.argument('col1')
@click.argument('link')
@click.argument('col2')
def linkto(ctx, col1, link, col2):
    """
    Add a one way link between two collections.
    e.g. col1 parent_of col2, would be
    linkto (col1, 'parent_of, col2)
    Makes no reciprocal links. This link can only
    be discovered from col1.
    Usage: cfsdb linkto collection1 relationshiplink collection2
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
    Usage: cfsdb linkto collection1 relationshiplink collection2
    """
    view_state, db = _set_context(ctx, col1)
    db.add_relationship(col1, col2, link)
    print("Relationship -",link,"- added between",col1,"and",col2)

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
    try:
        db.retrieve_collection(collection)
    except ValueError as err:
        print(err, file=sys.stderr)
    _print(db.retrieve_related(collection, link), 'name')
    view_state.save()


@cli.command()
@click.pass_context
@click.argument('collection')
@click.option('--force', default=False, help='(Optional) Deletes even if collection is full')
def delete_col(ctx, collection,force):
    """
    Delete an <collection> that contains no files
    Raises an error if the collection is not empty
    Usage: cfsdb delete-col <collection>
    """
    view_state, db = _set_context(ctx, None)
    _print(db.delete_collection(collection,force))
    view_state.save()

@cli.command()
@click.pass_context
@click.argument('collection')
def pr(ctx, collection):
    """
    Print information about a collection/json
    #FIXME add json support
    Usage: cfsdb pr <collection>
    """
    #FIXME add json support
    view_state, db = _set_context(ctx, None)
    markdown = db.collection_info(collection)
    md = Markdown(markdown)
    console = Console()
    console.print(md)
    view_state.save()


@cli.command()
@click.pass_context
@click.argument('collection')
def edit(ctx, collection):
    """
    Edit (and replace) a collection description
    Usage: cfsdb edit <collection>
    """
    view_state, db = _set_context(ctx, None)
    active_collection = db.retrieve_collection(collection)
    description = active_collection.description
    new_description = click.edit(description)
    active_collection.description = new_description
    print("New description saved for",collection)
    db.save()

@cli.command()
@click.pass_context
@click.argument('collection')
@click.argument('file')
def delete_file(ctx, collection,file):
    """
    Removes a file from a collection
    Usage: cfsdb delete-file <collection> <file>
    """
    view_state, db = _set_context(ctx, collection)
    db.delete_file_from_collection(collection,file)

if __name__ == "__main__":
    safe_cli()
