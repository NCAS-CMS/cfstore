Getting Started
===============

The ``cfstore`` package provides three sets of tools:

* ``cfin`` is used to ingest information about data held in a number of storage *locations*.
* ``cfsdb`` is used to add, organise and manipulate that information.
* ``cfmv`` allows you to use that information to move data between the storage *locations*.

Storage locations are places where you keep data. These storage locations will have different properties,
and you will likely need to provide views of data held in these locations and sometimes migrate
data between them (e.g. to and from tape).  Some storage locations could be read-only, in which
case you would mainly use ``cfsdb`` to provide views into that data, but most will be read/write, and your
main use of ``cfsdb`` will to organise your thinking about the data, while you use ``cfutils`` to move data around
to meet quota restrictions and/or performance requirements.

Whatever the nature of the storage location, you will need to start by ingesting initial views of the data
they already hold, using ``cfin``. Once you have that initial information, you will likely then organise
"virtual views" of that data, perhaps decorating those views with descriptions, tags, and properties,
all using `cfsdb`. With that information you should be able to identify where you have data held in multiple
locations, and make good decisions about what data you need where - and then use the `cfmv` tools to
get the ``cfstore`` servers to move the data accordingly.

For example: you currently run clamate simulations on ARCHER2, and have output data there. You also have some
of that data in a group workspace on JASMIN, and some in elastic tape. Once you have ingested views of
that data you could:

1. Find all duplicate files, ensure you have moved a copy of everything to tape, then remove the disk copies.
2. Organise virtual collections, such as all your data using the N512 UM, and all the data of similar resolution in the CEDA archive.
3. Create cf aggregation views of the atomic datasets in that data, and
4. extract subsets onto disk as required.

Configuration
-------------

The initial configuration is set up by default, using a configuration file and a database file put in a
``.cfstore`` directory in your home directory. You can choose to put the configuration file elsewhere
using an environment variable (``CFS_CONFIG_FILE``).

The default configuration uses an sqlite database which is put in the directory alongside the configuration
file. It that might grow quite large, so if the configuration location (e.g. your home disk) is not
the right place for it, you will want to edit the ``.cfstore/config.ini`` file to point to it's location.

(For now we only support an sqlite database, but other options will become available, and then
we expect you will be able to point to that location via the configuration file and/or tools
which modify the configuration file.)


Ingesting Information
---------------------

*The information in this section represents goals, not all of this functionality is yet available
and it might differ in detail when implemented.*

Two types of location are currently supported, elastic tape, and posix file systems
(local or remote). (Posix file systems are the familiar Unix file systems that look
like ``/home/user/fred/folder/...``)

*NB: There are changes to the ET interface to cfstore which arise from changes at JASMIN
made in November 2021, this documentation needs to be updated!*

1. For Elastic tape,
    - You can ingest everything that is known about an elastic tape GWS::

         cfin et add gwsname

      or
    - You can ingest an update for a gws::

         cfin et update gwsname
2. For remote posix file systems where you have ssh access,
    - you must first declare the location::

         cfin rp setup locationname ssh_host

      (you will need to have a running ssh-agent with the host key loaded)

3. To add local or remote posix files below a specfic directory:
    -  add a particular directory tree with ::

          cfin rp|local add collection_name_for_path path_to_add

       (where you use ``rp`` or ``local`` in that first argument depending on whether it is
       local or remote POSIX), or
    -  update an existing collection with information held at another path::

           cfin rp|local add collection_name_for_path new_path

    - In both cases the application will open an editor for you to enter an initial
      description for this collection. If you already have this in a file, you can do
      it this way::

          cfin rp|local add collection_name_for_path path_to_add < description_file

4. Sometimes you will want to remove the ``cfsdb`` representation of files held
   below a specific directory because you've moved/deleted them using different
   tools.

   - To do this, you can ::

        cfin rp|local clean path_to_clean

   - All representations of files held in that location below that path in ``cfsdb`` will be
     removed, no matter which collections they are in.  However, the collections
     themselves will not be removed, you will need to use ``cfsdb`` tools to do that.
     You might, or might not, then want to re-add the collection.









