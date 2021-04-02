Locations in cfstore
====================

cfstore allows you to have a range of different view of information held in one
or more storage *locations*.

Locations that cfstore knows about are:

1. The (POSIX) disks mounted where you are running cfstore itself (``location=local``);
2. POSIX disks mounted on a remote server which you can access using ssh;
3. The contents of buckets held in an object store accessible via S3; and
4. Data held in the JASMIN elastic tape store.

The method of working with locations is to _ingest_ "metadata" about what is held
in those locations. You don't need to know anything about that metadata, cfstore
knows what it needs to know, but you do need to tell it where to look for
information in those locations.

There are three sets of command line tools available:

* cfin - is a set of tools which allow you to ingest (load into the cfstore database):
    * all files in a directory (and all the sub directories) which are local or available via ssh (sftp actually).
    * all files known to Elastic Tape which are associated with a specific JASMIN group work space, and
    * all files (objects) held in an S3 bucket (not yet implemented).

Once the material is loaded into the database:

* cfdb - is a set of tools which allow you to
    * organise the files in your cfdb into _virtual_ collections, to tag collections, to add properties and descriptions to collections, all without moving data.
    * to create and manipulate the aggregations of files known as *atomic-datasets*.

When you want to manipulate the data,

* cfmove - is a set of tools to move data, or data sub-spaces, between locations.

Setting up locations
--------------------

The first thing you have to do is to create a cfstore database, or tell your tools where that database
exists. See :doc:`getting_started`.





