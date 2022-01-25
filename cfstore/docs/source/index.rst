.. cfstore documentation master file, created by
   sphinx-quickstart on Tue Dec 22 12:49:28 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to cfstore's documentation!
===================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   getting_started
   examples
   cmdline
   interface
   database
   location

Concept
-------
This package provides a command line interface to data held in a "cfstore",
that consists of views of data files held in one or more different storage locations
(e.g. POSIX disk, object store, tape).
The tools allow you to ingest information about collections (defined by you) of the data
into a "cfstore database", to create and document new views (virtual
collections) of your data, and where necessary move collections of data
between storage locations.

As well as the original files, the CF aggregation rules can be used to
provide simplified views of multiple files, and both the original files
and these "atomic dataset" can be organised into virtual collections which can
be tagged and decorated with markdown descriptions and key,value pairs.
Data can be extracted and moved between storage locations as required.

Currently cfstore knows about POSIX disks and the JASMIN elastic tape
system. S3 interfaces to object stores will be the next location
to be supported.

See :doc:`getting_started` and :doc:`location` to understand how to
get going and work with different storage locations.

See :doc:`examples` for some worked examples describing how to
ingest some data into your database, add information about that
data, and how to find and view your collections.


Command Line Interface
----------------------

There is a command line interface which is the main way that
users will interact with cfstore (at least until there is a
a web interface built). With the command line interface you
can ingest data from various locations into the cfstore,
interrogate cfstore about the data you have ingested, and
organise the data into virtual collections. Virtual
collections can be described by markdown formatted descriptions,
text tags, and textual key/value pairs.

See :doc:`cmdline`

Database Interface
------------------

See :doc:`interface`

Database Structure
------------------

The database structure is described at :doc:`database`.
That structure should only be of interest to those maintaining this software.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
