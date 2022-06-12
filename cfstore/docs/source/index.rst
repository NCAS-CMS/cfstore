



.. toctree::
   :titlesonly:
   
   getting_started
   Examples/index
   cmdline
   interface
   database

Welcome to cfstore's documentation!
===================================

Concept
-------
This package provides an interface to data held in a "cfstore",
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

There are two different interfaces available to the cfstore, the 
command line interface, and the python interface. At the moment
the python interface is rather low level and more suitable for
developers. A future release will provide a suitable interface for 
using in scientific scripts and python notebooks. Meanwhile
we are concentrating on support via the command line.

See :doc:`getting_started` to understand how to
get going and work with remote posix storage locations.

See :doc:`Examples/index` for some worked examples describing how to
ingest some data into your database, add information about that
data, and how to find and view your collections.


Command Line Interface
----------------------

There is a command line interface which is the main way that
users will interact with cfstore at the moment. 
With the command line interface you
can ingest data from various locations into the cfstore,
interrogate cfstore about the data you have ingested, and
organise the data into virtual collections. Virtual
collections can be described by markdown formatted descriptions,
text tags, and textual key/value pairs.

See :doc:`cmdline`

Python Interface
------------------

The python interface is currently more suitable for developers. 
See :doc:`interface`

Database Structure
------------------

The database structure is described at :doc:`database`. These details
of the structure should only be of interest to those maintaining this software.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
