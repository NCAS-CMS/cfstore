



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

Storage locations are places where you keep data. The purpose of cfstore is to maintain *views*
of the data (in those multiple locations) *in one place* and to allow you to decorate
those views with information about the data to help you both remember things about the
data and to able to organise it in ways that allow you to find specific subsets of data, and 
potentially move them around.

The ``cfstore`` package provides three sets of tools to provide this view:

* ``cfin`` is used to ingest information about data held in a number of storage *locations*.
* ``cfsdb`` is used to add, organise and manipulate that information.
* ``cfmv`` allows you to use that information to move data between the storage *locations*.


Where To Begin
--------------
See :doc:`getting_started` to understand how to
get going and work with remote posix storage locations.

See :doc:`Examples/index` for some worked examples describing how to
ingest some data into your database, add information about that
data, and how to find and view your collections.

What We're Using (and Compatible With)
--------------------------------------

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

Our main focus is Linux compatibility but there's no reason that Windows and Mac shouldn't also *mostly* work.
If using a non-Linux machine, do tread carefully.

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
