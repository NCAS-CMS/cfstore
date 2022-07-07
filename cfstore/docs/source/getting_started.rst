Getting Started
===============

The ``cfstore`` package provides three sets of tools:

* ``cfin`` is used to ingest information about data held in a number of storage *locations*.
* ``cfsdb`` is used to add, organise and manipulate that information.
* ``cfmv`` allows you to use that information to move data between the storage *locations*.

Storage locations are places where you keep data. The purpose of cfstore is to maintain *views*
of the data (in those multiple locations) *in one place* and to allow you to decorate
those views with information about the data to help you both remember things about the
data and to able to organise it in ways that allow you to find specific subsets of data, and 
potentially move them around.

It is important to remember that ``cfin`` does not actually ingest *the* data, it ingests
information *about* the data. ``cfsb`` allows you to arbitrarily organise information about
the data, but it does not touch or move the data itself.  However, data can be moved
between storage locations by ``cfmv`` so it does more than manipulate information *about*
the data, it actually does things *to* the data.

The storage locations where your data are now likely has different properties (at least
the physical location, but possibly other characteristics such as quotas, performance etc. Some will
be read-only, e.g. tape).
Often you will want to know what data you have across all locations, and think about
whether or not the data is unique, or replicated, and you might want to have virtual
collections which are subsets of physical collections and such virtual collections may 
even span physical locations. Some storage locations could be read-only, in which
case you would mainly use ``cfsdb`` to provide views into that data, but most will be read/write, and your
main use of ``cfsdb`` will to organise your thinking about the data, while you use ``cfmv`` to move data around
to meet quota restrictions and/or performance requirements.

NB: Despite all this talk about ``cfmv``, we haven't implmented that yet!

Workflow
--------

Whatever the nature of the storage location, you will need to start by ingesting initial views of the data
they already hold, using ``cfin``. Once you have that initial information, you will likely then organise
"virtual views" of that data, perhaps decorating those views with descriptions, tags, and properties,
all using ``cfsdb``. With that information you should be able to identify where you have data held in multiple
locations, and make good decisions about what data you need where - and then use the ``cfmv`` tools to
move the data accordingly.

For example: you currently run clamate simulations on ARCHER2, and have output data there. You also have some
of that data in a group workspace on JASMIN, and some in elastic tape. Once you have ingested views of
that data you could:

1. Find all duplicate files, ensure you have moved a copy of everything to tape, then remove the disk copies.
2. Organise virtual collections, such as all your data using the N512 UM,
3. Create cf aggregation views of the atomic datasets in that data, and
4. extract subsets onto disk as required.

(You can't do steps 3 and 4 yet with this version of the code, but that's where we are aiming.)

It is important to remember that your view of all the storage locations itself just lives on one machine,
perhaps your laptop/desktop, and you need to interact with cfstore from that one physical location.

Configuration
-------------

The initial configuration is set up by default, using a configuration file and a database file put in a
``.cfstore`` directory in your home directory. You can choose to put the configuration file elsewhere
using an environment variable (``CFS_CONFIG_FILE``).

The default configuration uses an sqlite database which is put in the directory alongside the configuration
file. It that might grow quite large, so if the configuration location (e.g. your home disk) is not
the right place for it, you will want to edit the ``.cfstore/config.ini`` file to point to its location.

(For now we only support an sqlite database, but other options will become available, and then
we expect you will be able to point to that location via the configuration file and/or tools
which modify the configuration file.)

``cfstore`` understands the concept of data being held on 
1. The (POSIX) disks mounted where you are running cfstore itself (``location=local``);
2. POSIX disks mounted on a remote server which you can access using ssh;
3. The contents of buckets held in an object store accessible via S3; and
4. Data held in the JASMIN elastic tape store.

To use cfstore to document data beyond your local environment, you will need to tell cfstore
something about your credentials. In particular you will need to set things up so that
cfstore knows which ssh credentials to use to access any given remote system, *and*, if
you are interested in elastic tape and are running cfstore outside RAL, you will need
to provide cfstore with ssh credentials to get inside the JASMIN firewall (these might
be the same credentials you have used to tell cfstore how to access JASMIN disk if you
have done that).

At the moment you can't configure cfstore to access S3 data, that will be available in later
release.

Configuring ssh and remote locations
------------------------------------

First, let's set things up for access to a remote posix system. We'll work this 
through for access to JASMIN, from, say, your laptop running in your university's VPN.
The following instructions assume you are on a linux or mac. We'll add Windows
instructions in due course.

1. Make sure you have setup access to the remote machine in your `~/.ssh/config` file,
something like the following for user madonna::
   Host nxlogin2   
      IdentityFile ~/.ssh/my_key_file
      Hostname nx-login2.jasmin.ac.uk
      User madonna
      ForwardAgent yes
   Host xfer1
      Hostname xfer1.jasmin.ac.uk
      User madonna
      IdentityFile ~/.ssh/my_key_file
      ProxyJump nxlogin2

2. It is best you have added the key of interest (``~/.ssh/my_key_file``) to a running agent
(instructions for JASMIN can be found `here <https://help.jasmin.ac.uk/article/187-login>`_).

3. Now tell cfstore about this 
location::
    cfin rp setup location_name xfer1 madonna

where you can use whatever _location_name_ you want. In this case you might simply want
to call it jasmin, so you'd be doing::
    cfin rp setup jasmin xfer1 madonna


If you have multiple remote sites with ssh access, you will need to repeat these steps to
set up ssh access to each remote location (with a different location name for each). 

Note  that ``cfstore`` makes and keeps no copies of ssh credentials, it is simply binding your 
_location_name_ to the credentials you already have, so you can use them when accessing
_location_name_ in subsequent commands.

Configuring for elastic tape access outside RAL
-----------------------------------------------

If you are setting up remote access to JASMIN, then you will be using the
same ssh credentials for access to JASMIN elastic tape. If you are using
elastic tape, but not JASMIN posix, disk, you will need steps 1 and 2,
but not step 3.

(If you are going to run cfstore
within JASMIN, you can use local posix, and you don't need to set anything
special up for either local posix or elastic tape in that situation).
