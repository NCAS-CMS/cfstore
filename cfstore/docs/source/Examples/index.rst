Examples
========

.. toctree::
    Ingesting Information
    Viewing Information
    Modifying Information
    Searching for Collections
    Identifying Replicants

The sections above provide more detailed information about how you can use ``cfin`` and ``cfsdb``,
here we work through one specific example - gathering information about a simulation
on jasmin disk disk, and seeing if we can find out how much of that data is on jasmin elastic tape.

This exercise is being carried out on a laptop at home. So in this instance the user has decided
that they will keep their cfstore information on one site, a laptop which they have with them
most of the time.

The simulation data of interest is a simulation called ``xjanp`` (don't ask why). The online
copy live inside the jasmin group workspace ``hiresgw`` at path ``/gws/nopw/j04/hiresgw/xjanp``.


We begin by assuming we have set up the remote posix location using the method outlined
in :doc:`../getting_started` so we have a remote posix location called jasmin.

So we want to add a collection to describe that online data::
    cfin rp add jasmin xjanp_ingws  /gws/nopw/j04/hiresgw/xjanp

INTERIM-COMMIT-THIS-NEEDS-COMPLETING
#TODO 









We work through some examples of things you might want to do:

1. Ingest a new collection of files from a specific location into your database.
2. View information about that collection and then modify the information by
   - adding tags
   - adding key-value pairs
   - modifying the description.
3. Finding a collection from within your collections via a tag or some words in the description.
4. Identifying replicants across collections.

----------------------
Ingesting a Collection
----------------------

TODO


-------------------
Viewing Information
-------------------

You can list all your collections with the `ls` command like this::

        cfsdb ls --collection=all

Output might look something like this::

    cfsdb ls --collection=all
    cfstore.db
    Collection <xjanp> has  100.1TiB in 1326 files
    Collection <et_582> has  553.3GiB in 71 files
    Collection <et_601> has  553.9GiB in 64 files
    Collection <et_602> has  3.2TiB in 137 files
    Collection <et_656> has  10.4TiB in 955 files
    Collection <et_4106> has  98.5TiB in 1172 files
    Collection <et_4867> has  31.9TiB in 318 files
    Collection <et_5548> has  36.6TiB in 654 files


Modifying the description: You can view information about a collection using the `pr` command::

        cfsdb pr collection_name

You should see something like this:

.. image:: /_static/screenshot-cfsdb-pr-eg1.png
    :width: 600
    :alt: (should show a screenshot of the output of this command in an example situation)

You can edit the description by using the `edit` command::

        cfsdb edit collection_name

which will open an editor, when you save the content it will replace
the collection description in your database.

