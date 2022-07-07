Ingesting Information
---------------------


* cfin - is a set of tools which allow you to ingest (load into the cfstore database):
    * all files in a directory (and all the sub directories) which are local or available via ssh (sftp actually).
    * all files known to Elastic Tape which are associated with a specific JASMIN group work space, and
    * all files (objects) held in an S3 bucket (not yet implemented).

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

         cfin rp setup locationname ssh_host username

      (you will need to have a running ssh-agent with the host key loaded)

3. To add local or remote posix files below a specfic directory:
    -  add a particular directory tree with ::

          cfin rp add location_name collection_name_for_path path_to_add
          cfin p|local add collection_name_for_path path_to_add

       (where you use ``rp`` or ``local`` in that first argument depending on whether it is
       local or remote POSIX)
       remote locations require a remote location name. Local locations do not, as the location is local
       , or
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




