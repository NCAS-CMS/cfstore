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

.. image:: _static/screenshot-cfsdb-pr-eg1.png
    :width: 600
    :alt: (should show a screenshot of the output of this command in an example situation)

You can edit the description by using the `edit` command::

        cfsdb edit collection_name

which will open an editor, when you save the content it will replace
the collection description in your database.

Adding tags: TBD

Adding key-value pairs: TBD

Finding collections by tags: TBD

Finding collections by searching: TBD