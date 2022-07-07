----------------------------------------
Modifying Information About a Collection
----------------------------------------

Editing a Description
---------------------
Input command::
    cfsdb edit testcollection

Example output::
    *description file opens*
    *when closed:* New description saved for testcollection

The edit function takes a collection and opens its description file to allow editing

Editing a Facet or Tag
----------------------
Input command::
    cfsdb examplekey examplevalue --collection=testcollection
    cfsdb examplekey examplevalue2 --collection=testcollection 

Example output::
    examplekey / examplevalue pair added
    examplekey / examplevalue2 pair added

Adding a facet to a previous key overwrites the value with the new value.