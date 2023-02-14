----------------------------------------
Modifying Information About a Collection
----------------------------------------

Types of Description
--------------------

``cfsdb`` provides four main methods of attaching information to collections:

- Descriptions
    Descriptions contain a short user-inputted summary of the collection. A collection can only have one description.
- Tags
    A tag is a single-word label attached to a collection. Collections can have any number of tags.
- Facets
    A facet is a key/value pair attached to a collection. Collections can have any number of facets. These are notably used to store a lot of automatically assigned information.
- Relations
    A relation is a named one-directional link between two collections. Relations do not combine or affect files - they simply show how two collections are connected.

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