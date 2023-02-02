Organizing Files into Collections
-------------------
The main way of organizing files is through the use of collections
A collection consists of:
    - A name
    - A list of files
    - A list of tags
    - A list of facets
    - A list of relationships between the collection and other collections

A collection can be generated one of two ways
    - The first method is done by ingesting information from a file location (see "Ingesting Information" for more information)
    - The second method is done by using the cfdb organize command like so:

    cfsdb organise CollectionFromFile --description_file=FileLocation.txt

Tags
-------------------

Tags are user inputted strings attached to collections.
They can be almost anything - just useful things for personal organizing!
Tags cannot start with underscores (e.g. _checksum), this is because this is reserved for automated tags
To tag a collection use the following command::

    cfsdb tag <collection> <tagname>

Which will output the following to confirm::

    Tag <tagname> added to <collection>



Facets 
-------------------

Facets are key pairs attached to collections.
Similar to tags, they can be manually set as almost anything
To add a facet to a collection use the following command::

    cfsdb facet <key> <value> --collection=<collection>

Which will output the following to confirm::

    <key>/<value> pair added



Relationships
-------------------

Relationships organise collections by linking two collections together.
This can be done by setting a one way collection like so::

    cfsdb linkto <collection1> <relationshiplink> <collection2>

This is only visible in <collection1>

Alternatively a two way collection can be set up like so::

    cfsdb linkto <collection1> <relationshiplink> <collection2>

This is visible in both <collection1> and <collection2>

Automated Tags
-------------------

Certain tags can be automatically added to a collection.
When ingesting information from a file location, the arguments involved will be stored as facets

Metadata can also be automatically parsed. 
More information can be found on the Ingesting Metadata section