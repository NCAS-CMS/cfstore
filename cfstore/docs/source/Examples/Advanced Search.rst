Advanced Search

Search collections allows a more precise search for specific properties of collections.
The command is structured as::
    
    cfdb search-collections --<searchoption>=<searchstring>


The following search options can be chosen
    - name_contains 
        - Searches within collection name
    - description_contains 
        - Searches within description
    - contains_file 
        - Checks if the collection contains a specific file
    - tagname 
        - Checks if a collection contains a specific file
    - facets
        - Checks if a collection has a specific facet key

Search for collections with specific features
The supported search properties are name_contains, description_contains, contains_file, tagname, facet


Metadata Searching
-------------------

Metadata has a bespoke search command aimed to pinpoint values
This is done by inputting the following command::

    cfsdb searchvariable <key> <value>

Browse
-------------------

Browse (as opposed to search) lets users build up searches one step at a time
To start with, an intial search is generated similar to searcg variable::

        cfsdb browsevariable <key> <value>

However this  will instead output a menu::

    There are <number of results> results found.
    Print them all or continue to browse
    Input (p)rint or (b)rowse

By inputting p or print, the output collection will be printed
By inputting b or browse the following will be output::

    Input additional search in the format "key,value"   

An additional input will then be requested in the format <key>,<value>
This will then repeat until printing is requested
