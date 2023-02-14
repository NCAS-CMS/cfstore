-------------------------
Searching for Collections
-------------------------

Finding collections by searching
--------------------------------

The primary method of searching is  ``findc`` ("Find Collections").
Findc works by taking an input string and one of several options (match, tag or facet).
In the case of match, it will find all collections that have the ``match`` input somewhere in their name *or* description.

Input command::

    cfsdb findc --match=test

Example output::

    *test*collection
    linked*test*collection
    linked*test*collection2
    collectionthatendswith*test*


Finding collections by tags
---------------------------
If tag or facet are chosen instead, then collections will only be included if they have that tag or facet.


Input command::

    cfsdb findc --tagname=exampletag

Example output::

    testcollection
    linkedtestcollection
    linkedtestcollection2



Finding collections by facets
---------------------------
Facets can be searched in a simlar fashion but require a key/value pair in the following format ``cfsdb findc --facet <key> <value``:


Input command::

    cfsdb findc --facet testkey testvalue

Example output::

    testcollection
    linkedtestcollection
    linkedtestcollection2



Finding files by searching
--------------------------------

Alternatively, files can be searched for directly using ``findf`` ("Find files").
This takes a match input which looks for the given text in the file name or path name. 
In addition, a collection can be added so that the search is only done for files within a collection.

Input command::

    cfsdb findf test --collection=all

Example output::

    G:\\examplepath\\morepath\\testcollection.py
    G:\\examplepath\\morepath\\linkedtestcollection.py
    G:\\examplepath\\morepath\\linkedtestcollection.py

Finding collections by relation
-------------------------------

Finally, tags can be searched by relation.
Relations take an input of a relation link and a starting collection.
It will return all collections that the inputted collection links to through the inputted relation.
The following example shows ``testcollection`` linked to both ``linkedtestcollection`` and ``linkedtestcollection2`` through the ``examplelink`` relation:

Input command::

    cfsdb findr examplelink --collection=testcollection

Example output::

    linkedtestcollection
    linkedtestcollection2
