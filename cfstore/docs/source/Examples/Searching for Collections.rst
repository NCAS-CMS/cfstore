-------------------------
Searching for Collections
-------------------------

Finding collections by tags
---------------------------
Input command:

cfsdb findc --tagname=exampletag

Example output:

testcollection
linkedtestcollection
linkedtestcollection2

Finding collections by searching
--------------------------------
Input command:

cfsdb findc --match=test

Example output:

*test*collection
linked*test*collection
linked*test*collection2
collectionthatendswith*test*

Finding collections by relation
-------------------------------
Input command:

cfsdb findr examplelink --collection=testcollection

Example output:

linkedtestcollection
linkedtestcollection2

Finding files by searching
--------------------------------
Input command:

cfsdb findf test --collection=all

Example output:

G:\\examplepath\\morepath\\testcollection.py
G:\\examplepath\\morepath\\linkedtestcollection.py
G:\\examplepath\\morepath\\linkedtestcollection.py