
Identifying Replicants
----------------------

Identifying replicant files (i.e. multiple identical files across collections) is one of the main use cases of cfstore.
For this purpose we have a CFSDB command called "locate-replicants".
Locate replicants takes in a single collection and identifies which of the stored files also exist in other collections ingested by cfstore.
We'll start by providing an example set of collections:

Input command:
cfsdb ls

Example output:
    cfstore.db
    Collection <et_582> has  1.1TiB in 71 files
    Collection <et_601> has  1.1TiB in 64 files
    Collection <et_602> has  6.5TiB in 137 files
    Collection <et_656> has  20.8TiB in 955 files
    Collection <et_4106> has  196.9TiB in 1172 files
    Collection <et_4867> has  63.7TiB in 318 files
    Collection <et_5548> has  73.2TiB in 654 files
    Collection <xjlehjas2> has  1.1TiB in 69 files
    Collection <xjlehjas3> has  553.9GiB in 69 files
    Collection <xjanpjas3> has  179.5TiB in 1278 files
    Collection <address> has  1.5MiB in 2 files

This is a combination of elastic tape files (et_NUMBER), files stored remotely on JASMIN (xj...) and local files (address)
The combination of ~4000 files is too big to manually identify which files are replicants.
We'll therefore run locate-replicants on various files


Basic searching
---------------
Input command:
cfsdb locate-replicants --collection=address --checkby=name

Example output:
No replicants found

This is the simplest example - all files in address are unique across collections. This information is outputted and the command ends.


Input command
cfsdb locate-replicants --collection=xjlehjas2 --checkby=name
Example outputs:
...
File: xjleha.pk19810921 has the following replicas:
Replica file "xjleha.pk19810921"  in the following collections: ['et_601'] 

Replica file "xjleha.pk19810921"  in the following collections: ['xjlehjas2', 'xjlehjas3']
...


...
File: ioserver_stash_log.0019 has the following replicas:
Replica file "ioserver_stash_log.0019"  in the following collections: ['et_582'] 

Replica file "ioserver_stash_log.0019"  in the following collections: ['et_601'] 

Replica file "ioserver_stash_log.0019"  in the following collections: ['et_602'] 

Replica file "ioserver_stash_log.0019"  in the following collections: ['xjlehjas2', 'xjlehjas3'] 
...

(example output is partial)

In the first half you can see an example file "xjleha.pk19810921".
The output shows two identical versions. 
The first version is stored only on et_601. 
The second is linked to two collections, xjlehjas2 and xjlehjas3, but is aware that these refer to the same file in the same path.

The second half is taken from the same output but has identified that the ioserver_stash_log.0019 (the io server log) is the same across multiple et files


Checking by property
--------------------
Notably, we set the "checkby" argument to "name" - this means the files are only compared by name.
We can instead check by filesize:

Input command
cfsdb locate-replicants --collection=xjlehjas2 --checkby=size
Example outputs:

..
File: ioserver_stash_log.0019 has the following replicas:
Replica file "ioserver_stash_log.0001"  in the following collections: ['et_582'] 

Replica file "ioserver_stash_log.0003"  in the following collections: ['et_582'] 

Replica file "ioserver_stash_log.0005"  in the following collections: ['et_582'] 

Replica file "ioserver_stash_log.0007"  in the following collections: ['et_582'] 

Replica file "ioserver_stash_log.0009"  in the following collections: ['et_582'] 
...

This is useful for when names may have changed but files will not or if looking for specifically sized files - for example empty ones.
By default "checkby" will be set to "both", checking both filesize and name.

Input command
cfsdb locate-replicants --collection=xjlehjas2 --checkby=both
Example outputs:
...
File: xjleha.pk19810921 has the following replicas:
Replica file "xjleha.pk19810921"  in the following collections: ['et_601'] 

Replica file "xjleha.pk19810921"  in the following collections: ['xjlehjas2', 'xjlehjas3']
...

...
File: ioserver_stash_log.0019 has the following replicas:
Replica file "ioserver_stash_log.0019"  in the following collections: ['et_582'] 

Replica file "ioserver_stash_log.0019"  in the following collections: ['et_601'] 

Replica file "ioserver_stash_log.0019"  in the following collections: ['et_602'] 

Replica file "ioserver_stash_log.0019"  in the following collections: ['xjlehjas2', 'xjlehjas3'] 
...

Output is identical to name.

Parsing Filepaths
-----------------

There are two additional arguments for parsing filepaths. "match-full-path" and "strip-base".

match-full-path defaults to False, if set true it only finds replicants that have exactly equal filepaths.
That means files that have identical storage locations will be linked.

Input command
match-full-path
Example outputs:
...
File: xjleha.pk19810921 has the following replicas:
Replica file "xjleha.pk19810921"  in the following collections: ['xjlehjas2', 'xjlehjas3']
...

if a string is inputted into strip-base, then that string is removed from the search path.