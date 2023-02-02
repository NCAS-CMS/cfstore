B-Metadata
---------------------

B or "Browse" metadata is designed to provide information useful for users to understand the information within files.
This is a mix of the context, generic metadata, and semantic information.
Further information on what b-metadata entails in available here https://www.dcc.ac.uk/news/bryan-lawrence-metadata-limit-sustainability


Parsing Metadata
---------------------

CF-store provides tools to parse b-metadata::

    cfin rp getbmetadataclean <sshlocation> <remotedirectory> <collection>

This function is more powerful than it looks - with configuration almost any information can be accessed remotely.
The main usage involves the --aggscriptname option. By setting this to a different script, different functionality can be applied.
By default, CF-store provides a scripts folder which contains some basic functionality.

In addition to this, cfsdb can parse metadata from aggregation files also provides a tool to read an aggregation file and store the metadata in a collection
This uses the (A)dd (A)ggregation (F)ile (T)o (C)ollection (aaftc) function::
  
    cfsdb aaftc <aggregationfile> <collection>

This will build a collection containing the files within the aggregation files with the collected metadata attached.