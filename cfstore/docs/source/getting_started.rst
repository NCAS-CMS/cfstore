Getting Started
===============

Under construction.

Configuration
-------------

The initial configuration is set up by default, using a configuration file and a database file put in a
``.cfstore`` directory in your home directory. You can choose to put the configuration file elsewhere
using an environment variable (``CFS_CONFIG_FILE``).

The default configuration uses an sqlite database which is put in the directory alongside the configuration
file. It that might grow quite large, so if the configuration location (e.g. your home disk) is not
the right place for it, you will want to edit the ``.cfstore/config.ini`` file to point to it's location.

(For now we only support an sqlite database, but other options will become available, and then
we expect you will be able to point to that location via the configuration file and/or tools
which modify the configuration file.)







