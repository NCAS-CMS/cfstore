----------------------
Installation
----------------------

Install CF-Python
----------------------

https://ncas-cms.github.io/cf-python/installation.html

pip install cf-python

Install udunits2
----------------------

conda install -c conda-forge udunits2

Install scipy
----------------------

pip install scipy 

Install CFStore
----------------------

pip install git+https://github.com/NCAS-CMS/cfstore.git

Dependencies
----------------------

netcdf4
BeautifulSoup4
paramiko
rich
sphinx-click
cfdm
python-dateutil
tqdm
deepdiff
django

Tests
----------------------

Tests are available from the tests directory and are runnable in any pytest compatible format

As an example, using coverage, we can run:
coverage manage.py test cfstore_viewer

Code Repo
----------------------

https://github.com/NCAS-CMS/cfstore