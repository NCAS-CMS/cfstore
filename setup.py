import fnmatch
import os

from setuptools import setup, find_packages


def find_package_data_files(directory):
    for root, dirs, files in os.walk(directory):
        for basename in files:
            if fnmatch.fnmatch(basename, "*"):
                filename = os.path.join(root, basename)
                yield filename.replace("cfstore/", "", 1)


plugins = [f for f in find_package_data_files("cfstore/plugins")]
package_data = plugins

setup(
    name='cfstore',
    version='0.3.0',
    packages=find_packages(),
    url='',
    license='MIT',
    author='Bryan Lawrence + George OBrien',
    author_email='bryan.lawrence@ncas.ac.uk',
    description='Provides an interface to managing cf compliant data held in multiple storage locations',
    platforms=["Linux", "MacOS"],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS"
    ],
    install_requires=[
        'SQLAlchemy',
#        'pygraphviz',
#        'eralchemy',
        'click',
        'BeautifulSoup4',
        'paramiko',

    ],
    entry_points={
        'console_scripts': [
            'cfsdb=cfstore.cfdb:safe_cli',
            'cfin=cfstore.cfin:safe_cli',
            'cfmv=cfstore.cfmv:safe_cli'
        ],
    },
    package_data={"cfstore": package_data},
)
