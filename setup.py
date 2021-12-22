from setuptools import setup, find_packages

setup(
    name='cfstore',
<<<<<<< HEAD
    version='0.3.0',
    packages=find_packages(),
=======
    version='0.2.1',
    packages=['cfstore'],
>>>>>>> 99dbf20976b4af194133e4e8a9bf180e9c03ae6e
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
)
