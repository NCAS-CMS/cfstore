from setuptools import setup, find_packages

setup(
    name='cfstore',
    version='0.2.1',
    packages=['cfstore'],
    url='',
    license='MIT',
    author='Bryan Lawrence',
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
