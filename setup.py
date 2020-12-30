from setuptools import setup, find_packages

setup(
    name='cfstore',
    version='0.2.0',
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
        'eralchemy',
        'click',
        'BeautifulSoup4',
    ],
    entry_points = {
        'console_scripts': [
            'cfstore=cfstore.cfdb:safe_cli',
        ],
    },
)
