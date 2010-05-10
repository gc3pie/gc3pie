#!/usr/bin/env python

# if `setuptools` are not installed, then use the simpler version
# provided with this package.
import ez_setup
ez_setup.use_setuptools()


# see http://peak.telecommunity.com/DevCenter/setuptools
# for an explanation of the keywords and syntax of this file.
#
from setuptools import setup, find_packages
setup( 
    name = "gc3utils",
    version = "0.5.10", # format: 0.MONTH.DAY (for now ...)

    packages = find_packages(exclude=['ez_setup']),
    scripts = ['gcmd.py'],

    # metadata for upload to PyPI
    description = "A Python library and simple command-line frontend for computational job submission to multiple resources.",
    author = "Grid Computing Competence Centre, University of Zurich",
    author_email = "info@gc3.uzh.ch",
    license = "LGPL",
    keywords = "grid arc globus ssh games batch job",
    url = "https://ocikbfs.uzh.ch/trac/gc3utils",   # project home page, if any

    #entry_points = {
    #    'cli': [
    #        # the generic, catch-all script:
    #        'gcmd = gc3utils.main.main',
    #       # symlinks to specific subcommands:
    #       'gsub = gc3utils.main.main',
    #       'gstat = gc3utils.main.main',
    #       'glist = gc3utils.main.main',
    #       'gkill = gc3utils.main.main',
    #       'gget = gc3utils.main.main',
    #       ],
    #   },

    # run-time dependencies ("pycrypto" is a dependency of Paramiko;
    # setuptools apparently does not process dependencies recursively)
    install_requires = ['paramiko', 'pycrypto>=1.9'], 

    # which non-Python files to install?
    package_data = {
        # If any package contains *.txt or *.rst files, include them:
        '': ['*.txt', '*.rst'],
        # And include any *.msg files found in the 'hello' package, too:
        #'hello': ['*.msg'],
    },

    # `zip_safe` can ease deployment, but is only allowed if the package
    # do *not* do any __file__/__path__ magic nor do they access package data
    # files by file name (use `pkg_resources` instead).
    zip_safe = True,
)
