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
    name = "gorg",
    version = "0.6.22", # format: 0.MONTH.DAY (for now ...)

    packages = find_packages(exclude=['ez_setup']),
    #scripts = ['gcmd.py'],

    # metadata for upload to PyPI
    description = "A Python library and simple command-line frontend for managing multiple jobs.",
    author = "Grid Computing Competence Centre, University of Zurich",
    author_email = "gc3utils-dev@gc3.lists.uzh.ch",
    license = "LGPL",
    keywords = "grid arc globus ssh games batch job",
    url = "https://ocikbfs.uzh.ch/trac/gc3utils",   # project home page, if any

    entry_points = {
        'console_scripts': [
            # the generic, catch-all script:
            #'gcmd = gc3utils.gcmd:main',
	    # todo: fix
            'gorgclient = gorg.gorgclient:main',
            'hessiantask = gorg.tcmd:main',
            'restarttask = gorg.tcmd:main',
            'singletask = gorg.tcmd:main',
            'tget = gorg.tcmd:main',
            'tkill = gorg.tcmd:main',
            'tlist = gorg.tcmd:main',
            'tstat = gorg.tcmd:main',
            'tsub = gorg.tcmd:main',
            ],
       },

    # run-time dependencies ("pycrypto" is a dependency of Paramiko;
    # setuptools apparently does not process dependencies recursively)
    #install_requires = ['paramiko', 'pycrypto>=1.9', 'lockfile'], 

    # additional non-Python files to be bundled in the package
    #package_data = {
    #    'gc3utils': ['etc/gc3utils.conf.example',
    #                 'etc/logging.conf.example'],
    #    },

    # `zip_safe` can ease deployment, but is only allowed if the package
    # do *not* do any __file__/__path__ magic nor do they access package data
    # files by file name (use `pkg_resources` instead).
    zip_safe = True,
)
