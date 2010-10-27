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
    name = "htpie",
    version = "0.1.00", # format: 0.MONTH.DAY (for now ...)

    packages = find_packages(exclude=['ez_setup']),

    # metadata for upload to PyPI
    description = "High throughput grid execution framework.",
    author = "Grid Computing Competence Centre, University of Zurich",
    author_email = "gc3pie@googlegroups.com",
    license = "LGPL",
    keywords = "grid arc globus ssh games batch job",
    url = "http://code.google.com/p/gc3pie/",   # project home page, if any

    entry_points = {
        'console_scripts': [
            # the generic, catch-all script:
            'htpie = htpie.gcmd:main'
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
