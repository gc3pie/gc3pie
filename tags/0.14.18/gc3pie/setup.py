#!/usr/bin/env python

# See http://packages.python.org/distribute/setuptools.html for details
from distribute_setup import use_setuptools
use_setuptools()


# XXX: `./setup.py` fails with "error: invalid command 'develop'" when
# package `distribute` is first downloaded by `distribute_setup`;
# subsequent invocations of it (when the `distribute-*.egg` file is
# already there run fine, apparently.  So, let us re-exec ourselves
# to ensure that `distribute` is loaded properly.
REINVOKE = "__SETUP_REINVOKE"
import sys
import os
if not os.environ.has_key(REINVOKE):
    # mutating os.environ doesn't work in old Pythons
    os.putenv(REINVOKE, "1")
    try:
        os.execvp(sys.executable, [sys.executable] + sys.argv)
    except OSError, x:
        sys.stderr.write("Failed to re-exec '%s' (got error '%s');"
                         " continuing anyway, keep fingers crossed.\n"
                         % (str.join(' ', sys.argv), str(x)))
if hasattr(os, "unsetenv"):
    os.unsetenv(REINVOKE)


# see http://peak.telecommunity.com/DevCenter/setuptools
# for an explanation of the keywords and syntax of this file.
#
import setuptools
import setuptools.dist
setuptools.setup(
    name = "gc3pie",
    version = "0.14.15", # format: 0.(MONTH+12).DAY (for now ...)

    packages = setuptools.find_packages(exclude=['ez_setup']),

    # metadata for upload to PyPI
    description = "A Python library and simple command-line frontend for computational job submission to multiple resources.",
    author = "Grid Computing Competence Centre, University of Zurich",
    author_email = "gc3utils-dev@gc3.lists.uzh.ch",
    license = "LGPL",
    keywords = "grid arc globus sge gridengine ssh gamess rosetta batch job",
    url = "http://gc3pie.googlecode.com/", # project home page

    entry_points = {
        'console_scripts': [
            # the generic, catch-all script:
            'gc3utils = gc3utils.frontend:main',
            # symlinks to specific subcommands:
            'gclean = gc3utils.frontend:main',
            'gget = gc3utils.frontend:main',
            'ginfo = gc3utils.frontend:main',
            'gkill = gc3utils.frontend:main',
            'glist = gc3utils.frontend:main',
            'gresub = gc3utils.frontend:main',
            'gstat = gc3utils.frontend:main',
            'gsub = gc3utils.frontend:main',
            'gtail = gc3utils.frontend:main',
            ],
       },

    # run-time dependencies
    install_requires = [
        # paramiko and pycrypto are required for SSH operations
        # ("pycrypto" is actually a dependency of Paramiko, but
        # setuptools apparently does not process dependencies recursively)
        'paramiko', 'pycrypto>=1.9', 
        # lockfile 0.9 dropped supprot for Python 2.4; let's stick with 0.8
        'lockfile==0.8',
        # texttable -- format tabular text output
        'texttable',
        # pyCLI -- object-oriented command-line app programming
        'pyCLI',
        # magnitude -- represent memory, time, etc. as physical quantitites
        'magnitude',
        ],

    # additional non-Python files to be bundled in the package
    package_data = {
        'gc3libs': ['etc/gc3pie.conf.example',
                    'etc/logging.conf.example'],
        },

    # `zip_safe` can ease deployment, but is only allowed if the package
    # do *not* do any __file__/__path__ magic nor do they access package data
    # files by file name (use `pkg_resources` instead).
    zip_safe = True,
)