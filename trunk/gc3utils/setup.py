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
    name = "gc3utils",
    version = "0.12.16", # format: 0.MONTH.DAY (for now ...)

    packages = setuptools.find_packages(exclude=['ez_setup']),
    #scripts = ['gcmd.py'],

    # metadata for upload to PyPI
    description = "A Python library and simple command-line frontend for computational job submission to multiple resources.",
    author = "Grid Computing Competence Centre, University of Zurich",
    author_email = "gc3utils-dev@gc3.lists.uzh.ch",
    license = "LGPL",
    keywords = "grid arc globus sge gridengine ssh gamess rosetta batch job",
    url = "http://gc3pie.googlecode.com/",   # project home page, if any

    entry_points = {
        'console_scripts': [
            # the generic, catch-all script:
            'gc3utils = gc3utils.gcmd:main',
            # symlinks to specific subcommands:
            'gclean = gc3utils.gcmd:main',
            'gget = gc3utils.gcmd:main',
            'ginfo = gc3utils.gcmd:main',
            'gkill = gc3utils.gcmd:main',
            'glist = gc3utils.gcmd:main',
            'gresub = gc3utils.gcmd:main',
            'gstat = gc3utils.gcmd:main',
            'gsub = gc3utils.gcmd:main',
            'gtail = gc3utils.gcmd:main',
            'gnotify = gc3utils.gcmd:main',
            ],
       },

    # run-time dependencies ("pycrypto" is a dependency of Paramiko;
    # setuptools apparently does not process dependencies recursively)
    install_requires = ['paramiko', 'pycrypto>=1.9', 'lockfile==0.8'],

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
