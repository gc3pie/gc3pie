#!/usr/bin/env python

# See http://packages.python.org/distribute/setuptools.html for details
from distribute_setup import use_setuptools
use_setuptools()


# XXX: `./setup.py` fails with "error: invalid command 'develop'" when
# package `distribute` is first downloaded by `distribute_setup`;
# subsequent invocations of it (when the `distribute-*.egg` file is
# already there run fine, apparently.  So, let us re-exec ourselves
# to ensure that `distribute` is loaded properly.
#REINVOKE = "__SETUP_REINVOKE"
# import sys
# import os
# if not os.environ.has_key(REINVOKE):
#     # mutating os.environ doesn't work in old Pythons
#     os.putenv(REINVOKE, "1")
#     try:
#         os.execvp(sys.executable, [sys.executable] + sys.argv)
#     except OSError, x:
#         sys.stderr.write("Failed to re-exec '%s' (got error '%s');"
#                          " continuing anyway, keep fingers crossed.\n"
#                          % (str.join(' ', sys.argv), str(x)))
# if hasattr(os, "unsetenv"):
#     os.unsetenv(REINVOKE)


def read_whole_file(path):
    stream = open(path, 'r')
    text = stream.read()
    stream.close
    return text


# see http://peak.telecommunity.com/DevCenter/setuptools
# for an explanation of the keywords and syntax of this file.
#
import setuptools
import setuptools.dist
setuptools.setup(
    name = "gc3pie",
    version = '2.0.0-rc1', # see: http://packages.python.org/distribute/setuptools.html

    packages = setuptools.find_packages(exclude=['ez_setup'])+['.'],
    # metadata for upload to PyPI
    description = "A Python library and simple command-line frontend for computational job submission to multiple resources.",
    long_description = read_whole_file('README.txt'),
    author = "Grid Computing Competence Centre, University of Zurich",
    author_email = "gc3utils-dev@gc3.lists.uzh.ch",
    license = "LGPL",
    keywords = "grid arc globus sge gridengine ssh gamess rosetta batch job",
    url = "http://gc3pie.googlecode.com/", # project home page

    # see http://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers = [
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)",
        "License :: DFSG approved",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.4",
        "Programming Language :: Python :: 2.5",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Chemistry",
        "Topic :: System :: Distributed Computing",
        ],

    entry_points = {
        'console_scripts': [
            # the generic, catch-all script:
            'gc3utils = gc3utils.frontend:main',
            # entry points to specific subcommands:
            'gclean = gc3utils.frontend:main',
            'gget = gc3utils.frontend:main',
            'ginfo = gc3utils.frontend:main',
            'gkill = gc3utils.frontend:main',
            'gservers = gc3utils.frontend:main',
            'gresub = gc3utils.frontend:main',
            'gstat = gc3utils.frontend:main',
            'gsub = gc3utils.frontend:main',
            'gtail = gc3utils.frontend:main',
            'gsession = gc3utils.frontend:main',
            'gselect = gc3utils.frontend:main',
            ],
       },

    # run-time dependencies
    install_requires = [
        # paramiko and pycrypto are required for SSH operations
        # ("pycrypto" is actually a dependency of Paramiko, but
        # setuptools apparently does not process dependencies recursively)
        'paramiko', 'pycrypto>=1.9',
        # lockfile 0.9 dropped support for Python 2.4; let's stick with 0.8
        'lockfile==0.8',
        # texttable -- format tabular text output
        'texttable',
        # pyCLI -- object-oriented command-line app programming
        'pyCLI>=2.0.3',
        # Needed by SqlStore
        'sqlalchemy',
        # Needed by ShellCmd backend
        'psutil>=0.6.1',
        # Needed for parsing human-readable dates (gselect uses it).
        'parsedatetime',
        ],
    # additional non-Python files to be bundled in the package
    data_files = [
        ('etc', [
            'gc3libs/etc/codeml.pl',
            'gc3libs/etc/gc3pie.conf.example',
            'gc3libs/etc/logging.conf.example',
            'gc3libs/etc/rosetta.sh',
            ],),
        ('gc3apps', [
            'gc3apps/gc3.uzh.ch/gridrun.py',
            'gc3apps/zods/gzods.py',
            'gc3apps/geotop/ggeotop.py',
            'gc3apps/geotop/ggeotop_utils.py',
            'gc3apps/ieu.uzh.ch/gmhc_coev.py',
            'gc3apps/turbomole/gricomp.py',
            'gc3apps/rosetta/gdocking.py',
            'gc3apps/rosetta/grosetta.py',
            'gc3apps/lacal.epfl.ch/gcrypto.py',
            'gc3apps/codeml/gcodeml.py',
            'gc3apps/gamess/grundb.py',
            'gc3apps/gamess/ggamess.py',
            ],),
        ],

    # `zip_safe` can ease deployment, but is only allowed if the package
    # do *not* do any __file__/__path__ magic nor do they access package data
    # files by file name (use `pkg_resources` instead).
    zip_safe = True,
)
