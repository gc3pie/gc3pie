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
    name = "htpie",
    version = "0.1.00", # format: 0.MONTH.DAY (for now ...)

    packages = setuptools.find_packages(exclude=['ez_setup']),

    # metadata for upload to PyPI
    description = "High throughput grid execution framework.",
    author = "Grid Computing Competence Centre, University of Zurich",
    author_email = "gc3pie@googlegroups.com",
    license = "LGPL",
    keywords = "framework workflow statemachine grid  batch job",
    url = "http://gc3pie.googlecode.com/",   # project home page, if any

    entry_points = {
        'console_scripts': [
            # the generic, catch-all script:
            'htpie = htpie.gcmd:main'
            ],
       },

    # run-time dependencies ("pycrypto" is a dependency of Paramiko;
    # setuptools apparently does not process dependencies recursively)
    # we also need ase-3.3.1 (https://wiki.fysik.dtu.dk/ase), but it is not in pypi
    install_requires = ['mongoengine==0.4', 'argparse', 'ConcurrentLogHandler', 'numpy', 'pygraphviz', 
                        'pyparsing'], 

    
    # additional non-Python files to be bundled in the package
    include_package_data = True, 
    #package_data = {
    #    'htpie': ['etc/htpie.conf.example',
    #                 'examples/*.*'],
    #    },

    # `zip_safe` can ease deployment, but is only allowed if the package
    # do *not* do any __file__/__path__ magic nor do they access package data
    # files by file name (use `pkg_resources` instead).
    zip_safe = True,
)
