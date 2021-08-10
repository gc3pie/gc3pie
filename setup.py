#!/usr/bin/env python
"""
Setup file for installing GC3Pie.
"""

from __future__ import absolute_import
import sys

# ensure we run a "recent enough" version of `setuptools` (CentOS7 still ships
# with setuptools 0.9.8!). There has been some instability in the support for
# PEP-496 environment markers in recent versions of `setuptools`, but
# Setuptools 20.10.0 seems to have restored full support for them, including
# `python_implementation`
from ez_setup import use_setuptools
use_setuptools(version='21.0.0')

import setuptools
import setuptools.dist
# avoid setuptools including `.svn` directories into the PyPI package
from setuptools.command import sdist
try:
    del sdist.finders[:]
except AttributeError:
    # `.finders` was removed from setuptools long ago
    pass


## auxiliary functions
#
def read_whole_file(path):
    """
    Return file contents as a string.
    """
    with open(path, 'r') as stream:
        return stream.read()


## test runner setup
#
# See http://tox.readthedocs.org/en/latest/example/basic.html#integration-with-setuptools-distribute-test-commands # noqa
# on how to run tox when python setup.py test is run
#
from setuptools.command.test import test as TestCommand

class Tox(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import tox
        errno = tox.cmdline(self.test_args)
        sys.exit(errno)


## conditional dependencies
#
# Although PEP-508 and a number of predecessors specify a syntax for
# conditional dependencies in Python packages, support for it is inconsistent
# (at best) among the PyPA tools. An attempt to use the conditional syntax has
# already caused issues #308, #249, #227, and many more headaches to me while
# trying to find a combination of `pip`, `setuptools`, `wheel`, and dependency
# specification syntax that would work reliably across all supported Linux
# distributions. I give up, and revert to computing the dependencies via
# explicit Python code in `setup.py`; this will possibly break wheels but it's
# the least damage I can do ATM.

python_version = sys.version_info[:2]
if python_version == (2, 6):
    version_dependent_requires = [
        # Alternate dependencies for Python 2.6:
        # - PyCLI requires argparse,
        'argparse',
        'lockfile==0.11.0',
        # Paramiko and its dependencies all ceased support for Py2.6
        # NOTE: order matters!  Dependencies must come before the main pkg
        'pycparser<2.19',
        'pynacl<1.1.0',
        'idna<2.7',
        'paramiko<2.4',
        # parsedatetime officially dropped supprt for Py 2.6 in version 1.0
        # but the PyPI tags show that it is compatible with Py2.6 until <=1.4
        'parsedatetime<1.5',
        # python-daemon seems to have dropped Py2.6 between v1.6 and v2.0
        'python-daemon<2.0',
        'pyyaml<=3.11',
        # SQLAlchemy ceased support for Py 2.6 in version 1.2.0
        'sqlalchemy<1.2',
    ]
else:
    version_dependent_requires = [
        'lockfile',
        'paramiko', 'pycrypto',
        # Needed for parsing human-readable dates (gselect uses it).
        'parsedatetime',
        # Needed by `gc3libs.cmdline`
        'python-daemon',
        'pyyaml',
        'sqlalchemy',
    ]


## real setup description begins here
#
setuptools.setup(
    name="gc3pie",
    version="2.6.8",  # see PEP 440

    packages=setuptools.find_packages(exclude=['ez_setup']),
    # metadata for upload to PyPI
    description=(
        "A Python library and simple command-line frontend for"
        " computational job submission to multiple resources."
    ),
    long_description=read_whole_file('README.rst'),
    author=', '.join([
        # only long-time core authors are listed here;
        # please see file `docs/credits.rst`
        'Sergio Maffioletti',
        'Antonio Messina',
        'Riccardo Murri',
    ]),
    author_email="gc3pie-dev@googlegroups.com",
    license="LGPL",
    keywords=str.join(' ', [
        "batch",
        "cloud",
        "cluster",
        "differential optimization",
        "ec2",
        "gridengine",
        "job management",
        "large-scale data analysis",
        "openstack",
        "pbs",
        "remote execution",
        "sge",
        "slurm",
        "ssh",
        "torque",
        "workflow",
    ]),
    url="http://gc3pie.readthedocs.io/",  # project home page

    # see http://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        ("License :: OSI Approved :: GNU Library or"
         " Lesser General Public License (LGPL)"),
        "License :: DFSG approved",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Chemistry",
        "Topic :: System :: Distributed Computing",
        ],

    entry_points={
        'console_scripts': [
            # the generic, catch-all script:
            'gc3utils = gc3utils.frontend:main',
            # entry points to specific subcommands:
            'gclean = gc3utils.frontend:main',
            'gclient = gc3utils.frontend:main',
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
            'gcloud = gc3utils.frontend:main',
            ],
    },

    # run-time dependencies
    install_requires=(version_dependent_requires + [
        'blinker',
        'coloredlogs',
        'dictproxyhack',
        # prettytable -- format tabular text output
        'prettytable',
        # pyCLI -- object-oriented command-line app programming
        'pyCLI',
        # needed by DependentTaskCollection
        # (but incompatible with Py 2.6, so we include a patched copy)
        #toposort==1.0
        # Py2/Py3 compatibility layer
        'future>=0.17.0',
        'six',  # only used in `gc3libs/quantity.py`
    ]),
    extras_require={
        'ec2': [
            # The following Python modules are required by GC3Pie's `ec2`
            # resource backend.
            'boto',
        ],
        'daemon': [
            # daemon and inotifyx required for SessionBasedDaemon but
            # inotify functionality is only available on Linux, so
            # this is an optional feature ...
            'inotify_simple',
        ],
        'openstack': [
            'python-keystoneclient',
            'python-glanceclient',
            'python-neutronclient',
            'python-novaclient',
            'python-swiftclient',
            'os-client-config',
        ],
        'optimizer': [
            # The following Python modules are required by GC3Pie's
            # `gc3libs.optimizer` module.
            'numpy',
        ],
    },

    # Apparently, this list is read from bottom to top...
    tests_require=[
        'tox',
        'pytest>=4.1',
        'mock',
    ],
    cmdclass={'test': Tox},

    # additional non-Python files to be bundled in the package
    package_data={
        'gc3libs': [
            # helper scripts for classes in the core library
            'etc/codeml.pl',
            'etc/rosetta.sh',
            'etc/run_R.sh',
            'etc/turbomole.sh',
            'etc/run_matlab.sh',
            # Downloader script
            'etc/downloader.py',
            # example files
            'etc/gc3pie.conf.example',
            'etc/logging.conf.example',
            'etc/minibacct.c',
            # helper scripts for specific gc3apps
            'etc/gbenchmark_infomap_wrapper.sh',
            'etc/gbenchmark_wrapper.sh',
            'etc/gbenchmark_wrapper_allinone.py',
            'etc/gcelljunction_wrapper.sh',
            'etc/geosphere_wrapper.sh',
            'etc/geotop_wrap.sh',
            'etc/geotop_wrap.sh',
            'etc/gmodis_wrapper.sh',
            'etc/gndn_wrapper.sh',
            'etc/gnfs-cmd',
            'etc/gnlp_wrapper.py',
            'etc/gpyrad_wrapper.sh',
            'etc/grdock_wrapper.sh',
            'etc/gstructure_wrapper.sh',
            'etc/gweight_wrap.sh',
            'etc/gwrappermc_wrapper.sh',
            'etc/run_gtsub_control.sh',
            'etc/smd_projections_wrapper.sh',
            'etc/square.sh',
        ],
    },
    data_files=[
        ('gc3apps', [
            'gc3apps/gc3.uzh.ch/gridrun.py',
            'gc3apps/codeml/gcodeml.py',
            'gc3apps/gamess/grundb.py',
            'gc3apps/gamess/ggamess.py',
            ]),
    ],

    # `zip_safe` can ease deployment, but is only allowed if the package
    # do *not* do any __file__/__path__ magic nor do they access package data
    # files by file name (use `pkg_resources` instead).
    zip_safe=True,
)
