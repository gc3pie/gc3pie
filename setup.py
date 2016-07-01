#!/usr/bin/env python
"""
Setup file for installing GC3Pie.
"""

import sys


# ensure we run a "recent enough" version of setuptools (CentOS7 still
# ships with setuptools 0.9.8!); release 8.0 is the first one to fully
# implement PEP 440 version specifiers
from ez_setup import use_setuptools
use_setuptools(version='8.0')

import setuptools
import setuptools.dist
# avoid setuptools including `.svn` directories into the PyPI package
from setuptools.command import sdist
if hasattr(sdist, 'finders'):
    del sdist.finders[:]


## auxiliary functions
#
def read_whole_file(path):
    """
    Return file contents as a string.
    """
    with open(path, 'r') as stream:
        return stream.read()

def read_file_lines(path):
    """
    Return list of file lines, stripped of leading and trailing
    whitespace (including newlines), and of comment lines.
    """
    with open(path, 'r') as stream:
        lines = [line.strip() for line in stream.readlines()]
        return [line for line in lines
                if line != '' and not line.startswith('#')]


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


## real setup description begins here
#
setuptools.setup(
    name="gc3pie",
    version="2.5.dev",  # see PEP 440

    packages=setuptools.find_packages(exclude=['ez_setup']),
    # metadata for upload to PyPI
    description=(
        "A Python library and simple command-line frontend for"
        " computational job submission to multiple resources."
    ),
    long_description=read_whole_file('README.rst'),
    author="S3IT, Zentrale Informatik, University of Zurich",
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
    url="http://gc3pie.googlecode.com/",  # project home page

    # see http://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU Library or"
        " Lesser General Public License (LGPL)",
        "License :: DFSG approved",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
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
    install_requires=read_file_lines('requirements.base.txt'),
    extras_require = {
        'openstack': read_file_lines('requirements.openstack.txt'),
        'ec2':       read_file_lines('requirements.ec2.txt'),
        'daemon':    read_file_lines('requirements.daemon.txt'),
        'optimizer': read_file_lines('requirements.optimizer.txt'),
    },
    # Apparently, this list is read from right to left...
    tests_require=[
        'tox', 'mock',
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
