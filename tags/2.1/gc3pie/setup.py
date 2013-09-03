#!/usr/bin/env python

# See http://packages.python.org/distribute/setuptools.html for details
from distribute_setup import use_setuptools
use_setuptools()

def read_whole_file(path):
    stream = open(path, 'r')
    text = stream.read()
    stream.close
    return text


# See http://tox.readthedocs.org/en/latest/example/basic.html#integration-with-setuptools-distribute-test-commands
# on how to run tox when python setup.py test is run
from setuptools.command.test import test as TestCommand
import sys

class Tox(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import tox
        errno = tox.cmdline(self.test_args)
        sys.exit(errno)


# see http://peak.telecommunity.com/DevCenter/setuptools
# for an explanation of the keywords and syntax of this file.
#
import setuptools
import setuptools.dist
# avoid setuptools including `.svn` directories into the PyPI package
from setuptools.command import sdist
del sdist.finders[:]

setuptools.setup(
    name = "gc3pie",
    version = '2.1', # see: http://packages.python.org/distribute/setuptools.html

    packages = setuptools.find_packages(exclude=['ez_setup']),
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
            'gcloud = gc3utils.frontend:main',
            ],
       },

    # run-time dependencies
    install_requires = [
        # Needed for the EC2 backend.  This would be an optional
        # dependency, but we're showcasing the new cloud support in
        # 2.1, so...
        'boto',
        # paramiko and pycrypto are required for SSH operations
        # ("pycrypto" is actually a dependency of Paramiko, but
        # setuptools apparently does not process dependencies recursively)
        'paramiko==1.7.7.2', 'pycrypto==2.6',
        # lockfile dropped support for Python 2.4 in 0.9
        'lockfile',
        # prettytable -- format tabular text output
        'prettytable',
        # pyCLI -- object-oriented command-line app programming
        'pyCLI==2.0.3',
        # Needed by SqlStore; 0.8.0 is compatible with all Pythons
        # >=2.5 according to http://docs.sqlalchemy.org/en/rel_0_8/intro.html#installation
        'sqlalchemy==0.8.0',
        # Needed for parsing human-readable dates (gselect uses it).
        'parsedatetime==0.8.7',
        # needed by Benjamin's DE optimizer code
        # To add as an *optional* dependency
        # 'numpy',
        ],
    # Apparently, this list is read from right to left...
    tests_require = ['tox'],
    cmdclass = {'test': Tox},
    # additional non-Python files to be bundled in the package
    package_data = {
        'gc3libs': [
            'etc/codeml.pl',
            'etc/gc3pie.conf.example',
            'etc/logging.conf.example',
            'etc/rosetta.sh',
            ],
    },
    data_files = [
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
            ]),
    ],
    # `zip_safe` can ease deployment, but is only allowed if the package
    # do *not* do any __file__/__path__ magic nor do they access package data
    # files by file name (use `pkg_resources` instead).
    zip_safe = True,
)
