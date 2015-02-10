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
    version = '2.0.0-rc3', # see: http://packages.python.org/distribute/setuptools.html

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
    include_package_data = False,
    package_data = {
        'gc3libs': [
            'etc/codeml.pl',
            'etc/gc3pie.conf.example',
            'etc/logging.conf.example',
            'etc/rosetta.sh',
            ],
    },
    # (but exclude these files ...
    exclude_package_data = {
        # ... from all packages)
        '': [
            '.bzrignore',
            'gc3apps/zods/test/',
            'gc3apps/zods/test/data/',
            'gc3apps/zods/test/data/small/',
            'gc3apps/zods/test/data/small/californium_simple_3.cif',
            'gc3apps/zods/test/data/small/data.xml',
            'gc3apps/zods/test/data/small/input.xml',
            'gc3apps/zods/test/data/large/',
            'gc3apps/zods/test/data/large/6/',
            'gc3apps/zods/test/data/large/6/input.xml',
            'gc3apps/zods/test/data/large/6/ref.xml',
            'gc3apps/zods/test/data/large/6/nalaf4_big.cif',
            'gc3apps/zods/test/data/large/3/',
            'gc3apps/zods/test/data/large/3/ref_int.xml',
            'gc3apps/zods/test/data/large/3/input.xml',
            'gc3apps/zods/test/data/large/3/hb2966.cif',
            'gc3apps/zods/test/data/large/4/',
            'gc3apps/zods/test/data/large/4/data.xml',
            'gc3apps/zods/test/data/large/4/input.xml',
            'gc3apps/zods/test/data/large/4/cu.cif',
            'gc3apps/zods/test/data/large/2/',
            'gc3apps/zods/test/data/large/2/input.xml',
            'gc3apps/zods/test/data/large/2/cuau_m.an',
            'gc3apps/zods/test/data/large/2/ref.xml',
            'gc3apps/zods/test/data/large/2/x.cif',
            'gc3apps/zods/test/data/large/5/',
            'gc3apps/zods/test/data/large/5/data.xml',
            'gc3apps/zods/test/data/large/5/input.xml',
            'gc3apps/zods/test/data/large/5/nalaf4_big.cif',
            'gc3apps/zods/test/data/large/1/',
            'gc3apps/zods/test/data/large/1/input.xml',
            'gc3apps/zods/test/data/large/1/ref.xml',
            'gc3apps/zods/test/data/large/1/nalaf4_big.cif',
            'gc3apps/zods/test/README.txt',
            'gc3apps/geotop/test/',
            'gc3apps/geotop/test/geotop_1_224_20120227_static',
            'gc3apps/geotop/test/data/',
            'gc3apps/geotop/test/data/GEOtop_public_test/',
            'gc3apps/geotop/test/data/GEOtop_public_test/geotop.inpts',
            'gc3apps/geotop/test/data/GEOtop_public_test/in/',
            'gc3apps/geotop/test/data/GEOtop_public_test/in/meteo0001.txt.old',
            'gc3apps/geotop/test/data/GEOtop_public_test/in/meteo0002.txt',
            'gc3apps/geotop/test/data/GEOtop_public_test/in/meteo0002.txt.old',
            'gc3apps/geotop/test/data/GEOtop_public_test/in/listpoints.txt',
            'gc3apps/geotop/test/data/GEOtop_public_test/in/meteo0001.txt',
            'gc3apps/geotop/test/data/GEOtop_public_test/out/',
            'gc3apps/geotop/test/data/GEOtop_public_test/out/.git_keep_empty_dir',
            'gc3apps/geotop/test/README.txt',
            'gc3apps/rosetta/test/',
            'gc3apps/rosetta/test/data/',
            'gc3apps/rosetta/test/data/2ormA.pdb',
            'gc3apps/rosetta/test/data/1fimA.pdb',
            'gc3apps/rosetta/test/data/1uizA.pdb',
            'gc3apps/rosetta/test/data/1otgA.pdb',
            'gc3apps/rosetta/test/data/2op8A.pdb',
            'gc3apps/rosetta/test/data/1gyjA.pdb',
            'gc3apps/rosetta/test/data/1mifA.pdb',
            'gc3apps/rosetta/test/data/1dptA.pdb',
            'gc3apps/rosetta/test/data/1gifA.pdb',
            'gc3apps/rosetta/test/data/1u9dA.pdb',
            'gc3apps/rosetta/test/data/alignment.filt',
            'gc3apps/rosetta/test/data/1ca7A.pdb',
            'gc3apps/rosetta/test/data/query.fasta',
            'gc3apps/rosetta/test/data/query.psipred_ss2',
            'gc3apps/rosetta/test/data/1hfoA.pdb',
            'gc3apps/rosetta/test/data/1mffA.pdb',
            'gc3apps/rosetta/test/data/1gczA.pdb',
            'gc3apps/rosetta/test/data/1mfiA.pdb',
            'gc3apps/rosetta/test/data/gdocking.flags',
            'gc3apps/rosetta/test/data/3c6vA.pdb',
            'gc3apps/rosetta/test/data/2aalA.pdb',
            'gc3apps/rosetta/test/data/1s0yA.pdb',
            'gc3apps/rosetta/test/data/1otfA.pdb',
            'gc3apps/rosetta/test/data/2os5A.pdb',
            'gc3apps/rosetta/test/data/2fltA.pdb',
            'gc3apps/rosetta/test/data/1bjpA.pdb',
            'gc3apps/rosetta/test/data/2aagA.pdb',
            'gc3apps/rosetta/test/data/score.sc',
            'gc3apps/rosetta/test/data/boinc_aaquery03_05.200_v1_3.gz',
            'gc3apps/rosetta/test/data/2fm7A.pdb',
            'gc3apps/rosetta/test/data/1mwwA.pdb',
            'gc3apps/rosetta/test/data/1p1gA.pdb',
            'gc3apps/rosetta/test/data/boinc_aaquery09_05.200_v1_3.gz',
            'gc3apps/rosetta/test/data/2aajA.pdb',
            'gc3apps/rosetta/test/data/grosetta.flags',
            'gc3apps/rosetta/test/data/1cgqA.pdb',
            'gc3apps/rosetta/test/README.txt',
            'gc3apps/bf.uzh.ch/test/',
            'gc3apps/bf.uzh.ch/test/makePlots.py',
            'gc3apps/bf.uzh.ch/test/idRisk.py',
            'gc3apps/bf.uzh.ch/test/base/',
            'gc3apps/bf.uzh.ch/test/base/lucasOneAgent.py',
            'gc3apps/bf.uzh.ch/test/base/input/',
            'gc3apps/bf.uzh.ch/test/base/input/gamma.in',
            'gc3apps/bf.uzh.ch/test/base/input/parameters.in',
            'gc3apps/bf.uzh.ch/test/base/genMarkov.py',
            'gc3apps/bf.uzh.ch/test/gidRiskParaSearchUML.py',
            'gc3apps/bf.uzh.ch/test/createTable.py',
            'gc3apps/bf.uzh.ch/test/pymods/',
            'gc3apps/bf.uzh.ch/test/pymods/__init__.py',
            'gc3apps/bf.uzh.ch/test/pymods/loop/',
            'gc3apps/bf.uzh.ch/test/pymods/loop/loop.py',
            'gc3apps/bf.uzh.ch/test/pymods/loop/__init__.py',
            'gc3apps/bf.uzh.ch/test/pymods/loop/loopPy2.py',
            'gc3apps/bf.uzh.ch/test/pymods/markovChain/',
            'gc3apps/bf.uzh.ch/test/pymods/markovChain/momentMatching.py',
            'gc3apps/bf.uzh.ch/test/pymods/markovChain/MkovM.py',
            'gc3apps/bf.uzh.ch/test/pymods/markovChain/__init__.py',
            'gc3apps/bf.uzh.ch/test/pymods/markovChain/johannes1987.py',
            'gc3apps/bf.uzh.ch/test/pymods/markovChain/GHQUAD.DAT',
            'gc3apps/bf.uzh.ch/test/pymods/markovChain/markov64.so',
            'gc3apps/bf.uzh.ch/test/pymods/markovChain/mcInterface.py',
            'gc3apps/bf.uzh.ch/test/pymods/markovChain/markov.so',
            'gc3apps/bf.uzh.ch/test/pymods/markovChain/knotekTerry.py',
            'gc3apps/bf.uzh.ch/test/pymods/classes/',
            'gc3apps/bf.uzh.ch/test/pymods/classes/__init__.py',
            'gc3apps/bf.uzh.ch/test/pymods/classes/tableDict.py',
            'gc3apps/bf.uzh.ch/test/pymods/plotting/',
            'gc3apps/bf.uzh.ch/test/pymods/plotting/__init__.py',
            'gc3apps/bf.uzh.ch/test/pymods/plotting/gplot.py',
            'gc3apps/bf.uzh.ch/test/pymods/plotting/demo-gplot.py',
            'gc3apps/bf.uzh.ch/test/pymods/support/',
            'gc3apps/bf.uzh.ch/test/pymods/support/wrapLogbook.py',
            'gc3apps/bf.uzh.ch/test/pymods/support/getopts.py',
            'gc3apps/bf.uzh.ch/test/pymods/support/log.py',
            'gc3apps/bf.uzh.ch/test/pymods/support/__init__.py',
            'gc3apps/bf.uzh.ch/test/pymods/support/supportPy2.py',
            'gc3apps/bf.uzh.ch/test/pymods/support/support.py',
            'gc3apps/bf.uzh.ch/test/pymods/support/getIndexPy2.py',
            'gc3apps/bf.uzh.ch/test/pymods/support/writer.py',
            'gc3apps/bf.uzh.ch/test/pymods/support/log2.py',
            'gc3apps/bf.uzh.ch/test/pymods/support/getIndex.py',
            'gc3apps/bf.uzh.ch/test/pymods/support/driver.py',
            'gc3apps/bf.uzh.ch/test/pymods/support/log3.py',
            'gc3apps/bf.uzh.ch/test/costlyOptimization.py',
            'gc3apps/bf.uzh.ch/test/idRiskOut',
            'gc3apps/bf.uzh.ch/test/README.txt',
            'gc3apps/bf.uzh.ch/test/gammaLoop/',
            'gc3apps/bf.uzh.ch/test/gammaLoop/para.loop',
            'gc3apps/bf.uzh.ch/test/gammaLoop/optimalRuns',
            'gc3apps/bf.uzh.ch/test/gammaLoop/localBaseDir/',
            'gc3apps/bf.uzh.ch/test/gammaLoop/localBaseDir/lucasOneAgent.py',
            'gc3apps/bf.uzh.ch/test/gammaLoop/localBaseDir/input/',
            'gc3apps/bf.uzh.ch/test/gammaLoop/localBaseDir/input/gamma.in',
            'gc3apps/bf.uzh.ch/test/gammaLoop/localBaseDir/input/parameters.in',
            'gc3apps/bf.uzh.ch/test/gammaLoop/localBaseDir/genMarkov.py',
            'gc3apps/codeml/test/',
            'gc3apps/codeml/test/data/',
            'gc3apps/codeml/test/data/midsize_hierarchical/',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_1/',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_1/FAM_1.1/',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_1/FAM_1.1/FAM_1.1.H1.ctl',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_1/FAM_1.1/FAM_1.1.phy',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_1/FAM_1.1/FAM_1.1.H0.ctl',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_1/FAM_1.1/FAM_1.1.nwk',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_1/FAM_1.2/',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_1/FAM_1.2/FAM_1.2.H1.ctl',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_1/FAM_1.2/FAM_1.2.H0.ctl',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_1/FAM_1.2/FAM_1.2.phy',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_1/FAM_1.2/FAM_1.2.nwk',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_2/',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_2/FAM_2.1/',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_2/FAM_2.1/FAM_2.1.H0.ctl',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_2/FAM_2.1/FAM_2.1.nwk',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_2/FAM_2.1/FAM_2.1.H1.ctl',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_2/FAM_2.1/FAM_2.1.phy',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_2/FAM_2.2/',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_2/FAM_2.2/FAM_2.2.nwk',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_2/FAM_2.2/FAM_2.2.phy',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_2/FAM_2.2/FAM_2.2.H0.ctl',
            'gc3apps/codeml/test/data/midsize_hierarchical/FAM_2/FAM_2.2/FAM_2.2.H1.ctl',
            'gc3apps/codeml/test/data/small_flat/',
            'gc3apps/codeml/test/data/small_flat/HBG003812-1.H1.ctl',
            'gc3apps/codeml/test/data/small_flat/HBG003812-1.phy',
            'gc3apps/codeml/test/data/small_flat/HBG003812-1.xrsl',
            'gc3apps/codeml/test/data/small_flat/HBG003812-1.H0.ctl',
            'gc3apps/codeml/test/data/small_flat/HBG003812-1.nwk',
            'gc3apps/codeml/test/README.txt',
            'gc3apps/gamess/test/',
            'gc3apps/gamess/test/gmsPatternObsolete.py',
            'gc3apps/gamess/test/data/',
            'gc3apps/gamess/test/data/exam05.inp',
            'gc3apps/gamess/test/data/exam21.inp',
            'gc3apps/gamess/test/data/exam14.inp',
            'gc3apps/gamess/test/data/exam44.inp',
            'gc3apps/gamess/test/data/exam40.inp',
            'gc3apps/gamess/test/data/exam34.inp',
            'gc3apps/gamess/test/data/exam38.inp',
            'gc3apps/gamess/test/data/exam24.inp',
            'gc3apps/gamess/test/data/exam25.inp',
            'gc3apps/gamess/test/data/exam06.inp',
            'gc3apps/gamess/test/data/exam31.inp',
            'gc3apps/gamess/test/data/exam37.inp',
            'gc3apps/gamess/test/data/exam41.inp',
            'gc3apps/gamess/test/data/exam43.inp',
            'gc3apps/gamess/test/data/exam13.inp',
            'gc3apps/gamess/test/data/exam28.inp',
            'gc3apps/gamess/test/data/exam32.inp',
            'gc3apps/gamess/test/data/exam42.inp',
            'gc3apps/gamess/test/data/exam10.inp',
            'gc3apps/gamess/test/data/exam11.inp',
            'gc3apps/gamess/test/data/README.txt',
            'gc3apps/gamess/test/data/exam19.inp',
            'gc3apps/gamess/test/data/exam16.inp',
            'gc3apps/gamess/test/data/exam07.inp',
            'gc3apps/gamess/test/data/exam27.inp',
            'gc3apps/gamess/test/data/exam36.inp',
            'gc3apps/gamess/test/data/exam15.inp',
            'gc3apps/gamess/test/data/exam35.inp',
            'gc3apps/gamess/test/data/exam33.inp',
            'gc3apps/gamess/test/data/exam30.inp',
            'gc3apps/gamess/test/data/exam39.inp',
            'gc3apps/gamess/test/data/exam04.inp',
            'gc3apps/gamess/test/data/exam01.inp',
            'gc3apps/gamess/test/data/exam03.inp',
            'gc3apps/gamess/test/data/exam12.inp',
            'gc3apps/gamess/test/data/exam09.inp',
            'gc3apps/gamess/test/data/exam26.inp',
            'gc3apps/gamess/test/data/exam17.inp',
            'gc3apps/gamess/test/data/exam02.inp',
            'gc3apps/gamess/test/data/exam08.inp',
            'gc3apps/gamess/test/data/exam20.inp',
            'gc3apps/gamess/test/data/exam18.inp',
            'gc3apps/gamess/test/data/exam29.inp',
            'gc3apps/gamess/test/data/exam23.inp',
            'gc3apps/gamess/test/data/exam22.inp',
            'gc3apps/gamess/test/README.txt',
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
