#! /usr/bin/env python
#
"""
This script will execute all available tests for the applications in gc3pie.gc3apps.
"""
# Copyright (C) 2012, GC3, University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
__docformat__ = 'reStructuredText'
__version__ = '$Revision$'

import os
import sys
import time
import subprocess
import logging
import shutil
import re

import cli.app

import gc3libs
from gc3libs.utils import read_contents

def make_sessiondir(basedir, name='TEST_SESSION'):
    """
    Returns an unique session dir name inside `basedir`
    """
    sessiondir = os.path.join(basedir, name)
    if os.path.exists(sessiondir):
        item = 1
        orig_sessiondir = sessiondir
        sessiondir = orig_sessiondir + '.%d' % item
        while os.path.exists(sessiondir):
            item += 1
            sessiondir = orig_sessiondir + '.%d' % item

    os.mkdir(sessiondir)
    return sessiondir

def make_log_files(basedir, name='UNKNOWN'):
    """
    Returns a pair of file descriptor (`stdout`, `stderr`) to be used
    to redirect standard output and error of the application.

    Files are in `basedir` and are named after the `name` plus
    `.stdout.log` or `.stderr.log`. In case at least one of the files
    already exists, an integer will be added to the filenames.
    """
    basefile = os.path.join(basedir, name)
    out = basefile + '.stdout.log'
    err = basefile + '.stderr.log'
    if os.path.exists(out) or os.path.exists(err):
        item = 1
        while os.path.exists(out) or os.path.exists(err):
            out = basefile + '.%d.stdout.log' % item
            err = basefile + '.%d.stderr.log' % item
            item += 1
    return (open(out, 'w'), open(err,'w'))

# Generic Test class
# ==================
class TestRunner(object):
    def __init__(self, appdir):
        """
        TO OVERRIDE

        Must set `self.args` as a list with the command line arguemnts
        to run. Please note that the extra arguments may be added to
        the list in `self.args`.

        When the `run_test()` method is called,

        If you need to run on a specific directory you can
        `os.chdir()` to it. However, the following methods will run
        with the current working directory equal to `self.appdir`:

        * `run_test()`

        * `terminate()`

        * `cleanup()`

        """
        os.chdir(appdir)
        self.args = ['/bin/true']

    def cleanup(self):
        """
        This method is called after everything is done, and it is
        supposed to cleanup any output file or directory the test
        created.

        The current working directory while this method is called is
        `self.appdir`.
        """
        pass

    def terminate(self):
        """
        This method is called after the script returned. It is
        supposed to set `self.passed` to a boolean after checking the
        output of the application.

        The current working directory while this method is called is
        `self.appdir`.
        """
        if self.proc.returncode == 0:
            self.passed = True
        else:
            self.passed = False

    def run_test(self, extra_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE):
        """
        This method will actually execute the tests. This method is
        not supposed to be overwritten.

        The current working directory while this method is called is
        `self.appdir`.
        """
        gc3libs.log.debug("Running command `%s`" % str.join(' ', self.args + list(extra_args)))
        self.proc = subprocess.Popen(str.join(' ', self.args + list(extra_args)),
                                     stdout=stdout,
                                     stderr=stderr,
                                     shell=True)

    def __str__(self):
        return str.join(" ", self.args)


class GCodemlTest(TestRunner):
    def __str__(self):
        return "GCodeml"

    def __init__(self, appdir):
        """
        Run gcodeml test
        """
        self.testdir = os.path.join(appdir, 'test')

        self.sessiondir = make_sessiondir(self.testdir)
        os.chdir(self.testdir)
        self.args = ['../gcodeml.py',
                     '-s', self.sessiondir,
                     '-C', '45',
                     'data/small_flat']
        self.datadir = os.path.join(self.testdir, 'data/small_flat')
        self.outputdir = os.path.join(self.datadir, 'small_flat.out')

    def cleanup(self):
        for d in (self.outputdir, self.sessiondir):
            if os.path.exists(d):
                gc3libs.log.info("GCodeml: removing directory %s" % d)
                shutil.rmtree(d)

    def terminate(self):
        if not os.path.isdir(self.outputdir):
            gc3libs.log.error("GCodeml: No output directory `%s` found" % self.outputdir)
            self.passed = False
            return

        for ctlfile in os.listdir(self.datadir):
            if ctlfile.endswith('.ctl'):
                outfile = os.path.join(self.outputdir, ctlfile[:-3]+'mlc')
                if not os.path.exists(outfile):
                    gc3libs.log.error("GCodeml: No output file %s" % outfile)
                    self.passed = False
                    return
                fd = open(outfile)
                output = fd.readlines()
                fd.close()
                if not output or not output[-1].startswith("Time used:"):
                    gc3libs.log.error("GCodeml: Error in output file `%s`" % outfile)
                    self.passed = False
                    return

        self.passed = True

class GGamessTest(TestRunner):
    def __str__(self):
        return "GGamess"

    def __init__(self, appdir):
        self.testdir = os.path.join(appdir, 'test')
        self.examdir = os.path.join(self.testdir, 'exam01')
        if os.path.exists(self.examdir):
            shutil.rmtree(self.examdir)

        self.sessiondir = make_sessiondir(self.testdir)
        os.chdir(self.testdir)
        self.args = ['../ggamess.py',
                     '-s', self.sessiondir,
                     '-C', '45',
                     '-R', '2012R1',
                     'data/exam01.inp']

    def cleanup(self):
        for d in (self.examdir, self.sessiondir):
            if os.path.exists(d):
                gc3libs.log.info("GGamess: removing directory %s" % d)
                shutil.rmtree(d)

    def terminate(self):
        """
        Parse the output and clean up the directory
        """
        examfile = os.path.join(self.examdir, 'exam01.out')
        if not os.path.isfile(examfile):
            self.passed = False
        stdout = read_contents(examfile)

        if re.match('.*\n (EXECUTION OF GAMESS TERMINATED NORMALLY).*', stdout, re.M|re.S):
            self.passed = True
        else:
            gc3libs.log.error("GGamess: Output file %s does not match success regexp" % examfile)
            self.passed = False


class GRosettaTest(TestRunner):
    def __str__(self):
        return "GRosetta"

    def __init__(self, appdir):
        self.testdir = os.path.join(appdir, 'test')
        self.sessiondir = make_sessiondir(self.testdir)
        os.chdir(self.testdir)

        self.args = ['../grosetta.py',
                     '-s', self.sessiondir,
                     '-C', '45', '-vvv',
                     '--total-decoys', '5',
                     '--decoys-per-job', '2',
                     'data/grosetta.flags',
                     'data/alignment.filt',
                     'data/boinc_aaquery0*',
                     'data/query.*',
                     'data/*.pdb',
                     ]
        self.jobdirs = [os.path.join(self.testdir, d) for d in ('0--1', '2--3', '4--5')]

    def cleanup(self):
        for d in self.jobdirs + [self.sessiondir]:
            if os.path.exists(d):
                gc3libs.log.debug("GRosetta: removing directory %s" % d)
                shutil.rmtree(d)

    def terminate(self):
        for jobdir in self.jobdirs:
            if not os.path.isdir(jobdir):
                gc3libs.log.error("GRosetta: missing job directory %s" % jobdir)
                self.passed = False
                return
            outfile = os.path.join(jobdir, 'minirosetta.static.stdout.txt')

            if not os.path.isfile(outfile):
                gc3libs.log.error("GRosetta: missing output file %s" % outfile)
                self.passed = False
                return
            fd = open(outfile)
            output = fd.readlines()
            fd.close()
            try:
                if not output or output[-1] != 'minirosetta.static: All done, exitcode: 0':
                    gc3libs.log.error("GRosetta: expecting different output in `%s`. Last line is: `%s`" % (outfile, output[-1]))
                    self.passed = False
                    return
            except Exception, ex:
                gc3libs.log.error(
                    "GRosetta: Error while checking exit status of `grosetta`: %s" % str(ex))
                self.passed = False
                raise ex
        self.passed = True


class GDockingTest(TestRunner):
    def __str__(self):
        return "GDocking"

    def __init__(self, appdir):
        self.testdir = os.path.join(appdir, 'test')
        self.sessiondir = make_sessiondir(self.testdir)
        os.chdir(self.testdir)

        self.args = ['../gdocking.py',
                     '-s', self.sessiondir,
                     '-C', '45',
                     '--decoys-per-file', '5',
                     '--decoys-per-job', '2',
                     '-f', 'data/gdocking.flags',
                     'data/1bjpA.pdb',
                     ]
        self.jobdirs = [os.path.join(self.testdir, "1bjpA.%s" % d) for d in ('1--2', '3--4', '5--5')]

    def cleanup(self):
        for d in self.jobdirs + [self.sessiondir]:
            if os.path.exists(d):
                gc3libs.log.debug("GRosetta: removing directory %s" % d)
                shutil.rmtree(d)

    def terminate(self):
        for jobdir in self.jobdirs:
            if not os.path.isdir(jobdir):
                gc3libs.log.error("GDocking: missing job directory %s" % jobdir)
                self.passed = False
                return
            outfile = os.path.join(jobdir, 'docking_protocol.stdout.txt')

            if not os.path.isfile(outfile):
                gc3libs.log.error("GDocking: missing output file %s" % outfile)
                self.passed = False
                return
        self.passed = True


class GGeotopTest(TestRunner):
    def __str__(self):
        return "GGeotop"

    def __init__(self, appdir):
        self.testdir = os.path.join(appdir, 'test')
        self.sessiondir = make_sessiondir(self.testdir)
        os.chdir(self.testdir)

        self.args = ['../ggeotop.py',
                     '-s', self.sessiondir,
                     '-C', '45',
                     '-vvv',
                     '-x', 'geotop_1_224_20120227_static',
                     'data/GEOtop_public_test',
                     ]


class GCryptoTest(TestRunner):
    def __str__(self):
        return "GCrypto"


class GMhCoevTest(TestRunner):
    def __str__(self):
        return "Gmh_coev"


class GZodsTest(TestRunner):
    def __str__(self):
        return "GZods"

    def __init__(self, appdir):
        self.testdir = os.path.join(appdir, 'test')
        self.sessiondir = make_sessiondir(self.testdir)
        os.chdir(self.testdir)

        self.args = ['../gzods.py',
                     '-s', self.sessiondir,
                     '-C', '45',
                     '-vvv',
                     'data/small',
                     ]


## main: run tests
applicationdirs = {
    # 'bf.uzh.ch': None,
    'codeml': (GCodemlTest,),
    # 'compchem.unipg.it': None,
    'gamess': (GGamessTest,),
    # 'gc3.uzh.ch': None,
    'geotop': (GGeotopTest, ),
    # 'ieu.uzh.ch': (GMhCoevTest, ),
    # 'imsb.ethz.ch': None,
    # 'ior.uzh.ch': None,
    # 'lacal.epfl.ch': (GCryptoTest, ),
    'rosetta': (GRosettaTest, GDockingTest),
    # 'turbomole': None,
    'zods': (GZodsTest, ),
    }


class RunTests(cli.app.CommandLineApp):
    """
    Run tests inside gc3apps.
    """
    def setup(self):
        cli.app.CommandLineApp.setup(self)
        self.add_param('-r', '--resource', metavar='RESOURCE',
                       help="Resource, string identifying the name of the resource to use.")

        self.add_param('args',
                       nargs='*',
                       metavar='TESTS',
                       default=[test for test in applicationdirs.keys() if applicationdirs[test]],
                       help="List of tests to run")

        self.add_param('-d', '--delay', metavar='TIME',
                       default=60,
                       type=int,
                       help="Seconds to sleep between two check.",)

        self.add_param('--no-cleanup', dest='cleanup',
                       default=True,
                       action='store_false',
                       help="Do not attempt to cleanup after the tests have finished.",
                       )

        self.add_param('-v', '--verbose',
                       action='count',
                       dest='verbose',
                       default=0,
                       help="Print more detailed information about the program's activity."
                       " Increase verbosity each time this option is encountered on the"
                       " command line."
                       )

    def pre_run(self):
        cli.app.CommandLineApp.pre_run(self)

        self.params.args = [i.rstrip('/') for i in self.params.args]
        self.applicationdirs = {}
        self.extra_args = []
        self.running = []
        self.finished = []

        loglevel = max(1, logging.ERROR - 10 * max(0, self.params.verbose))
        logging.basicConfig(format='GC3Pie test runner: [%(asctime)s] %(levelname)-8s: %(message)s',
                            datefmt='%b %d %H:%M:%S')

        gc3libs.log.setLevel(loglevel)
        for path in self.params.args:
            if not os.path.exists(path):
                gc3libs.log.error("Error: path %s not found" % path)
            elif not path in applicationdirs:
                gc3libs.log.error("Error: test not found for path %s" % path)
            else:
                self.applicationdirs[path] = applicationdirs[path]

        if self.params.resource:
            self.extra_args += ['-r', self.params.resource]

    def main(self):
        retvalue = 0

        parentdir = os.getcwd()
        for appdir, clss in self.applicationdirs.iteritems():
            if not clss: continue
            appdir = os.path.abspath(appdir)

            for cls in clss:
                try:
                    os.chdir(appdir)
                    app = cls(appdir)
                    gc3libs.log.info("Running test %s on dir `%s`" % (app, appdir))
                    baselog = os.path.join(parentdir,
                                               os.path.basename(sys.argv[0]).replace('.py','') + '.' + os.path.basename(str(app).lower()))
                    stdout, stderr = make_log_files(parentdir, str(app))
                    app.run_test(self.extra_args, stdout=stdout, stderr=stderr)
                    app.appdir = appdir
                    self.running.append(app)
                except Exception, ex:
                    gc3libs.log.error("%s: %s" % (app, ex))
                finally:
                    os.chdir(parentdir)

        while self.running:
            for app in self.running:
                if app.proc.poll() is not None:
                    self.running.remove(app)
                    self.finished.append(app)
                    # Terminate application
                    try:
                        os.chdir(app.appdir)
                        gc3libs.log.debug("Calling `terminate()` method of application %s" % app)
                        app.terminate()
                    except Exception, ex:
                        gc3libs.log.error(
                            "Error while calling `terminate()` method of application %s: %s" % (app, str(ex)))
                    finally:
                        os.chdir(parentdir)
                    gc3libs.log.info("Application %s terminated with return code `%d`" % (app, app.proc.returncode))
                    if app.passed:
                        gc3libs.log.info("Application %s PASSED the tests" % str(app))
                    else:
                        gc3libs.log.info("Application %s DID NOT PASS the tests" % str(app))

            if not self.running: break
            gc3libs.log.debug("Sleeping %d seconds" % self.params.delay)
            time.sleep(self.params.delay)

        gc3libs.log.info("All applications have finished")
        for app in self.finished:
            print "Application:    %s" % app
            print "  Return code:  %d" % (app.proc.returncode)
            print "  Test passed?: %s" % app.passed
            if not app.passed:
                retvalue += 1

            if self.params.cleanup:
                try:
                    os.chdir(app.appdir)
                    app.cleanup()
                except Exception, ex:
                    gc3libs.log.error(
                        "Error while calling `cleanup()` method in %s application: %s" % (app, str(ex)))
                finally:
                    os.chdir(parentdir)
        return retvalue
if "__main__" == __name__:
    from run_test_apps import RunTests
    RunTests().run()
