#! /usr/bin/env python
#
"""
This script will run all configured tests for directories in
``gc3pie/gc3apps``.

A valid test is a class that inherits both from `TestRunner`:class and
`Task`:class classes. Each test is associated to a directory (which is
supposed to contain a `test` subdir) and a `SessionBasedScript`:class that
accepts the following standard options:

  -s SESSION
      the session dir to use.

  -r RESOURCE
      the remote resource to use (corresponds to the `-R` option of
      the script).

  -C 45
      to configure the polling frequency.

  -vvv
      to increase verbosity.

A test is therefore an application that is run in a
`ParallelTaskCollection` on the localhost, and that executes a gc3
script to submit the tests to a remote resource.

The signature of the `__init__` method of the test class must be::

    def __init__(self, appdir, **extra_args):

where `appdir` is the application directory
(e.g. ``gc3pie/gc3apps/rosetta``).

After a test is completed, the script will assume that the
`terminated()` method of the test had set the `self.passed` attribute
to a boolean, with the obvious meaning.

For each directory multiple tests can be defined. However, different
tests with different arguments must be different classes.

In order to *enable* one or more tests for a specific directory you
have to update the `RunTestsInParallel.applicationdirs` dictionary and
add a mapping `directory` => `(Test1, Test2, Test3)`
"""
# Copyright (C) 2012  University of Zurich. All rights reserved.
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

from __future__ import absolute_import, print_function
import os
import re

import gc3libs
from gc3libs import Application, Task
from gc3libs.utils import read_contents
from gc3libs.cmdline import SessionBasedScript
from gc3libs.workflow import ParallelTaskCollection, TaskCollection
from gc3libs.backends.shellcmd import ShellcmdLrms
from gc3libs.backends.transport import LocalTransport


class TestRunner(object):
    """
    This class is used to initialize some commonly used attributes and
    values of the `**extra` arguments passed by the
    `SessionBasedScript`.
    """
    def __init__(self, appdir, kw):
        self.appdir = os.path.abspath(appdir)
        self.testdir = os.path.join(self.appdir, 'test')
        self.passed = False
        kw['environment'] = {'PYTHONUNBUFFERED': 'yes'}
        kw['output_dir'] = os.path.join(
            kw.get('output_dir', ''), os.path.basename(appdir))
        kw['stdout'] = str(self).lower()+'.stdout.log'
        kw['stderr'] = str(self).lower()+'.stderr.log'
        self.stdargs = ['-s', 'TEST_SESSION',
                        '-r', kw['resource'],
                        '-C', '45',
                        '-vvvv']

    def compatible_resources(self, resources):
        shellcmd = [r for r in resources if isinstance(r, ShellcmdLrms)]
        shellcmd_local = [r for r in shellcmd if
                          isinstance(r.transport, LocalTransport)]
        if shellcmd_local:
            return shellcmd_local
        else:
            return shellcmd


class GCodemlTest(TestRunner, Application):
    def __str__(self):
        return "GCodeml"

    def __init__(self, appdir, **kw):
        TestRunner.__init__(self, appdir, kw)
        self.datadir = os.path.join(self.testdir, 'data/small_flat')
        Application.__init__(
            self,
            arguments=['./gcodeml.py'] +
            self.stdargs + ['small_flat'],
            inputs=[
                os.path.join(self.appdir, 'gcodeml.py'),
                self.datadir],
            outputs=['small_flat.out'],
            **kw)

    def terminated(self):
        self.outdir = os.path.join(self.output_dir, 'small_flat.out')

        if not os.path.isdir(self.outdir):
            gc3libs.log.error("GCodeml: No output directory `%s` found",
                              self.outdir)
            self.passed = False
            return

        for ctlfile in os.listdir(self.datadir):
            if ctlfile.endswith('.ctl'):
                outfile = os.path.join(self.outdir, ctlfile[:-3]+'mlc')
                if not os.path.exists(outfile):
                    gc3libs.log.error("GCodeml: No output file %s",
                                      outfile)
                    self.passed = False
                    return
                fd = open(outfile)
                output = fd.readlines()
                fd.close()
                if not output or not output[-1].startswith("Time used:"):
                    gc3libs.log.error("GCodeml: Error in output file `%s`",
                                      outfile)
                    self.passed = False
                    return

        self.passed = True


class GGamessTest(TestRunner, Application):
    def __str__(self):
        return "GGamess"

    def __init__(self, appdir, **kw):
        TestRunner.__init__(self, appdir, kw)

        Application.__init__(
            self,
            arguments=['./ggamess.py'] + self.stdargs + ['-R', '2012R1',
                                                         'exam01.inp'],
            inputs=[os.path.join(self.appdir, 'ggamess.py'),
                    os.path.join(self.appdir, 'test/data/exam01.inp')],
            outputs=['exam01'],
            **kw)

    def terminated(self):
        """
        Parse the output
        """
        examfile = os.path.join(self.output_dir, 'exam01', 'exam01.out')
        if not os.path.isfile(examfile):
            self.passed = False
        stdout = read_contents(examfile)

        if re.match('.*\n (EXECUTION OF GAMESS TERMINATED NORMALLY).*',
                    stdout, re.M | re.S):
            self.passed = True
        else:
            gc3libs.log.error(
                "GGamess: Output file %s does not match success regexp",
                examfile)
            self.passed = False


class GGeotopTest(TestRunner, Application):
    def __str__(self):
        return "GGeotop"

    def __init__(self, appdir, **kw):
        TestRunner.__init__(self, appdir, kw)
        Application.__init__(
            self,
            arguments=['./ggeotop.py'] + self.stdargs \
                + ['-x', 'geotop_1_224_20120227_static',
                   'GEOtop_public_test'],
            inputs=[
                os.path.join(self.appdir, name) for name in
                ['ggeotop.py',
                 'test/geotop_1_224_20120227_static',
                 'test/data/GEOtop_public_test']],
            outputs=['GEOtop_public_test/out'],
            **kw)

    def terminated(self):
        self.passed = os.path.isdir(os.path.join(self.output_dir, 'out'))


class GZodsTest(TestRunner, Application):
    def __str__(self):
        return "GZods"

    def __init__(self, appdir, **kw):
        TestRunner.__init__(self, appdir, kw)
        Application.__init__(
            self,
            arguments=['./gzods.py'] + self.stdargs + ['small'],
            inputs=[
                os.path.join(self.appdir, i) for i in [
                    'gzods.py', 'test/data/small']],
            outputs=['small', 'input'],
            **kw)

    def terminated(self):
        if self.execution._exitcode == 0:
            self.passed = True


class GRosettaTest(TestRunner, Application):
    def __str__(self):
        return "GRosetta"

    def __init__(self, appdir, **kw):
        TestRunner.__init__(self, appdir, kw)
        self.jobdirs = ['0--1', '2--3', '4--5']
        kw['output_dir'] = os.path.join(
            os.path.dirname(kw['output_dir']), 'rosetta')

        Application.__init__(
            self,
            arguments=['./grosetta.py'] + self.stdargs \
                + ['--total-decoys', '5',
                   '--decoys-per-job', '2',
                   'data/grosetta.flags',
                   'data/alignment.filt',
                   'data/boinc_aaquery0*',
                   'data/query.*',
                   'data/*.pdb',
                   ],
            inputs=[os.path.join(self.appdir, 'grosetta.py'),
                    os.path.join(self.appdir, 'test/data')],
            outputs=self.jobdirs + ['data'],
            **kw)

    def terminated(self):
        for jobdir in self.jobdirs:
            if not os.path.isdir(jobdir):
                gc3libs.log.error("GRosetta: missing job directory %s", jobdir)
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
                if not output or output[-1] != \
                        'minirosetta.static: All done, exitcode: 0':
                    gc3libs.log.error(
                        "GRosetta: expecting different output in `%s`. "
                        "Last line is: `%s`", outfile, output[-1])
                    self.passed = False
                    return
            except Exception, ex:
                gc3libs.log.error(
                    "GRosetta: Error while checking exit status of `grosetta`:"
                    " %s", str(ex))
                self.passed = False
                raise ex
        self.passed = True


class GDockingTest(TestRunner, Application):
    def __str__(self):
        return "GDocking"

    def __init__(self, appdir, **kw):
        TestRunner.__init__(self, appdir, kw)
        kw['output_dir'] = os.path.join(
            os.path.dirname(kw['output_dir']), 'docking')

        self.jobdirs = ["1bjpA.%s" % d for d in ('1--2', '3--4', '5--5')]
        Application.__init__(
            self,
            arguments=['./gdocking.py'] + self.stdargs \
                + ['--decoys-per-file', '5',
                   '--decoys-per-job', '2',
                   '-f', 'data/gdocking.flags',
                   'data/1bjpA.pdb',
                   ],
            inputs=[
                os.path.join(self.appdir, 'gdocking.py'),
                os.path.join(self.appdir, 'test/data')],
            outputs=[self.jobdirs],
            **kw)

    def terminated(self):
        for jobdir in self.jobdirs:
            if not os.path.isdir(jobdir):
                gc3libs.log.error(
                    "GDocking: missing job directory %s" % jobdir)
                self.passed = False
                return
            outfile = os.path.join(jobdir, 'docking_protocol.stdout.txt')

            if not os.path.isfile(outfile):
                gc3libs.log.error(
                    "GDocking: missing output file %s" % outfile)
                self.passed = False
                return
        self.passed = True

class GGcGpsTest(TestRunner, Application):
    def __str__(self):
        return "GGcGps"

    def __init__(self, appdir, **kw):
        TestRunner.__init__(self, appdir, kw)
        Application.__init__(
            self,
            arguments = ['./gc_gps.py']  + self.stdargs + ['small.txt', 'src', '-i', 'in', '-o', 'out'],
            inputs = [
                os.path.join(self.appdir, name) for name in
                ('gc_gps.py', 'test-gc_gps/small.txt', 'test-gc_gps/src', 'test-gc_gps/in')],
            outputs=['out'],
            **kw)

    def terminated(self):
        self.passed = self.execution._exitcode == 0


class GCryptoTest(TestRunner, Application):
    def __str__(self):
        return "GCrypto"

    def __init__(self, appdir, **kw):
        TestRunner.__init__(self, appdir, kw)

        args = ['/bin/true']
        ifiles = [os.path.join(self.appdir, 'gcrypto.py'),
                  os.path.join(self.testdir, 'input.tgz'),
                  os.path.join(self.testdir, 'gnfs-cmd_20120406'),
                  ]
        if os.path.isdir(self.testdir) \
                and min(os.path.isfile(f) for f in ifiles):
            # input files found. Run gcrypto test.
            args = [
                './gcrypto.py',
                '-i', 'input.tgz',
                '-g', 'gnfs-cmd_20120406',
                '-c', '2'] + self.stdargs \
                + ['800000000', '800001000', '500', ]

        Application.__init__(
            self,
            arguments=args,
            inputs=ifiles,
            outputs=['NAME'],
            **kw)

    def terminated(self):
        if self.execution._exitcode == 0:
            self.passed = True


class RunTestsInParallel(ParallelTaskCollection):
    applicationdirs = {'codeml': (GCodemlTest,),
                       'gamess': (GGamessTest,),
                       'geotop': (GGeotopTest, GGcGpsTest,),
                       'lacal.epfl.ch': (GCryptoTest, ),
                       'rosetta': (GRosettaTest, GDockingTest),
                       'zods': (GZodsTest, ),
                       # 'bf.uzh.ch': None,
                       # 'compchem.unipg.it': None,
                       # 'gc3.uzh.ch': None,
                       # 'ieu.uzh.ch': (GMhCoevTest, ),
                       # 'imsb.ethz.ch': None,
                       # 'ior.uzh.ch': None,
                       # 'turbomole': None,
                       }

    def __init__(self, tests=None, **extra):
        """
        `tests` is a list of subdirectories which must match the
        `RunTestsInParallel` dictionary
        """
        if not tests:
            tests = self.applicationdirs
        else:
            tests = dict((k, v) for k, v in self.applicationdirs.iteritems()
                         if k in tests)
        tasks = []
        extra['output_dir'] = "RunTestAppsInParallel"
        for testdir, classes in tests.iteritems():
            appdir = os.path.abspath(testdir)

            tasks += [
                cls(appdir, **extra) for cls in classes
                if issubclass(cls, Task) and issubclass(cls, TestRunner)]
        if not tasks:
            raise RuntimeError("No tasks found")
        ParallelTaskCollection.__init__(self, tasks, **extra)


class TestAppsScript(SessionBasedScript):
    """
    Run tests for each application in `gc3apps`
    """
    version = '1.0'

    def setup_args(self):
        self.add_param(
            'args',
            nargs='*',
            metavar='TESTS',
            default=RunTestsInParallel.applicationdirs.keys(),
            help="If no `TEST` is given all configured tests will be run. "
            "If one or more directories are given from command line, only "
            "tests configured for those directories will be run.")

    def setup_options(self):
        self.add_param('-R', '--test-resource',
                       dest='test_resource', metavar='RESOURCE',
                       required=True,
                       help="Run test script so that they are submitted to "
                       "`RESOURCE` resource, "
                       "by adding option `-r RESOURCE` to the gc3apps "
                       "script.")

    def new_tasks(self, extra):
        extra_args = extra.copy()
        extra_args['resource'] = self.params.test_resource
        return [RunTestsInParallel(self.params.args, **extra_args)]

    def get_tasks(self):
        for task in self.session.tasks.values():
            if isinstance(task, TaskCollection):
                for t in task.tasks:
                    yield t
            else:
                yield t

    def before_main_loop(self):
        print "Tests passed from command line: %s" % str.join(", ",
                                                              self.params.args)
        # The resource may requires a password to be properly
        # initialized (e.g. x509 proxy certificates). This is usually
        # done during submission, but when the application is
        # submitted to, e.g., smscg, we don't have a prompt anymore,
        # so let's try now and check that the resource is available.
        lrms = self._core.get_backend(self.params.test_resource)
        if not lrms.enabled:
            lrms.enabled = True
        self._core.update_resources()
        if not lrms.updated:            
            raise RuntimeError("Resource '%s' not updated. Exiting" % lrms.name)

    def every_main_loop(self):
        print
        print "Current status of applications:"

        for task in self.get_tasks():
            print "Task %s: %s (passed: %s)" % (task,
                                                task.execution.state,
                                                task.passed)
        print

    after_main_loop = every_main_loop

if __name__ == "__main__":
    from run_test_apps import TestAppsScript
    TestAppsScript().run()
