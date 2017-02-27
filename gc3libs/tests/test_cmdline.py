#! /usr/bin/env python
#
"""
Tests for the cmdline module
"""
# Copyright (C) 2012-2016 S3IT, Zentrale Informatik, University of
# Zurich. All rights reserved.
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

import os
import shutil
import signal
import subprocess
import tempfile
import re
import time

import cli.test

import gc3libs.cmdline
import gc3libs.session


class TestScript(cli.test.FunctionalTest):

    def __init__(self, *args, **extra_args):
        cli.test.FunctionalTest.__init__(self, *args, **extra_args)
        self.scriptdir = os.path.join(
            os.path.dirname(
                os.path.abspath(__file__)),
            'scripts')

    def setUp(self):
        cli.test.FunctionalTest.setUp(self)
        CONF_FILE = """
[auth/dummy]
type = ssh
username = dummy

[resource/localhost]
enabled = true
auth = dummy
type = shellcmd
frontend = localhost
transport = local
max_cores_per_job = 2
max_memory_per_core = 8
max_walltime = 8
max_cores = 2
architecture = x86_64
override = False
resourcedir = %s
"""
        (fd, self.cfgfile) = tempfile.mkstemp()
        self.resourcedir = self.cfgfile + '.d'
        f = os.fdopen(fd, 'w')
        f.write(CONF_FILE % self.resourcedir)
        f.close()

    def tearDown(self):
        os.remove(self.cfgfile)
        cli.test.FunctionalTest.tearDown(self)
        try:
            shutil.rmtree(self.resourcedir)
        except:
            # Double check if some dir is still present
            pass

    def test_simplescript(self):
        """Test a very simple script based on `SessionBasedScript`:class:

        The script is found in ``gc3libs/tests/scripts/simplescript.py``
        """
        # XXX: WARNING: This will only work if you have a "localhost"
        # section in gc3pie.conf!!!
        result = self.run_script(
            'python',
            os.path.join(
                self.scriptdir,
                'simplescript.py'),
            '-C',
            '1',
            '-s',
            'TestOne',
            '--config-files',
            self.cfgfile,
            '-r',
            'localhost')

        assert re.match(
                '.*TERMINATED\s+3/3\s+\(100.0+%\).*',
                result.stdout,
                re.S)

        # FIXME: output dir should be inside session dir
        session_dir = os.path.join(self.env.base_path, 'TestOne')
        assert os.path.isdir(
                os.path.join(self.env.base_path, 'SimpleScript.out.d')
            )
        assert os.path.isfile(
                os.path.join(
                    self.env.base_path,
                    'SimpleScript.out.d',
                    'SimpleScript.stdout'))

        assert os.path.isdir(
                os.path.join(self.env.base_path, 'SimpleScript.out2.d')
            )
        assert os.path.isfile(
                os.path.join(
                    self.env.base_path,
                    'SimpleScript.out2.d',
                    'SimpleScript.stdout'))

        assert os.path.isdir(session_dir)

        assert os.path.isfile(
                os.path.join(
                    session_dir,
                    gc3libs.session.Session.INDEX_FILENAME,
                ))

        assert os.path.isfile(
                os.path.join(
                    session_dir,
                    gc3libs.session.Session.STORE_URL_FILENAME,
                ))

    def test_simpledaemon_d(self):
        wdir = os.path.join(self.env.base_path, 'wdir')
        proc = subprocess.Popen([
            'python',
            os.path.join(self.scriptdir, 'simpledaemon.py',),
            '--config-files', self.cfgfile,
            'server',
            '-C', '1',
            '--working-dir', wdir,
            '-r', 'localhost',
        ],)

        clean_exit = False
        for i in range(10):
            # Wait up to 10 seconds
            time.sleep(1)

            if os.path.isdir(os.path.join(wdir, 'EchoApp')):
                clean_exit = True
                break

        # Kill the daemon
        # We should have a pidfile
        pidfile = os.path.join(wdir, 'simpledaemon.pid')
        assert os.path.isfile(pidfile)

        pid = open(pidfile).read()
        os.kill(int(pid), signal.SIGTERM)

        assert clean_exit, "Daemon didn't complete after 10 seconds"
        assert os.path.isdir(wdir)

        # Since it's a daemon, this shouldn't be needed
        proc.kill()

        # a logfile
        assert os.path.isfile(os.path.join(wdir, 'simpledaemon.log'))

        # the output directory
        assert os.path.isdir(os.path.join(wdir, 'EchoApp'))

    def test_simpledaemon_inbox(self):
        wdir = os.path.join(self.env.base_path, 'wdir')
        inboxdir = os.path.join(wdir, 'inbox')
        proc = subprocess.Popen([
            'python',
            os.path.join(self.scriptdir, 'simpledaemon.py',),
            '--config-files', self.cfgfile,
            '-vvv',
            'server',
            '-C', '1',
            '--working-dir', wdir,
            '-r', 'localhost',
            inboxdir,
        ],)

        clean_exit = False
        # Wait until the daemon is up and running.
        # It will create the directory, so let's wait until then.
        daemon_running = False
        for i in range(10):
            if os.path.isdir(inboxdir):
                daemon_running = True
                break
            time.sleep(1)

        assert daemon_running
        fd = open(os.path.join(inboxdir, 'foo'), 'w+')
        fd.close()

        for i in range(10):
            # Wait up to 10 seconds
            time.sleep(1)
            # Create fake file

            if os.path.isdir(os.path.join(wdir, 'LSApp.foo')):
                clean_exit = True
                break

        # Kill the daemon
        # We should have a pidfile
        pidfile = os.path.join(wdir, 'simpledaemon.pid')
        assert os.path.isfile(pidfile)

        pid = open(pidfile).read()
        os.kill(int(pid), signal.SIGTERM)
        os.kill(int(pid), signal.SIGHUP)

        assert clean_exit, "Daemon didn't complete after 10 seconds"
        assert os.path.isdir(wdir)

        # Since it's a daemon, this shouldn't be needed
        proc.kill()

        # a logfile
        assert os.path.isfile(os.path.join(wdir, 'simpledaemon.log'))

        # the output directory
        assert os.path.isdir(os.path.join(wdir, 'EchoApp'))

# main: run tests

if "__main__" == __name__:
    import pytest
    pytest.main(["-v", __file__])
