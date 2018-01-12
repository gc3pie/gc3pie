#! /usr/bin/env python
#
"""
Tests for the cmdline module
"""
# Copyright (C) 2012-2016, 2018 S3IT, Zentrale Informatik, University of
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
__docformat__ = 'reStructuredText'

import os
import shutil
import signal
import subprocess
import tempfile
import re
import time

import pytest

import gc3libs.cmdline
import gc3libs.session


class TestScript(object):

    @pytest.fixture(autouse=True)
    def setUp(self):
        self.scriptdir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts')

        self.basedir = tempfile.mkdtemp()
        orig_wd = os.getcwd()
        os.chdir(self.basedir)

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
resourcedir = {basedir}/resource.d
"""
        self.cfgfile = os.path.join(self.basedir, 'config.ini')
        with open(self.cfgfile, 'w') as cfg:
            cfg.write(CONF_FILE.format(basedir=self.basedir))

        yield

        os.chdir(orig_wd)
        try:
            shutil.rmtree(basedir)
        except:
            pass


    def test_simplescript(self):
        """
        Test a very simple script based on `SessionBasedScript`:class:

        The script is found in ``gc3libs/tests/scripts/simplescript.py``
        """
        proc = subprocess.Popen([
            'python',
            os.path.join(self.scriptdir, 'simplescript.py'),
            '-C',
            '1',
            '-s',
            'TestOne',
            '--config-files',
            self.cfgfile,
            '-r',
            'localhost'
        ], stdout=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        gc3libs.log.debug("Script output:\n<<<<<<<<\n%s\n>>>>>>>>", stdout)

        assert re.match(
                '.*TERMINATED\s+3/3\s+\(100.0+%\).*',
                stdout,
                re.S)

        # FIXME: output dir should be inside session dir
        session_dir = os.path.join(self.basedir, 'TestOne')
        assert os.path.isdir(
                os.path.join(self.basedir, 'SimpleScript.out.d')
            )
        assert os.path.isfile(
                os.path.join(
                    self.basedir,
                    'SimpleScript.out.d',
                    'SimpleScript.stdout'))

        assert os.path.isdir(
                os.path.join(self.basedir, 'SimpleScript.out2.d')
            )
        assert os.path.isfile(
                os.path.join(
                    self.basedir,
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
        proc = subprocess.Popen([
            'python',
            os.path.join(self.scriptdir, 'simpledaemon.py',),
            '--config-files', self.cfgfile,
            'server',
            '-C', '1',
            '--working-dir', self.basedir,
            '-r', 'localhost',
        ],)

        for i in range(10):
            # Wait up to 10 seconds
            time.sleep(1)
            if os.path.isdir(os.path.join(self.basedir, 'EchoApp')):
                clean_exit = True
                break
        else:
            clean_exit = False

        # Kill the daemon
        # We should have a pidfile
        pidfile = os.path.join(self.basedir, 'simpledaemon.pid')
        assert os.path.isfile(pidfile)

        pid = open(pidfile).read()
        os.kill(int(pid), signal.SIGTERM)

        assert clean_exit, "Daemon didn't complete after 10 seconds"

        # Since it's a daemon, this shouldn't be needed
        proc.kill()

        # a logfile
        assert os.path.isfile(os.path.join(self.basedir, 'simpledaemon.log'))

        # the output directory
        assert os.path.isdir(os.path.join(self.basedir, 'EchoApp'))


    def test_simpledaemon_inbox(self):
        inboxdir = os.path.join(self.basedir, 'inbox')

        proc = subprocess.Popen([
            'python',
            os.path.join(self.scriptdir, 'simpledaemon.py',),
            '--config-files', self.cfgfile,
            '-vvv',
            'server',
            '-C', '1',
            '--working-dir', self.basedir,
            '-r', 'localhost',
            inboxdir,
        ],)

        # Wait until the daemon is up and running.
        # It will create the directory, so let's wait until then.
        for i in range(10):
            if os.path.isdir(inboxdir):
                daemon_running = True
                break
            time.sleep(1)
        else:
            daemon_running = False

        assert daemon_running

        # create marker file
        with open(os.path.join(inboxdir, 'foo'), 'w+') as fd:
            fd.write('contents')

        for i in range(10):
            # Wait up to 10 seconds
            time.sleep(1)
            if os.path.isdir(os.path.join(self.basedir, 'LSApp.foo')):
                clean_exit = True
                break
        else:
            clean_exit = False

        # Kill the daemon
        # We should have a pidfile
        pidfile = os.path.join(self.basedir, 'simpledaemon.pid')
        assert os.path.isfile(pidfile)

        pid = open(pidfile).read()
        os.kill(int(pid), signal.SIGTERM)
        os.kill(int(pid), signal.SIGHUP)

        assert clean_exit, "Daemon didn't complete after 10 seconds"

        # Since it's a daemon, this shouldn't be needed
        proc.kill()

        # a logfile
        assert os.path.isfile(os.path.join(self.basedir, 'simpledaemon.log'))

        # the output directory
        assert os.path.isdir(os.path.join(self.basedir, 'EchoApp'))


# main: run tests

if "__main__" == __name__:
    import pytest
    pytest.main(["-v", __file__])
