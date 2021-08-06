#! /usr/bin/env python
#
"""
Tests for the cmdline module
"""
# Copyright (C) 2021                  Google LLC.
# Copyright (C) 2012-2016, 2018, 2019 University of Zurich. All rights reserved.
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

from __future__ import absolute_import, print_function, unicode_literals
from builtins import range
from builtins import object
import json
import os
from os.path import abspath, dirname, exists, isdir, isfile, join
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time

import pytest

import gc3libs
import gc3libs.session
from gc3libs.utils import read_contents, write_contents, to_str


class _TestsCommon(object):
    """
    Set up environment for testing session-based commands.
    """

    @pytest.fixture(autouse=True)
    def setUp(self):
        self.scriptdir = join(dirname(abspath(__file__)), 'scripts')
        self.client_py = join(self.scriptdir, 'simpleclient.py')
        self.daemon_py = join(self.scriptdir, 'simpledaemon.py')
        self.script_py = join(self.scriptdir, 'simplescript.py')

        self.basedir = tempfile.mkdtemp(
            prefix=(__name__ + '.'),
            suffix='.tmp.d')
        orig_wd = os.getcwd()
        os.chdir(self.basedir)

        self.cfgfile = join(self.basedir, 'config.ini')
        write_contents(self.cfgfile,
                       """
[resource/localhost]
enabled = true
auth = none
type = shellcmd
frontend = localhost
transport = local
max_cores_per_job = 2
max_memory_per_core = 8GB
max_walltime = 8h
max_cores = 2
architecture = x86_64
override = False
resourcedir = {basedir}/resource.d
                       """.format(basedir=self.basedir))

        try:
            yield
        finally:
            os.chdir(orig_wd)
            shutil.rmtree(self.basedir, ignore_errors=True)

    def run(self, cmd):
        """
        Run `cmd` and return exit code and STDOUT+STDERR.
        """
        # FIXME: cannot use `subprocess.check_call` as it's not
        # available on Py 2.6
        proc = subprocess.Popen(
            # same Python that's running tests
            [sys.executable] + cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        stdout = to_str(proc.communicate()[0], 'terminal')
        rc = proc.returncode
        gc3libs.log.debug(
            "Log of command `%s`:\n```\n%s\n```",
            ' '.join(cmd), stdout)
        assert rc == 0, (
            "Command `{0}` exited with non-zero exitcode {1}"
            .format(' '.join(cmd), rc))
        return (rc, stdout)

    def check_with_timeout(self, condition, timeout, errmsg):
        """
        Repeatedly test `condition` until it either returns ``True``
        or `timeout` seconds have passed.
        """
        for _ in range(timeout):
            if condition():
                return
            time.sleep(1)
        else:
            assert False, (
                (errmsg + " (within {timeout} seconds)")
                .format(timeout=timeout))


# FIXME: why do Session-based scripts fail on Cirrus CI?
@pytest.mark.skipif(os.environ.get('CIRRUS_CI', 'false') == 'true',
                    reason="Session-based script tests stall on Cirrus CI")
class TestSessionBasedScript(_TestsCommon):
    """
    Test suite for session-based (interactive) scripts.
    """

    def test_session_based_script(self):
        """
        Test a very simple script based on `SessionBasedScript`:class:

        The script is found in ``gc3libs/tests/scripts/simplescript.py``
        """

        rc, stdout = self.run([
            self.script_py,
            '-C', '1',
            '-s', 'session',
            '--config-files', self.cfgfile,
            '-r', 'localhost',
        ])

        assert re.match(
                r'.*TERMINATED\s+3/3\s+\(100.0+%\).*',
                stdout,
                re.S)

        assert isdir(join(self.basedir, 'SimpleScript.out.d'))
        assert isfile(join(self.basedir,
                           'SimpleScript.out.d',
                           'SimpleScript.stdout'))

        assert isdir(join(self.basedir, 'SimpleScript.out2.d'))
        assert isfile(join(self.basedir,
                           'SimpleScript.out2.d',
                           'SimpleScript.stdout'))

        session_dir = join(self.basedir, 'session')
        assert isdir(session_dir)

        assert isfile(join(session_dir,
                           gc3libs.session.Session.INDEX_FILENAME,))

        assert isfile(join(session_dir,
                           gc3libs.session.Session.STORE_URL_FILENAME,))


# FIXME: why do Session-based scripts fail on Cirrus CI?
@pytest.mark.skipif(os.environ.get('CIRRUS_CI', 'false') == 'true',
                    reason="Session-based script tests stall on Cirrus CI")
# daemon-related tests fail on Py2.6 because (apparently) python-daemon
# does not correctly create PID files on that platform; disable
# the tests altogether, as we'll have to drop Py2.6 support soon...
@pytest.mark.skipif(sys.version_info < (2,7),
                    reason="python-daemon requires python 2.7+")
class TestSessionBasedDaemon(_TestsCommon):
    """
    Test suite for session-based daemons.
    """

    @pytest.fixture
    def daemon(self):
        inbox_dir = join(self.basedir, 'inbox')
        session_dir = join(self.basedir, 'session')

        subprocess.call([
            sys.executable,  # same Python that's running the tests
            join(self.scriptdir, 'simpledaemon.py',),
            '--config-files', self.cfgfile,
            '-C', '1',
            '-s', 'session',
            '--working-dir', self.basedir,
            '-r', 'localhost',
            '-vvv',
            inbox_dir,
        ])

        pidfile = join(self.basedir, 'session', 'simpledaemon.pid')
        self.check_with_timeout(
            condition=lambda: exists(pidfile),
            timeout=15,
            errmsg="PID file not created")
        pid = int(read_contents(pidfile))

        try:
            # transfer control back to test code
            yield pid, session_dir, inbox_dir
        finally:
            try:
                # kill the daemon and clean up
                os.kill(pid, signal.SIGTERM)
                # give it time to shut down
                time.sleep(3)
            except OSError as ex:
                if ex.errno == 3:  # "No such process"
                    # daemon has already exited, ignore
                    pass
                else:
                    raise


    def test_session_based_daemon_command_hello(self, daemon):
        pid, session_dir, _ = daemon

        _, stdout = self.run([self.client_py, session_dir, 'hello'])
        assert stdout.startswith("HELLO")


    def test_session_based_daemon_new_tasks(self, daemon, max_wait=10):
        pid, session_dir, _ = daemon

        # wait up to max_wait seconds for task to complete
        def check_for_terminated():
            _, stdout = self.run([self.client_py, session_dir, 'stats', 'json'])
            stats = json.loads(stdout)
            return stats.get("TERMINATED", 0) > 0
        self.check_with_timeout(
            condition=check_for_terminated,
            timeout=max_wait,
            errmsg="Daemon didn't complete initial task"
        )

        # check that output directory is there
        output_dir = join(self.basedir, 'EchoApp')
        self.check_with_timeout(
            condition=lambda: isdir(output_dir),
            timeout=max_wait,
            errmsg="Daemon didn't write output directory"
        )


    def test_session_based_daemon_inbox(self, daemon, max_wait=10):
        _, _, inboxdir = daemon

        # Wait until the daemon is up and running.
        # It will create the directory, so let's wait until then.
        self.check_with_timeout(
            condition=lambda: isdir(inboxdir),
            timeout=max_wait,
            errmsg="Daemon didn't create inbox dir"
        )

        # create marker file
        marker_file = join(inboxdir, 'foo')
        write_contents(marker_file, 'whatever')

        # wait up to max_wait seconds for task to complete
        output_dir = join(self.basedir, 'LSApp.foo')
        self.check_with_timeout(
            condition=lambda: isdir(output_dir),
            timeout=max_wait,
            errmsg="Daemon didn't process incoming file"
        )


    def test_session_based_daemon_command_help(self, daemon):
        pid, session_dir, _ = daemon

        rc, stdout = self.run([self.client_py, session_dir, 'help'])
        assert ("The following daemon commands are available:" in stdout)


    def test_session_based_daemon_command_quit(self, daemon, max_wait=60):
        pid, session_dir, _ = daemon

        rc, stdout = self.run([self.client_py, session_dir, 'quit'])

        # wait up to max_wait seconds for daemon to shut down and
        # remove PID file
        pidfile = join(session_dir, 'simpledaemon.pid')
        self.check_with_timeout(
            condition=lambda: not exists(pidfile),
            timeout=max_wait,
            errmsg="Daemon didn't shut down"
        )


    def test_session_based_daemon_reload_session(self, max_wait=60):
        inbox_dir = join(self.basedir, 'inbox')
        session_dir = join(self.basedir, 'session')

        # start daemon
        self.run([
            self.daemon_py,
            '--config-files', self.cfgfile,
            '-C', '1',
            '-s', 'session',
            '--working-dir', self.basedir,
            '-r', 'localhost',
            '-vvv',
            inbox_dir,
        ])

        pidfile = join(self.basedir, 'session', 'simpledaemon.pid')
        self.check_with_timeout(
            condition=lambda: exists(pidfile),
            timeout=15,
            errmsg="PID file not created")
        pid = int(read_contents(pidfile))

        # get IDs of tasks in session -- retry until the server is ready to respond
        for _ in range(15):
            try:
                _, stdout = self.run([self.client_py, session_dir, 'list', 'json'])
                task_ids = json.loads(stdout)
            except ValueError:
                time.sleep(1)
        assert len(task_ids) > 0

        # wait up to max_wait seconds for task to complete
        output_dir = join(self.basedir, 'EchoApp')
        self.check_with_timeout(
            condition=lambda: isdir(output_dir),
            timeout=max_wait,
            errmsg="Daemon didn't complete initial task"
        )

        # stop daemon
        self.run([self.client_py, session_dir, 'quit'])

        # wait up to max_wait seconds for daemon to shut down and
        # remove PID file
        pidfile = join(session_dir, 'simpledaemon.pid')
        self.check_with_timeout(
            condition=lambda: not exists(pidfile),
            timeout=max_wait,
            errmsg="Daemon didn't shut down"
        )

        # restart daemon
        self.run([
            self.daemon_py,
            '--config-files', self.cfgfile,
            '-C', '1',
            '-s', 'session',
            '--working-dir', self.basedir,
            '-r', 'localhost',
            '-vvv',
            inbox_dir,
        ])

        # it may take a few seconds before the daemon is ready to serve requests,
        # so try and possibly re-try...
        for _ in range(15):
            try:
                _, stdout = self.run([self.client_py, session_dir, 'list', 'json'])
                task_ids2 = json.loads(stdout)
            except ValueError:
                time.sleep(1)

        # check that task IDs are the same
        assert set(task_ids) == set(task_ids2)

        # check that one TERMINATED task is there
        _, stdout = self.run([self.client_py, session_dir, 'stats', 'json'])
        stats = json.loads(stdout)
        assert stats["TERMINATED"] == len(task_ids)


## main: run tests

if "__main__" == __name__:
    import pytest
    pytest.main(["-v", __file__])
