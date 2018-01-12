#! /usr/bin/env python
#
#   daemon.py -- Base classes for running daemonized GC3Pie scripts
#
#   Copyright (C) 2010-2018 University of Zurich
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
"""
Base classes for running daemonized GC3Pie scripts.

Classes implemented in this file provide common and recurring
functionality for GC3Libs command-line utilities and scripts.  User
applications should implement their specific behavior by subclassing
and overriding a few customization methods.

There is currently only one public class provided here:

`SessionBasedDaemon`:class:
  Base class for GC3Pie daemons. Implements a long-running daemon with
  XML-RPC interface and support for "inboxes" (which can add or remove
  tasks based on external events).
"""

from __future__ import (absolute_import, division, print_function)

# stdlib modules
import fnmatch
import logging
from logging.handlers import SysLogHandler
import math
import os
import os.path
import signal
import sys
from prettytable import PrettyTable
import time
import threading
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
from collections import defaultdict
from prettytable import PrettyTable
import SimpleXMLRPCServer as sxmlrpc
import xmlrpclib

import daemon
import json
import yaml

# 3rd party modules
import daemon
import cli  # pyCLI
import cli.app
import cli._ext.argparse as argparse


# interface to Gc3libs
import gc3libs
import gc3libs.config
from gc3libs.cmdline import _Script, _SessionBasedCommand
from gc3libs.compat import lockfile
from gc3libs.compat.lockfile.pidlockfile import PIDLockFile
import gc3libs.core
import gc3libs.exceptions
import gc3libs.persistence
import gc3libs.utils
import gc3libs.url
from gc3libs.quantity import Memory, GB, Duration, hours
from gc3libs.session import Session
from gc3libs.poller import make_poller, get_mask_description, events as notify_events


class _CommDaemon(object):
    portfile_name = 'daemon.port'

    def __init__(self, name, listenip, workingdir, parent):
        self.parent = parent
        self.log = self.parent.log

        # Start XMLRPC server
        self.server = sxmlrpc.SimpleXMLRPCServer((listenip, 0),
                                                 logRequests=False)
        self.ip, self.port = self.server.socket.getsockname()
        self.log.info("XMLRPC daemon running on %s,"
                      "port %d.", self.ip, self.port)
        self.portfile = os.path.join(workingdir, self.portfile_name)
        ### FIXME: we should check if the file exists already
        with open(self.portfile, 'w') as fd:
            self.log.debug("Writing current port (%d) I'm listening to"
                           " in file %s" % (self.port, self.portfile))
            fd.write("%s:%d\n" % (self.ip, self.port))

        # Register XMLRPC methods
        self.server.register_introspection_functions()

        self.server.register_function(self.help, "help")
        self.server.register_function(self.list_jobs, "list")
        self.server.register_function(self.show_job, "show")
        self.server.register_function(self.stat_jobs, "stat")
        self.server.register_function(self.kill_job, "kill")
        self.server.register_function(self.resubmit_job, "resubmit")
        self.server.register_function(self.remove_job, "remove")
        self.server.register_function(self.terminate, "terminate")
        self.server.register_function(self.json_list, "json_list")
        self.server.register_function(self.json_show_job, "json_show")

    def start(self):
        return self.server.serve_forever()

    def stop(self):
        try:
            self.server.shutdown()
            self.server.socket.close()
            os.remove(self.portfile)
        except Exception as ex:
            # self.stop() could be called twice, let's assume it's not
            # an issue but log the event anyway
            self.log.warning(
                "Ignoring exception caught while closing socket: %s", ex)

    def terminate(self):
        """Terminate daemon"""
        # Start a new thread so that we can reply
        def killme():
            self.log.info("Terminating as requested via IPC")
            # Wait 1s so that the client connection is not hang up and
            # we have time to give feedback
            time.sleep(1)
            # daemon cleanup will aslo call self.stop()
            self.parent.cleanup()

            # Send kill signal to current process
            os.kill(os.getpid(), signal.SIGTERM)

            # This should be enough, but just in case...
            self.log.warning(
                "Waiting 5s to see if my sucide attempt succeeded")
            time.sleep(5)

            # If this is not working, try mor aggressive approach.

            # SIGINT is interpreted by SessionBasedDaemon class. It
            # will also call self.parent.cleanup(), again.
            self.log.warning("Still alive: sending SIGINT signal to myself")
            os.kill(os.getpid(), signal.SIGINT)

            # We whould never reach this point, but Murphy's law...
            time.sleep(5)
            # Revert back to SIGKILL. This will leave the pidfile
            # hanging around.
            self.log.warning("Still alive: forcing SIGKILL signal.")
            os.kill(os.getpid(), signal.SIGKILL)

        t = threading.Thread(target=killme)
        t.start()
        return "Terminating %d in 1s" % os.getpid()

    ### user visible methods

    def help(self, cmd=None):
        """Show available commands, or get information about a specific
        command"""

        if not cmd:
            return self.server.system_listMethods()
        else:
            return self.server.system_methodHelp(cmd)

    @staticmethod
    def print_app_table(app, indent, recursive):
        """Returns a list of lists containing a summary table of the jobs"""
        rows = []
        try:
            jobname = app.jobname
        except AttributeError:
            jobname = ''

        rows.append([indent + str(app.persistent_id),
                     jobname,
                     app.execution.state,
                     app.execution.returncode,
                     app.execution.info])
        if recursive and 'tasks' in app:
            indent = " "*len(indent) + '  '
            for task in app.tasks:
                rows.extend(_CommDaemon.print_app_table(task, indent, recursive))
        return rows

    def list_jobs(self, opts=None):
        """usage: list [detail]

        List jobs"""

        if opts and 'details'.startswith(opts):
            rows = []
            for app in self.parent.session.tasks.values():
                rows.extend(self.print_app_table(app, '', True))
            table = PrettyTable(["JobID", "Job name", "State", "rc", "Info"])
            table.align = 'l'
            for row in rows:
                table.add_row(row)
            return str(table)
        elif opts and 'all'.startswith(opts):
            return str.join(' ', [i.persistent_id for i in self.parent.session.iter_workflow()])
        else:
            return str.join(' ', self.parent.session.list_ids())

    def json_list(self):
        """usage: json_show jobid

        List jobs"""

        jobids = [i.persistent_id for i in self.parent.session.iter_workflow()]
        jobs = []
        for jobid in jobids:
            app = self.parent.session.store.load(jobid)
            sapp = StringIO()
            gc3libs.utils.prettyprint(app, output=sapp)
            jobs.append(yaml.load(sapp.getvalue()))
        return json.dumps(jobs)

    def json_show_job(self, jobid=None, *attrs):
        """usage: json_show <jobid>

        Same output as `ginfo -v <jobid>
        """

        if not jobid:
            return "Usage: show <jobid>"

        if jobid not in self.parent.session.tasks:
            all_tasks = dict((i.persistent_id,i) for i in self.parent.session.iter_workflow() if hasattr(i, 'persistent_id'))
            if jobid not in all_tasks:
                return "Job %s not found in session" % jobid
            else:
                app = all_tasks[jobid]
        else:
            app = self.parent.session.tasks[jobid]
        sapp = StringIO()
        gc3libs.utils.prettyprint(app, output=sapp)
        return json.dumps(yaml.load(sapp.getvalue()))

    def show_job(self, jobid=None, *attrs):
        """usage: show <jobid> [attributes]

        Same output as `ginfo -v <jobid> [-p attributes]`
        """

        if not jobid:
            return "Usage: show <jobid> [attributes]"

        if jobid not in self.parent.session.tasks:
            all_tasks = dict((i.persistent_id,i) for i in self.parent.session.iter_workflow() if hasattr(i, 'persistent_id'))
            if jobid not in all_tasks:
                return "Job %s not found in session" % jobid
            else:
                app = all_tasks[jobid]
        else:
            app = self.parent.session.tasks[jobid]
        try:
            out = StringIO()
            if not attrs:
                attrs = None
            gc3libs.utils.prettyprint(app,
                                      indent=4,
                                      output=out,
                                      only_keys=attrs)
            return out.getvalue()
        except Exception as ex:
            return "Unable to find job %s" % jobid

    def kill_job(self, jobid=None):
        if not jobid:
            return "Usage: kill <jobid>"

        if jobid not in self.parent.session.tasks:
            all_tasks = dict((i.persistent_id,i) for i in self.parent.session.iter_workflow() if hasattr(i, 'persistent_id'))
            if jobid not in all_tasks:
                return "Job %s not found in session" % jobid
            else:
                app = all_tasks[jobid]
        else:
            app = self.parent.session.tasks[jobid]
        try:
            app.attach(self.parent._controller)
            app.kill()
            return "Job %s successfully killed" % jobid
        except Exception as ex:
            return "Error while killing job %s: %s" % (jobid, ex)

    def remove_job(self, jobid=None):
        if not jobid:
            return "Usage: remove <jobid>"

        if jobid not in self.parent.session.tasks:
            return "Job %s not found in session" % jobid
        app = self.parent.session.load(jobid)
        if app.execution.state != gc3libs.Run.State.TERMINATED:
            return "Error: you can only remove a terminated job. Current status is: %s" % app.execution.state
        try:
            self.parent._controller.remove(app)
            self.parent.session.remove(jobid)
            return "Job %s successfully removed" % jobid
        except Exception as ex:
            return "Error while removing job %s: %s" % (jobid, ex)

    def resubmit_job(self, jobid=None):
        if not jobid:
            return "Usage: resubmit <jobid>"
        if jobid not in self.parent.session.tasks:
            all_tasks = dict((i.persistent_id,i) for i in self.parent.session.iter_workflow() if hasattr(i, 'persistent_id'))
            if jobid not in all_tasks:
                return "Job %s not found in session" % jobid
            else:
                app = all_tasks[jobid]
        else:
            app = self.parent.session.tasks[jobid]

        try:
            self.parent._controller.redo(app)
        except Exception as ex:
            return "Error while resubmitting job %s: %s" % (jobid, ex)
        return "Successfully resubmitted job %s" % jobid

    def stat_jobs(self):
        """Print how many jobs are in any given state"""

        stats = self.parent._controller.stats()
        return str.join('\n', ["%s:%s" % x for x in stats.items()])


class SessionBasedDaemon(_SessionBasedCommand):
    """Base class for GC3Pie daemons. Implements a long-running script
    that can daemonize, provides an XML-RPC interface to interact with
    the current workflow and implement the concept of "inbox" to
    trigger the creation of new jobs as soon as a new file is created
    on a folder, or is available on an HTTP(s) or SWIFT endpoint.

    The generic script impelemnts a command line like the following::

      PROG [global options] server|client [specific options]

    When running as a `server` it will accepts the same options as a
    :py:class:`SessionBasedScript()` class, plus some extra options:

      PROG server [server options] [INBOX [INBOX]]

    Available options:

    `-F, --foreground`
      do not daemonize.

    `--syslog`
      send logs to syslog instead of writing it to a file

    `--working-dir`
      working directory of the daemon, where the session
      will be stored and some auxiliary files will be saved

    `--notify-state [EVENT,[EVENT]]`
      a comma separated list of events
      on the inbox we want to be notified of

    `--listen IP`
      IP or hostname we want to listen to. Default is localhost.

    `INBOX`
      path or url of one or more folder/url that will be watched for
      new events.

    When running as client instead::

      PROG client [-c FILE] ARGS

    `-c`
      file written by the server conatining the hostname and port the
      server is listening to. Also accepts a path to the directory
      (the value of `--working-dir` the server was started with) where
      the `daemon.port` file created by the server is stored.

    """

    def cleanup(self, signume=None, frame=None):
        self.log.debug("Waiting for communication thread to terminate")
        try:
            self.comm.stop()
            self.commthread._Thread__stop()
            self.commthread._Thread__delete()
            self.commthread.join(1)
        except AttributeError:
            # If the script is interrupted/killed during command line
            # parsing the `self.comm` daemon is not present and we get
            # an AttributeError we can safely ignore.
            pass

    def setup(self):
        _Script.setup(self)
        self.subparsers = self.argparser.add_subparsers(title="commands")
        self.parser_server = self.subparsers.add_parser('server', help="Run the main daemon script.")
        self.parser_client = self.subparsers.add_parser('client', help="Connect to a running daemon.")

        # Wrapper function around add_argument.
        def _add_param_server(*args, **kwargs):
            action = self.parser_server.add_argument(*args, **kwargs)
            self.actions[action.dest] = action
            return action
        self.parser_server.add_param = _add_param_server
        # This method assumes `self.add_param` exists
        self.setup_common_options(self.parser_server)

        # Also, when subclassing, add_param should add arguments to
        # the server parser by default.
        self.add_param = lambda *x, **kw: self.parser_server.add_param(*x, **kw)

        self.parser_server.set_defaults(func=self._main_server)
        self.parser_client.set_defaults(func=self._main_client)

        # change default for the `-C`, `--session` and `--output` options
        self.actions['wait'].default = 30
        self.actions['wait'].help = 'Check the status of the jobs every NUM'
        ' seconds. Default: %(default)s'

        # Default session dir and output dir are computed from
        # --working-directory.  Set None here so that we can update it
        # in pre_run()
        self.actions['session'].default = None
        self.actions['output'].default = None

    def setup_args(self):
        self.parser_server.add_param('-F', '--foreground',
                       action='store_true',
                       default=False,
                       help="Run in foreground. Default: %(default)s")

        self.parser_server.add_param('--syslog',
                       action='store_true',
                       default=False,
                       help="Send log messages (also) to syslog."
                       " Default: %(default)s")

        self.parser_server.add_param('--working-dir',
                       default=os.getcwd(),
                       help="Run in WORKING_DIR. Ignored if run in foreground."
                       " Default: %(default)s")

        self.parser_server.add_param('--notify-state',
                       nargs='?',
                       default='CLOSE_WRITE',
                       help="Comma separated list of notify events to watch."
                       " Default: %(default)s. Available events: " +
                       str.join(', ', [i[3:] for i in notify_events.keys()]))

        self.parser_server.add_param(
            '--listen',
            default="localhost",
            metavar="IP",
            help="IP or hostname where the XML-RPC thread should"
            " listen to. Default: %(default)s")

        self.parser_server.add_param('inbox', nargs='*',
                       help="`inbox` directories: whenever a new file is"
                       " created in one of these directories, a callback is"
                       " triggered to add new jobs")

        self.parser_client.add_argument('-c', '--connect',
                                        metavar="FILE",
                                        required=True,
                                        help="Path to the file containing hostname and port of the"
                                        " XML-RPC enpdoint of the daemon")
        self.parser_client.add_argument('cmd',
                                        metavar='COMMAND',
                                        help="XML-RPC command. Run `help` to know"
                                        " which commands are available.")
        self.parser_client.add_argument('args',
                                        nargs='*',
                                        metavar='ARGS',
                                        help="Optional arguments of CMD.")

    def pre_run(self):
        ### FIXME: Some code copied from _Script.pre_run()

        self.setup_options()
        self.setup_args()
        cli.app.CommandLineApp.pre_run(self)
        loglevel = max(1, logging.WARNING -
                       10 *
                       max(0, self.params.verbose -
                           self.verbose_logging_threshold))
        gc3libs.configure_logger(loglevel, "gc3.gc3utils")  # alternate: self.name
        logging.root.setLevel(loglevel)
        # alternate: ('gc3.' + self.name)
        self.log = logging.getLogger('gc3.gc3utils')
        self.log.setLevel(loglevel)
        self.log.propagate = True
        self.log.info("Starting %s at %s; invoked as '%s'",
                      self.name, time.asctime(), str.join(' ', sys.argv))

        # FIXME: we need to ignore the process_args method as the
        # `client` subparser has different options than the `server`
        # one, and the default is the `server` subparser.
        if self.params.func == self._main_client:
            # override `process_args` with a noop.
            self.process_args = lambda *x, **kw: None
            return

        # Read config file(s) from command line
        self.params.config_files = self.params.config_files.split(',')
        # interface to the GC3Libs main functionality
        self.config = self._make_config(self.params.config_files)
        try:
            self._core = gc3libs.core.Core(self.config)
        except gc3libs.exceptions.NoResources:
            # translate internal error `NoResources` to a
            # user-readable message.
            raise gc3libs.exceptions.FatalError(
                "No computational resources defined."
                " Please edit the configuration file(s): '%s'."
                % (str.join("', '", self.params.config_files)))

        self.params.working_dir = os.path.abspath(self.params.working_dir)

        # Default session dir is inside the working directory
        if not self.params.session:
            self.params.session = os.path.join(
                self.params.working_dir, self.name)

        # Convert inbox to Url objects
        self.params.inbox = [gc3libs.url.Url(i) for i in self.params.inbox]
        # Default output directory is the working directory.
        if not self.params.output:
            self.params.output = self.params.working_dir

        self._prerun_common_checks()
        self.parse_args()

        # Syntax check for notify events
        self.params.notify_state = self.params.notify_state.split(',')

        # Add IN_ as we use shorter names for the command line
        state_names = ['IN_' + i for i in self.params.notify_state]

        # Build the notify mask, for later use
        self.notify_event_mask = 0

        # Ensure all the supplied states are correct
        for istate in state_names:
            if istate not in notify_events:
                raise gc3libs.exceptions.InvalidUsage(
                    "Invalid notify state %s." % state)
            self.notify_event_mask |= notify_events[istate]

    def __setup_logging(self):
        # FIXME: Apparently, when running in foreground _and_ --syslog
        # is used, only a few logs are sent to syslog and then nothing
        # more.

        # We use a function to avoid repetitions: since when
        # demonizing all open file descriptors are called, we need to
        # configure logging from *within* the DaemonContext context

        # If --syslog, add a logging handler to send to local syslog
        if self.params.syslog:
            # Update the root logger, not just 'gc3utils'
            logging.root.addHandler(
                SysLogHandler(address="/dev/log",
                              facility=SysLogHandler.LOG_USER))
        elif not self.params.foreground:
            # The default behavior when run in daemon mode
            # is to log to a file in working directory called
            # <application>.log
            #
            # Also, update the root logger, not just 'gc3utils'
            logging.root.addHandler(
                logging.FileHandler(
                    os.path.join(self.params.working_dir,
                                 self.name + '.log')))
        # else: log to stdout, which is the default.

    def __setup_pollers(self):
        # Setup inotify on inbox directories
        self.pollers = []
        # We need to create an inotify file descriptor for each
        # directory, because events returned by
        # `inotifyx.get_events()` do not contains the full path to the
        # file.
        for inbox in self.params.inbox:
            self.pollers.append(make_poller(inbox, self.notify_event_mask, recurse=True))

    def __setup_comm(self, listen):
        # Communication thread must run on a different thread
        try:
            self.comm = _CommDaemon(
                self.name,
                self.params.listen,
                self.params.working_dir,
                self)
            def commthread():
                self.log.info("Starting XML-RPC server")
                self.comm.start()

            self.commthread = threading.Thread(target=commthread)
            self.commthread.start()
            self.log.info("Communication thread started")
        except Exception as ex:
            self.log.error(
                "Error initializinig Communication thread: %s" % ex)

    def _main_client(self):
        portfile = self.params.connect
        if os.path.isdir(portfile):
            gc3libs.log.debug("First argument of --client is a directory. Checking if file `%s` is present in it" % _CommDaemon.portfile_name)
            portfile = os.path.join(portfile, _CommDaemon.portfile_name)
            if not os.path.isfile(portfile):
                self.argparser.error("First argument of --client must be a file.")
            gc3libs.log.info("Using file `%s` as argument of --connect option" % portfile)
        with open(portfile, 'r') as fd:
            try:
                ip, port = fd.read().split(':')
                port = int(port)
            except Exception as ex:
                print("Error parsing file %s: %s" % (portfile, ex))
        server = xmlrpclib.ServerProxy('http://%s:%d' % (ip, port))
        func = getattr(server, self.params.cmd)
        try:
            print(func(*self.params.args))
        except xmlrpclib.Fault as ex:
            print("Error while running command `%s`: %s" % (self.params.cmd, ex.faultString))
            print("Use `help` command to list all available methods")

    def _main(self):
        return self.params.func()

    def _main_server(self):
        self.process_args()

        if self.params.foreground:
            # If --foreground, then behave like a SessionBasedScript with
            # the exception that the script will never end.
            self.__setup_logging()
            self.__setup_pollers()
            self.log.info("Running in foreground as requested")

            self.__setup_comm(self.params.listen)
            return super(SessionBasedDaemon, self)._main()
        else:
            lockfile = os.path.join(self.params.working_dir,
                                    self.name + '.pid')
            lock = PIDLockFile(lockfile)
            if lock.is_locked():
                raise gc3libs.exceptions.FatalError(
                    "PID File %s is already present. Ensure not other daemon"
                    " is running. Delete file to continue." % lockfile)
            context = daemon.DaemonContext(
                working_directory=self.params.working_dir,
                umask=0o002,
                pidfile=lock,
                stdout=open(
                    os.path.join(self.params.working_dir, 'stdout.txt'),
                    'w'),
                stderr=open(
                    os.path.join(self.params.working_dir, 'stderr.txt'),
                    'w'),
            )

            context.signal_map = {signal.SIGTERM: self.cleanup}
            self.log.info("About to daemonize")
            with context:
                self.__setup_logging()
                self.__setup_pollers()
                self.log.info("Daemonizing ...")
                self.__setup_comm(self.params.listen)
                return super(SessionBasedDaemon, self)._main()

    def _main_loop_done(self, rc):
        # Run until interrupted
        return False

    def _main_loop(self):
        """
        The main loop of the application.  It is in a separate
        function so that we can call it just once or properly loop
        around it, as directed by the `self.params.wait` option.

        .. note::

          Overriding this method can disrupt the whole functionality of
          the script, so be careful.

        Invocation of this method should return a numeric exitcode,
        that will be used as the scripts' exitcode.  See
        `_main_loop_exitcode` for an explanation.
        """
        # hook method: this is the method used to add new applications
        # to the session.
        self.every_main_loop()

        # Check if new files were created. 1s timeout
        for poller in self.pollers:
            events = poller.get_events()
            for url, mask in events:
                self.log.debug("Received notify event %s for %s",
                               get_mask_description(mask), url)

                new_jobs = self.new_tasks(self.extra.copy(),
                                          epath=url,
                                          emask=mask)
                self._add_new_tasks(list(new_jobs))
                for task in list(new_jobs):
                    self._controller.add(task)

        # advance all jobs
        self._controller.progress()

        # summary
        stats = self._controller.stats()
        # compute exitcode based on the running status of jobs
        self.session.save_all()
        return self._main_loop_exitcode(stats)

    def _main_loop_exitcode(self, stats):
        # FIXME: Do these exit statuses make sense?
        """
        Compute the exit code for the `_main` function.
        (And, hence, for the whole `SessionBasedScript`.)

        The exitcode is a bitfield; only the 4 least-significant bits
        are used, with the following meaning:

           ===  ============================================================
           Bit  Meaning
           ===  ============================================================
             0  Set if a fatal error occurred: the script could not complete
             1  Set if there are jobs in `FAILED` state
             2  Set if there are jobs in `RUNNING` or `SUBMITTED` state
             3  Set if there are jobs in `NEW` state
           ===  ============================================================

        Override this method if you need to alter the termination
        condition for a `SessionBasedScript`.
        """
        rc = 0
        if stats['failed'] > 0:
            rc |= 2
        if stats[gc3libs.Run.State.RUNNING] > 0 \
                or stats[gc3libs.Run.State.SUBMITTED] > 0 \
                or stats[gc3libs.Run.State.UNKNOWN]:
            rc |= 4
        if stats[gc3libs.Run.State.NEW] > 0:
            rc |= 8
        return rc

    def new_tasks(self, extra, epath=None, emask=0):
        """
        This method is called every time the daemon is started with
        `epath=None` and `emask=0`. Similarly to
        :py:meth:`SessionBasedScript.new_tasks()` method it is
        supposed to return a list of :py:class:`Task()` instances to
        be added to the session.

        It is also called for each event in INBOX (for instance every
        time a file is created), depending on the value of
        `--notify-state` option. In this case the method will be
        called for each file with the file path and the mask of the
        event as arguments.

        :param extra: by default: `self.extra`

        :param epath: an instance of :py:class:`gc3libs.url.Url`
                      containing the path to the updated file or
                      directory :param

        :param emask: mask explaining the event type. This reflects
                      the inotify events, and are defined in
                      :py:const:`gc3libs.poller.events`

        """
        return []
