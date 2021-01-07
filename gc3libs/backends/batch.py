#! /usr/bin/env python

"""
This module provides a generic BatchSystem class from which all
batch-like backends should inherit.
"""
# Copyright (C) 2009-2019   University of Zurich. All rights reserved.
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

from __future__ import absolute_import, print_function, unicode_literals
from builtins import str
__docformat__ = 'reStructuredText'


from collections import namedtuple
from getpass import getuser
import os
import posixpath
import shlex
import sys
import tempfile
import time
import uuid

import gc3libs
from gc3libs import log, Run
import gc3libs.defaults
from gc3libs.backends import LRMS
from gc3libs.utils import same_docstring_as, sh_quote_safe
import gc3libs.backends.transport

# Define some commonly used functions

# FIXME: (Riccardo?) thinks this function is completely wrong and only
# exists to support GAMESS' ``qgms``, which does not allow users to
# specify the name of STDOUT/STDERR files.  When we have a standard
# flexible submission mechanism for all applications, we should remove
# it!


def generic_filename_mapping(jobname, jobid, file_name):
    """
    Map STDOUT/STDERR filenames (as recorded in `Application.outputs`)
    to commonly used default STDOUT/STDERR file names (e.g.,
    ``<jobname>.o<jobid>``).
    """
    try:
        return {('%s.out' % jobname): ('%s.o%s' % (jobname, jobid)),
                ('%s.err' % jobname): ('%s.e%s' % (jobname, jobid)),
                # FIXME: the following is definitely GAMESS-specific
                ('%s.cosmo' % jobname): ('%s.o%s.cosmo' % (jobname, jobid)),
                ('%s.dat' % jobname): ('%s.o%s.dat' % (jobname, jobid)),
                ('%s.inp' % jobname): ('%s.o%s.inp' % (jobname, jobid)),
                ('%s.irc' % jobname): ('%s.o%s.irc' % (jobname, jobid))
                }[file_name]
    except KeyError:
        return file_name


def _make_remote_and_local_path_pair(transport, job, remote_relpath,
                                     local_root_dir, local_relpath):
    """
    Return list of (remote_path, local_path) pairs corresponding to
    """
    # see https://github.com/fabric/fabric/issues/306 about why it is
    # correct to use `posixpath.join` for remote paths (instead of
    # `os.path.join`)
    remote_path = posixpath.join(job.ssh_remote_folder,
                                 generic_filename_mapping(job.lrms_jobname,
                                                          job.lrms_jobid,
                                                          remote_relpath))
    local_path = os.path.join(local_root_dir, local_relpath)
    if transport.isdir(remote_path):
        # recurse, accumulating results
        result = []
        for entry in transport.listdir(remote_path):
            result += _make_remote_and_local_path_pair(
                transport, job,
                posixpath.join(remote_relpath, entry),
                local_path, entry)
        return result
    else:
        return [(remote_path, local_path)]


class BatchSystem(LRMS):

    """
    Base class for backends dealing with a batch-queue system (e.g.,
    PBS/TORQUE, Grid Engine, etc.)

    This is an abstract class, that you should subclass in order to
    interface with a given batch queuing system. (Remember to call
    this class' constructor in the derived class ``__init__`` method.)

    """

    _batchsys_name = 'batch queueing system'
    """
    A human-readable identifier for the batch queueing system
    class/type (e.g., PBS, LSF, etc.).
    """

    def __init__(self, name,
                 # this are inherited from the base LRMS class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime,
                 auth,  # ignored if `transport` is 'local'
                 # these are specific to the this backend
                 frontend, transport,
                 accounting_delay=15,
                 # SSH-related options; ignored if `transport` is 'local'
                 ssh_config=None,
                 keyfile=None,
                 ignore_ssh_host_keys=False,
                 ssh_timeout=None,
                 large_file_threshold=None,
                 large_file_chunk_size=None,
                 spooldir=gc3libs.defaults.SPOOLDIR,
                 **extra_args):

        # init base class
        LRMS.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth, **extra_args)

        # backend-specific setup
        self.frontend = frontend
        self.spooldir = spooldir
        if transport == 'local':
            self.transport = gc3libs.backends.transport.LocalTransport()
            self._username = getuser()
        elif transport == 'ssh':
            auth = self._auth_fn()
            self._username=auth.username
            self.transport = gc3libs.backends.transport.SshTransport(
                frontend,
                ignore_ssh_host_keys=ignore_ssh_host_keys,
                ssh_config=(ssh_config or auth.ssh_config),
                username=self._username,
                port=auth.port,
                keyfile=(keyfile or auth.keyfile),
                pkey=auth.pkey,
                timeout=(ssh_timeout or auth.timeout),
                large_file_threshold=large_file_threshold,
                large_file_chunk_size=large_file_chunk_size,
            )
        else:
            raise gc3libs.exceptions.TransportError(
                "Unknown transport '%s'" % transport)
        self.accounting_delay = accounting_delay


    def get_jobid_from_submit_output(self, output, regexp):
        """Parse the output of the submission command. Regexp is
        provided by the caller. """
        for line in output.split('\n'):
            match = regexp.match(line)
            if match:
                return match.group('jobid')
        raise gc3libs.exceptions.InternalError(
            "Could not extract jobid from submission command output `{0}`"
            .format(output.rstrip()))

    def _get_command_argv(self, name, default=None):
        """
        Return an *argv*-style array for invoking command `name`.

        The command name is looked up in this resource's configuration
        parameters, and, if found, the associated string is split into
        words (according to the normal POSIX shell rules) and this
        list of words is returned::

          | >>> b = BatchSystem(..., bsub='/usr/local/bin/bsub -R lustre')
          | >>> b._get_command('bsub')
          | ['/usr/local/bin/bsub', '-R', 'lustre']

        Otherwise, if no configuration parameter by name `name` is
        found, then second argument `default` is returned as the sole
        member of the *argv*-array::

          | >>> b = BatchSystem(...)
          | >>> b._get_command('foo', 'bar')
          | ['bar']

        If `default` is `None`, then the value of `name` is used
        instead::

          | >>> b._get_command('baz')
          | ['baz']

        """
        if default is None:
            default = name
        # lookup the command name in the resource config parameters;
        # return it unchanged as a default
        cmd = self.get(name, default)
        # return argv-style array
        return shlex.split(cmd, comments=True)

    def _get_command(self, name, default=None):
        """
        Return an command-line (string) for invoking command `name`.

        The command name is looked up in this resource's configuration
        parameters, and, if found, the associated string is returned::

          | >>> b = BatchSystem(..., bsub='/usr/local/bin/bsub -R lustre')
          | >>> b._get_command('bsub')
          | '/usr/local/bin/bsub -R lustre'

        Otherwise, if no configuration parameter by name `name` is
        found, then second argument `default` is returned::
        member of the *argv*-array::

          | >>> b = BatchSystem(...)
          | >>> b._get_command('foo', 'bar')
          | 'bar'

        If `default` is `None`, then the value of `name` is used
        instead::

          | >>> b._get_command('baz')
          | 'baz'

        """
        if default is None:
            default = name
        # lookup the command name in the resource config parameters;
        # return it unchanged as a default
        argv = self._get_command_argv(name, default)
        return ' '.join(argv)

    def _submit_command(self, app):
        """This method returns a string containing the command to
        issue to submit the job."""
        raise NotImplementedError(
            "Abstract method `_submit_command()` called - "
            "this should have been defined in a derived class.")

    def _parse_submit_output(self, stdout):
        """This method will parse the output of the submit command and
        return the jobid of the submitted job."""
        raise NotImplementedError(
            "Abstract method `parse_submit_output()` called - "
            "this should have been defined in a derived class.")

    def _stat_command(self, job):
        """This method returns a string containing the command to
        issue to get status information about a job."""
        raise NotImplementedError(
            "Abstract method `_stat_command()` called - "
            "this should have been defined in a derived class.")

    def _parse_stat_output(self, stdout):
        """
        Parse the output of the "stat" command and return the current
        status of the job and its exit status.

        :return: A `_stat_result`:class: instance.
        """
        raise NotImplementedError(
            "Abstract method `_parse_stat_output()` called - "
            "this should have been defined in a derived class.")

    # the only purpose of this class definition is to add a docstring
    # to the named tuple
    class _stat_result(namedtuple('_stat_result', ['state', 'termstatus'])):
        """
        Result of parsing the "stat" command output.
        It is a pair ``(state, termstatus)``, where:

        * ``state`` is a valid GC3Pie task state
          (i.e., instance of `Run.State`:class:, which see);

        * ``termstatus`` is either ``None`` (to indicate that no
          termination status information is available), or any value
          that can be used to set GC3Pie's `Run.returncode`:attr: (for
          instance, a 16-bit POSIX process termination status).

        The ``termstatus`` part is only meaningful when ``state``
        is ``TERMINATING``.
        """
        pass

    def _acct_command(self, job):
        """
        Return a string containing the command to issue to get accounting
        information about the `job`.

        It is usually called only if the _stat_command() fails.
        """
        raise NotImplementedError(
            "Abstract method `_acct_command()` called - "
            "this should have been defined in a derived class.")

    def _parse_acct_output(self, stdout):
        """
        Parse the output of `_acct_command` and return a dictionary
        containing infos about the job.

        The `BatchSystem`:class: does not make any assumption about
        the keys contained in the dictionary.
        """
        raise NotImplementedError(
            "Abstract method `_parse_acct_output()` called - "
            "this should have been defined in a derived class.")

    def _secondary_acct_command(self, job):
        """
        Like `_acct_command` but called only if it exits with non-0 status.

        This is used to allow for a fallback method in case there are
        multiple ways to get information from a job.

        By default this method does nothing, as most batch systems do
        only have one and one only way of getting accounting
        information.
        """
        return None

    def _parse_secondary_acct_output(self, stdout):
        """
        Parse the output of `_secondary_acct_command` and return a
        dictionary containing infos about the job.

        The `BatchSystem` class does not make any assumption about
        the keys contained in the dictionary.
        """
        raise NotImplementedError(
            "Abstract method `_parse_secondary_acct_output()` called - "
            "this should have been defined in a derived class.")

    def _cancel_command(self, jobid):
        """This method returns a string containing the command to
        issue to delete the job identified by `jobid`
        """
        raise NotImplementedError(
            "Abstract method `_cancel_command()` called -"
            " this should have been defined in a derived class.")

    def _get_prepost_scripts(self, app, scriptnames):
        script_txt = []
        for script in scriptnames:
            if script not in self:
                gc3libs.log.debug(
                    "%s script not defined for resource %s", script, self.name)
                continue
            if os.path.isfile(self[script]):
                gc3libs.log.debug(
                    "Adding %s file `%s` to the submission script"
                    % (script, self[script]))
                script_file = open(self[script])
                script_txt.append("\n# %s file `%s` BEGIN\n"
                                  % (script, self[script]))
                script_txt.append(script_file.read())
                script_txt.append("\n# %s file END\n" % script)
                script_file.close()
            else:
                # The *content* of the variable will be inserted in
                # the script.
                script_txt.append('\n# inline script BEGIN\n')
                script_txt.append(self[script])
                script_txt.append('\n# inline script END\n')
        return "".join(script_txt)

    def get_prologue_script(self, app):
        """
        This method will get the prologue script(s) for the `app`
        application and will return a string which contains the
        contents of the script(s) merged together.
        """
        prologues = ['prologue', app.application_name + '_prologue',
                     'prologue_content',
                     app.application_name + '_prologue_content']
        return self._get_prepost_scripts(app, prologues)

    def get_epilogue_script(self, app):
        """
        This method will get the epilogue script(s) for the `app`
        application and will return a string which contains the
        contents of the script(s) merged together.
        """
        epilogues = ['epilogue', app.application_name + '_epilogue',
                     'epilogue_content',
                     app.application_name + '_epilogue_content']
        return self._get_prepost_scripts(app, epilogues)

    @LRMS.authenticated
    def submit_job(self, app):
        """This method will create a remote directory to store job's
        sandbox, and will copy the sandbox in there.
        """
        job = app.execution

        # Create the remote directory.
        self.transport.connect()
        cmd = (
            "mkdir -p {0};"
            " mktemp -d {0}/batch_job.XXXXXXXXXX"
            .format(self.spooldir))
        exit_code, stdout, stderr = self.transport.execute_command(cmd)
        if exit_code != 0:
            raise gc3libs.exceptions.SpoolDirError(
                "Cannot create temporary job working directory"
                " on resource '%s'; command '%s' exited"
                " with code: %d and stderr: '%s'."
                % (self.name, cmd, exit_code, stderr))
        ssh_remote_folder = stdout.split('\n')[0]

        # Copy the input file(s) to remote directory.
        for local_path, remote_path in list(app.inputs.items()):
            remote_path = os.path.join(ssh_remote_folder, remote_path)
            remote_parent = os.path.dirname(remote_path)
            try:
                if remote_parent not in ['', '.']:
                    log.debug("Making remote directory '%s'",
                              remote_parent)
                    self.transport.makedirs(remote_parent)
                log.debug("Transferring file '%s' to '%s'",
                          local_path.path, remote_path)
                self.transport.put(local_path.path, remote_path)
                # preserve execute permission on input files
                if os.access(local_path.path, os.X_OK):
                    self.transport.chmod(remote_path, 0o755)
            except:
                log.critical(
                    "Copying input file '%s' to remote cluster '%s' failed",
                    local_path.path, self.frontend)
                raise

        if app.arguments[0].startswith('./'):
            gc3libs.log.debug("Making remote path '%s' executable.",
                              app.arguments[0])
            self.transport.chmod(os.path.join(ssh_remote_folder,
                                              app.arguments[0]), 0o755)

        # if STDOUT/STDERR should be saved in a directory, ensure it
        # exists (see Issue 495 for details)
        for dest in (app.stdout, app.stderr):
            if dest:
                destdir = os.path.dirname(dest)
                if destdir:
                    self.transport.makedirs(
                        posixpath.join(ssh_remote_folder, destdir))

        try:
            sub_cmd, aux_script = self._submit_command(app)
            if aux_script != '':
                # create temporary script name
                script_filename = ('./script.%s.sh' % uuid.uuid4())
                # save script to a temporary file and submit that one instead
                local_script_file = tempfile.NamedTemporaryFile(mode='wt')
                local_script_file.write('#!/bin/sh\n')
                # Add preamble file
                prologue = self.get_prologue_script(app)
                if prologue:
                    local_script_file.write(prologue)

                local_script_file.write(aux_script)

                # Add epilogue files
                epilogue = self.get_epilogue_script(app)
                if epilogue:
                    local_script_file.write(epilogue)

                local_script_file.flush()
                # upload script to remote location
                self.transport.put(
                    local_script_file.name,
                    os.path.join(ssh_remote_folder, script_filename))
                # set execution mode on remote script
                self.transport.chmod(
                    os.path.join(ssh_remote_folder, script_filename), 0o755)
                # cleanup
                local_script_file.close()
                if os.path.exists(local_script_file.name):
                    os.unlink(local_script_file.name)
            else:
                # we still need a script name even if there is no
                # script to submit
                script_filename = ''

            # Submit it
            exit_code, stdout, stderr = self.transport.execute_command(
                "/bin/sh -c %s" % sh_quote_safe('cd %s && %s %s' % (
                    ssh_remote_folder, sub_cmd, script_filename)))

            if exit_code != 0:
                raise gc3libs.exceptions.LRMSError(
                    "Failed executing command 'cd %s && %s %s' on resource"
                    " '%s'; exit code: %d, stderr: '%s'."
                    % (ssh_remote_folder, sub_cmd, script_filename,
                       self.name, exit_code, stderr))

            jobid = self._parse_submit_output(stdout)
            log.debug('Job submitted with jobid: %s', jobid)

            job.execution_target = self.frontend

            job.lrms_jobid = jobid
            job.lrms_jobname = jobid
            try:
                if app.jobname:
                    job.lrms_jobname = app.jobname
            except:
                pass

            if 'stdout' in app:
                job.stdout_filename = app.stdout
            else:
                job.stdout_filename = '%s.o%s' % (job.lrms_jobname, jobid)
            if app.join:
                job.stderr_filename = job.stdout_filename
            else:
                if 'stderr' in app:
                    job.stderr_filename = app.stderr
                else:
                    job.stderr_filename = '%s.e%s' % (job.lrms_jobname, jobid)
            job.history.append('Submitted to %s @ %s, got jobid %s'
                               % (self._batchsys_name, self.name, jobid))
            job.history.append("Submission command output:\n"
                               "  === stdout ===\n%s"
                               "  === stderr ===\n%s"
                               "  === end ===\n"
                               % (stdout, stderr), 'pbs', 'qsub')
            job.ssh_remote_folder = ssh_remote_folder

            return job

        except:
            log.critical(
                "Failure submitting job to resource '%s' - "
                "see log file for errors", self.name)
            raise


    def __run_command_and_parse_output(self, cmd, parser, kind='accounting'):
        log.debug("Checking remote job %s info with `%s` ...", kind, cmd)
        exit_code, stdout, stderr = self.transport.execute_command(cmd)
        if exit_code == 0:
            return parser(stdout, stderr)
        else:
            raise gc3libs.exceptions.AuxiliaryCommandError(
                "Failed running %s command `%s`:"
                " exit code: %d, stderr: '%s'"
                % (kind, cmd, exit_code, stderr),
                do_log=True)


    @same_docstring_as(LRMS.update_job_state)
    @LRMS.authenticated
    def update_job_state(self, app):
        job = app.execution
        try:
            job.lrms_jobid
        except AttributeError as ex:
            # `job` has no `lrms_jobid`: object is invalid
            raise gc3libs.exceptions.InvalidArgument(
                "Job object is invalid: {ex}".format(ex=ex))

        self.transport.connect()

        cmd = self._stat_command(job)
        try:
            state, termstatus = self.__run_command_and_parse_output(
                cmd, self._parse_stat_output, 'status')
            if state != Run.State.TERMINATING:
                # no need to go further and parse acct info; also,
                # exit status is not relevant in this case
                job.state = state
                log.debug("Task %s state set to %s", app, state)
                return state
        except gc3libs.exceptions.AuxiliaryCommandError:
            # use the special state value ``None`` to signal that
            # the "status" command failed, we might need this
            # after the "acct" command has run
            state, termstatus = None, None
        assert state is None or state == Run.State.TERMINATING
        log.debug("Job status command gave state `%s`"
                  " and termination status `%s` for task %s",
                  state, termstatus, app)

        # In some batch systems, jobs disappear from qstat
        # output as soon as they are finished. In these cases,
        # we have to check some *accounting* command to check
        # the exit status.
        acctinfo = {}
        for cmd_fn, parse_fn in [
                # this is the regular sacct/qacct/bjobs command
                (self._acct_command, self._parse_acct_output),
                # This is used to distinguish between a standard
                # Torque installation and a PBSPro where `tracejob`
                # does not work but if `job_history_enable=True`,
                # then we can actually access information about
                # finished jobs with `qstat -x -f`.
                (self._secondary_acct_command, self._parse_secondary_acct_output),
        ]:
            cmd = cmd_fn(job)
            # `._secondary_acct_command` returns ``None`` if no
            # "secondary" accouting method is defined -- skip to next
            # iteration, if any
            if cmd is None:
                continue
            try:
                acctinfo = self.__run_command_and_parse_output(
                    cmd, parse_fn, 'accounting')
                # use info from the first acct command that succeeds
                if acctinfo:
                    log.debug("Gathered accounting info %r for task %s",
                              acctinfo, app)
                    break
            except gc3libs.exceptions.AuxiliaryCommandError:
                log.debug("Accounting command `%s` failed.", cmd)
                # try next one
                pass
            except gc3libs.exceptions.UnexpectedJobState as ex:
                log.debug(
                    "Unexpected output from accounting command `%s`: %s.",
                    cmd, ex)
                # try next one
                pass

        # if no termination status is known and the acct
        # command provided one, use it
        if 'termstatus' in acctinfo:
            # if we have a termination status, then the job has terminated
            state = Run.State.TERMINATING
            if termstatus is None:
                termstatus = acctinfo['termstatus']
            else:
                # this should not happen!  but one never knows how new
                # versions of the software may break old habits and
                # parsing rules, so better fail loudly here so we get
                # a bug report and a chance to fix...
                assert termstatus == acctinfo['termstatus'], (
                    "Status and accounting commands disagree"
                    " on job termination status: {1} vs {2}"
                    .format(termstatus, acctinfo['termstatus'])
                )

        if termstatus is None:
            # No *stat command and no *acct command returned correctly.
            try:
                job.stat_failed_at
            except AttributeError:
                # this is the first time `qstat` fails, record a
                # timestamp and retry later
                job.stat_failed_at = time.time()
                return job.state

            if (time.time() - job.stat_failed_at) <= self.accounting_delay:
                # do nothing, let's try later...
                return job.state
            else:
                # accounting info should be there, if it's not
                # then job is definitely lost
                job.state = Run.State.UNKNOWN
                raise gc3libs.exceptions.LRMSError(
                    "Could not retrieve status information for task {app}"
                    .format(app=app))

        # if we got to this point the job is in TERMINATING state
        # and we know at least the termination status
        assert state == Run.State.TERMINATING

        job.state = state
        job.returncode = termstatus
        job.update(acctinfo)

        return state

    @same_docstring_as(LRMS.peek)
    @LRMS.authenticated
    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        job = app.execution
        assert 'ssh_remote_folder' in job, \
            "Missing attribute `ssh_remote_folder` on `Job` instance" \
            " passed to `PbsLrms.peek`."

        if size is None:
            size = sys.maxsize

        _filename_mapping = generic_filename_mapping(
            job.lrms_jobname, job.lrms_jobid, remote_filename)
        _remote_filename = os.path.join(
            job.ssh_remote_folder, _filename_mapping)

        try:
            self.transport.connect()
            remote_handler = self.transport.open(
                _remote_filename, mode='r', bufsize=-1)
            remote_handler.seek(offset)
            data = remote_handler.read(size)
        except Exception as ex:
            log.error("Could not read remote file '%s': %s: %s",
                      _remote_filename, ex.__class__.__name__, str(ex))

        try:
            local_file.write(data)
        except (TypeError, AttributeError):
            output_file = open(local_file, 'w+b')
            output_file.write(data)
            output_file.close()
        log.debug('... Done.')

    @same_docstring_as(LRMS.validate_data)
    def validate_data(self, data_file_list):
        """
        Supported protocols: file
        """
        for url in data_file_list:
            if url.scheme not in ['file']:
                return False
        return True

    @same_docstring_as(LRMS.cancel_job)
    @LRMS.authenticated
    def cancel_job(self, app):
        job = app.execution
        try:
            self.transport.connect()
            cmd = self._cancel_command(job.lrms_jobid)
            exit_code, stdout, stderr = self.transport.execute_command(cmd)
            if exit_code != 0:
                # XXX: It is possible that 'qdel' fails because job
                # has been already completed thus the cancel_job
                # behaviour should be tolerant to these errors.
                log.error(
                    "Failed executing remote command '%s'; exit status %d",
                    cmd, exit_code)
                log.debug("  remote command returned STDOUT '%s'", stdout)
                log.debug("  remote command returned STDERR '%s'", stderr)
                if exit_code == 127:
                    # command was not executed, time to signal an exception
                    raise gc3libs.exceptions.LRMSError(
                        "Cannot execute remote command '%s'"
                        " -- See DEBUG level log for details" % (cmd,))
            return job
        except:
            log.critical('Failure checking status')
            raise

    @same_docstring_as(LRMS.free)
    @LRMS.authenticated
    def free(self, app):

        job = app.execution
        try:
            self.transport.connect()
            self.transport.remove_tree(job.ssh_remote_folder)
        except Exception as err:
            log.warning("Failed removing remote folder '%s': %s: %s",
                        job.ssh_remote_folder, err.__class__, err)
        return

    @same_docstring_as(LRMS.get_results)
    @LRMS.authenticated
    def get_results(self, app, download_dir,
                    overwrite=False, changed_only=True):
        if app.output_base_url is not None:
            raise gc3libs.exceptions.UnrecoverableDataStagingError(
                "Retrieval of output files to non-local destinations"
                " is not supported (yet).")

        job = app.execution
        try:
            self.transport.connect()
            # Make list of files to copy, in the form of (remote_path,
            # local_path) pairs.  This entails walking the
            # `Application.outputs` list to expand wildcards and
            # directory references.
            stageout = list()
            for remote_relpath, local_url in app.outputs.items():
                local_relpath = local_url.path
                if remote_relpath == gc3libs.ANY_OUTPUT:
                    remote_relpath = ''
                    local_relpath = ''
                stageout += _make_remote_and_local_path_pair(
                    self.transport, job, remote_relpath, download_dir,
                    local_relpath)

            # copy back all files, renaming them to adhere to the
            # ArcLRMS convention
            log.debug("Downloading job output into '%s' ...", download_dir)
            for remote_path, local_path in stageout:
                # ignore missing files (this is what ARC does too)
                self.transport.get(remote_path, local_path,
                                   ignore_nonexisting=True,
                                   overwrite=overwrite,
                                   changed_only=changed_only)
            return

        except:
            raise

    @same_docstring_as(LRMS.validate_data)
    @LRMS.authenticated
    def close(self):
        self.transport.close()


if "__main__" == __name__:
    import doctest
    doctest.testmod(name="sge",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
