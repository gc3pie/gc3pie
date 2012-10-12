#! /usr/bin/env python
#
"""
This module provides a generic BatchSystem class from which all
batch-like backends should inherit.
"""
# Copyright (C) 2009-2012 GC3, University of Zurich. All rights reserved.
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
__version__ = '2.0.0 version (SVN $Revision$)'


from getpass import getuser
import os
import posixpath
import random
import shlex
import stat
import sys
import tempfile
import time

import gc3libs
from gc3libs import log, Run
from gc3libs.backends import LRMS
from gc3libs.utils import ifelse, same_docstring_as
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
        return {
            # XXX: PBS/SGE-specific?
            ('%s.out' % jobname) : ('%s.o%s' % (jobname, jobid)),
            ('%s.err' % jobname) : ('%s.e%s' % (jobname, jobid)),
            # FIXME: the following is definitely GAMESS-specific
            ('%s.cosmo' % jobname) : ('%s.o%s.cosmo' % (jobname, jobid)),
            ('%s.dat'   % jobname) : ('%s.o%s.dat'   % (jobname, jobid)),
            ('%s.inp'   % jobname) : ('%s.o%s.inp'   % (jobname, jobid)),
            ('%s.irc'   % jobname) : ('%s.o%s.irc'   % (jobname, jobid)),
            }[file_name]
    except KeyError:
        return file_name


def _make_remote_and_local_path_pair(transport, job, remote_relpath, local_root_dir, local_relpath):
    """
    Return list of (remote_path, local_path) pairs corresponding to
    """
    # see https://github.com/fabric/fabric/issues/306 about why it is
    # correct to use `posixpath.join` for remote paths (instead of `os.path.join`)
    remote_path = posixpath.join(job.ssh_remote_folder,
                                 generic_filename_mapping(job.lrms_jobname, job.lrms_jobid,
                                                       remote_relpath))
    local_path = os.path.join(local_root_dir, local_relpath)
    if transport.isdir(remote_path):
        # recurse, accumulating results
        result = [ ]
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
                 auth, # ignored if `transport` is 'local'
                 # these are specific to the this backend
                 frontend, transport,
                 accounting_delay = 15,
                 **extra_args):

        # init base class
        LRMS.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth, **extra_args)

        # backend-specific setup
        self.frontend = frontend
        if transport == 'local':
            self.transport = gc3libs.backends.transport.LocalTransport()
            self._username = getuser()
        elif transport == 'ssh':
            auth = self._auth_fn()
            self._username = auth.username
            self.transport = gc3libs.backends.transport.SshTransport(
                frontend, username=self._username)
        else:
            raise gc3libs.exceptions.TransportError(
                "Unknown transport '%s'" % transport)


    def get_jobid_from_submit_output(self, output, regexp):
        """Parse the output of the submission command. Regexp is
        provided by the caller. """
        for line in output.split('\n'):
            match = regexp.match(line)
            if match:
                return match.group('jobid')
        raise gc3libs.exceptions.InternalError("Could not extract jobid from qsub output '%s'"
                            % qsub_output.rstrip())

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
        return str.join(' ', argv)

    def _submit_command(self, app):
        """This method returns a string containing the command to
        issue to submit the job."""
        raise NotImplementedError("Abstract method `_submit_command()` called - this should have been defined in a derived class.")

    def _parse_submit_output(self, stdout):
        """This method will parse the output of the submit command and
        return the jobid of the submitted job."""
        raise NotImplementedError("Abstract method `parse_submit_output()` called - this should have been defined in a derived class.")

    def _stat_command(self, job):
        """This method returns a string containing the command to
        issue to get status information about a job."""
        raise NotImplementedError("Abstract method `_stat_command()` called - this should have been defined in a derived class.")

    def _parse_stat_output(self, stdout):
        """This method will parse the output of the stat command and
        return the current status of the job. The return value will be
        a dictionary which will be used to update job's information.

        The only expected key is `state`, which must be a valid
        `Run.State` state.
        """
        raise NotImplementedError("Abstract method `_parse_stat_output()` called - this should have been defined in a derived class.")

    def _acct_command(self, job):
        """This method returns a string containing the command to
        issue to get accounting information about the `job`.

        It is usually called only if the _stat_command() fails.
        """
        raise NotImplementedError("Abstract method `_acct_command()` called - this should have been defined in a derived class.")

    def _parse_acct_output(self, stdout):
        """This method will parse the output of the acct command and
        return a dictionary containing infos about the
        job. `BatchSystem` class does not make any assumption about
        the keys contained in the dictionary.
        """
        raise NotImplementedError("Abstract method `_parse_acct_output()` called - this should have been defined in a derived class.")

    def _cancel_command(self, jobid):
        """This method returns a string containing the command to
        issue to delete the job identified by `jobid`
        """
        raise NotImplementedError("Abstract method `_cancel_command()` called - this should have been defined in a derived class.")

    def _get_prepost_scripts(self, app, scriptnames):
        script_txt = []
        for script in scriptnames:
            if script in self and os.path.isfile(self[script]):
                gc3libs.log.debug("Adding %s file `%s` to the submission script" % (script, self[script]))
                script_file = open(self[script])
                script_txt.append("\n# %s file `%s` BEGIN\n" % (script, self[script]))
                script_txt.append(script_file.read())
                script_txt.append("\n# %s file END\n" % script)
                script_file.close()
        return str.join("", script_txt)

    def get_prologue_script(self, app):
        """
        This method will get the prologue script(s) for the `app`
        application and will return a string which contains the
        contents of the script(s) merged together.
        """
        prologues = ['prologue', app.application_name+'_prologue']
        return self._get_prepost_scripts(app, prologues)

    def get_epilogue_script(self, app):
        """
        This method will get the epilogue script(s) for the `app`
        application and will return a string which contains the
        contents of the script(s) merged together.
        """
        epilogues = ['epilogue', app.application_name+'epilogue']
        return self._get_prepost_scripts(app, epilogues)

    @LRMS.authenticated
    def submit_job(self, app):
        """This method will create a remote directory to store job's
        sandbox, and will copy the sandbox in there.
        """
        job = app.execution
        # Create the remote directory.
        try:
            self.transport.connect()

            _command = 'mkdir -p $HOME/.gc3pie_jobs; mktemp -p $HOME/.gc3pie_jobs -d lrms_job.XXXXXXXXXX'
            log.info("Creating remote temporary folder: command '%s' " % _command)
            exit_code, stdout, stderr = self.transport.execute_command(_command)
            if exit_code == 0:
                ssh_remote_folder = stdout.split('\n')[0]
            else:
                raise gc3libs.exceptions.LRMSError(
                    "Failed executing command '%s' on resource '%s';"
                    " exit code: %d, stderr: '%s'."
                    % (_command, self.name, exit_code, stderr))
        except gc3libs.exceptions.TransportError, x:
            raise
        except:
            raise
                # Copy the input file to remote directory.
        for local_path,remote_path in app.inputs.items():
            remote_path = os.path.join(ssh_remote_folder, remote_path)
            remote_parent = os.path.dirname(remote_path)
            try:
                if remote_parent not in ['', '.']:
                    log.debug("Making remote directory '%s'" % remote_parent)
                    self.transport.makedirs(remote_parent)
                log.debug("Transferring file '%s' to '%s'" % (local_path.path, remote_path))
                self.transport.put(local_path.path, remote_path)
                # preserve execute permission on input files
                if os.access(local_path.path, os.X_OK):
                    self.transport.chmod(remote_path, 0755)
            except:
                log.critical("Copying input file '%s' to remote cluster '%s' failed",
                                      local_path.path, self.frontend)
                raise

        if app.arguments[0].startswith('./'):
            gc3libs.log.debug("Making remote path '%s' executable.",
                              app.arguments[0])
            self.transport.chmod(os.path.join(ssh_remote_folder,
                                              app.arguments[0]), 0755)

        try:
            sub_cmd, aux_script = self._submit_command(app)
            if aux_script != '':
                # create temporary script name
                # XXX: The `uuid` module is available from Py 2.5 onwards
                script_filename = ('./script.%x.sh' % random.randint(0, sys.maxint))
                # save script to a temporary file and submit that one instead
                local_script_file = tempfile.NamedTemporaryFile()
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
                self.transport.put(local_script_file.name,
                                   os.path.join(ssh_remote_folder, script_filename))
                # set execution mode on remote script
                self.transport.chmod(os.path.join(ssh_remote_folder, script_filename), 0755)
                # cleanup
                local_script_file.close()
                if os.path.exists(local_script_file.name):
                    os.unlink(local_script_file.name)
            else:
                # we still need a script name even if there is no script to submit
                script_filename = ''

            # Submit it
            exit_code, stdout, stderr = self.transport.execute_command(
                "/bin/sh -c 'cd %s && %s %s'"
                % (ssh_remote_folder, sub_cmd, script_filename))

            if exit_code != 0:
                raise gc3libs.exceptions.LRMSError(
                    "Failed executing command '%s' on resource '%s';"
                    " exit code: %d, stderr: '%s'."
                    % (_command, self.name, exit_code, stderr))

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
            log.critical("Failure submitting job to resource '%s' - see log file for errors"
                                  % self.name)
            raise


    @same_docstring_as(LRMS.update_job_state)
    @LRMS.authenticated
    def update_job_state(self, app):
        try:
            job = app.execution
            job.lrms_jobid
        except AttributeError, ex:
            # `job` has no `lrms_jobid`: object is invalid
            raise gc3libs.exceptions.InvalidArgument("Job object is invalid: %s" % str(ex))

        try:
            self.transport.connect()
            _command = self._stat_command(job)
            log.debug("Checking remote job status with '%s' ..." % _command)
            exit_code, stdout, stderr = self.transport.execute_command(_command)
            if exit_code == 0:
                jobstatus = self._parse_stat_output(stdout)
                job.update(jobstatus)

                job.state = jobstatus.get('state', Run.State.UNKNOWN)
                if job.state == Run.State.UNKNOWN:
                    log.warning(
                        "Unknown batch job status,"
                        " setting GC3Pie job state to `UNKNOWN`")

                if 'exit_status' in jobstatus:
                    job.exitcode = int(jobstatus['exit_status'])
                    # XXX: we should set the `signal` part accordingly
                    job.signal = 0

                # SLURM's `squeue` command exits with code 0 if the job ID exists
                # in the database (i.e., a job with that ID has been run) but prints
                # no output.  In this case, we need to continue and examine the
                # accounting command output to get the termination status etc.
                if job.state != Run.State.TERMINATING:
                    return job.state

            # In some batch systems, jobs disappear from qstat
            # output as soon as they are finished. In these cases,
            # we have to check some *accounting* command to check
            # the exit status.
            _command = self._acct_command(job)
            if _command:
                log.debug("The `qstat`/`bjobs` command returned no job information;"
                          " trying with '%s' instead ..." % _command)
                exit_code, stdout, stderr = self.transport.execute_command(_command)
                if exit_code == 0:
                    jobstatus = self._parse_acct_output(stdout)
                    job.update(jobstatus)
                    if 'exitcode' in jobstatus:
                        job.returncode = int(jobstatus['exitcode'])
                        job.state = Run.State.TERMINATING
                    return job.state

            # No *stat command and no *acct command returned
            # correctly.
            try:
                if (time.time() - job.stat_failed_at) > self.accounting_delay:
                    # accounting info should be there, if it's not then job is definitely lost
                    log.critical("Failed executing remote command: '%s'; exit status %d"
                                 % (_command, exit_code))
                    log.debug("  remote command returned stdout: '%s'" % stdout)
                    log.debug("  remote command returned stderr: '%s'" % stderr)
                    raise gc3libs.exceptions.LRMSError(
                        "Failed executing remote command: '%s'; exit status %d"
                        % (_command,exit_code))
                else:
                    # do nothing, let's try later...
                    return job.state
            except AttributeError:
                # this is the first time `qstat` fails, record a timestamp and retry later
                job.stat_failed_at = time.time()

        except Exception, ex:
            log.error("Error in querying Batch resource '%s': %s: %s",
                    self.name, ex.__class__.__name__, str(ex))
            raise
        # If we reach this point it means that we don't actually know
        # the current state of the job.
        job.state = Run.State.UNKNOWN
        return job.state

    @same_docstring_as(LRMS.peek)
    @LRMS.authenticated
    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        job = app.execution
        assert job.has_key('ssh_remote_folder'), \
            "Missing attribute `ssh_remote_folder` on `Job` instance passed to `PbsLrms.peek`."

        if size is None:
            size = sys.maxint

        _filename_mapping = generic_filename_mapping(job.lrms_jobname, job.lrms_jobid, remote_filename)
        _remote_filename = os.path.join(job.ssh_remote_folder, _filename_mapping)

        try:
            self.transport.connect()
            remote_handler = self.transport.open(_remote_filename, mode='r', bufsize=-1)
            remote_handler.seek(offset)
            data = remote_handler.read(size)
        except Exception, ex:
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
            if not url.scheme in ['file']:
                return False
        return True

    @same_docstring_as(LRMS.cancel_job)
    @LRMS.authenticated
    def cancel_job(self, app):
        job = app.execution
        try:
            self.transport.connect()
            _command = self._cancel_command(job.lrms_jobid)
            exit_code, stdout, stderr = self.transport.execute_command(_command)
            if exit_code != 0:
                # It is possible that 'qdel' fails because job has been already completed
                # thus the cancel_job behaviour should be to
                log.error("Failed executing remote command '%s'; exit status %d",
                          _command, exit_code)
                log.debug("  remote command returned stdout '%s'", stdout)
                log.debug("  remote command returned stderr '%s'", stderr)
                if exit_code == 127:
                    # no such command
                    raise gc3libs.exceptions.LRMSError(
                        "Cannot execute remote command '%s'" % _command)

            return job

        except:
            log.critical('Failure in checking status')
            raise

    @same_docstring_as(LRMS.free)
    @LRMS.authenticated
    def free(self, app):

        job = app.execution
        try:
            self.transport.connect()
            self.transport.remove_tree(job.ssh_remote_folder)
        except:
            log.warning("Failed removing remote folder '%s': %s: %s"
                        % (job.ssh_remote_folder, sys.exc_info()[0], sys.exc_info()[1]))
        return

    @same_docstring_as(LRMS.get_results)
    @LRMS.authenticated
    def get_results(self, app, download_dir, overwrite=False):
        if app.output_base_url is not None:
            raise gc3libs.exceptions.UnrecoverableDataStagingError(
                "Retrieval of output files to non-local destinations"
                " is not supported (yet).")

        job = app.execution
        try:
            self.transport.connect()
            # Make list of files to copy, in the form of (remote_path, local_path) pairs.
            # This entails walking the `Application.outputs` list to expand wildcards
            # and directory references.
            stageout = [ ]
            for remote_relpath, local_url in app.outputs.iteritems():
                local_relpath = local_url.path
                if remote_relpath == gc3libs.ANY_OUTPUT:
                    remote_relpath = ''
                    local_relpath = ''
                stageout += _make_remote_and_local_path_pair(
                    self.transport, job, remote_relpath, download_dir, local_relpath)

            # copy back all files, renaming them to adhere to the ArcLRMS convention
            log.debug("Downloading job output into '%s' ...", download_dir)
            for remote_path, local_path in stageout:
                log.debug("Downloading remote file '%s' to local file '%s'",
                          remote_path, local_path)
                if (overwrite
                    or not os.path.exists(local_path)
                    or os.path.isdir(local_path)):
                    log.debug("Copying remote '%s' to local '%s'"
                              % (remote_path, local_path))
                    # ignore missing files (this is what ARC does too)
                    self.transport.get(remote_path, local_path,
                                       ignore_nonexisting=True)
                else:
                    log.info("Local file '%s' already exists;"
                             " will not be overwritten!",
                             local_path)

            return # XXX: should we return list of downloaded files?

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
