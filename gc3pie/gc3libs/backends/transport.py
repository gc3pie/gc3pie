#! /usr/bin/env python
#
"""
The `Transport` class hierarchy provides an abstraction layer to
execute commands and copy/move files irrespective of whether the
destination is the local computer or a remote front-end that we access
via SSH.
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


import os
import os.path
import errno
import shutil
import getpass

from gc3libs.utils import same_docstring_as
import gc3libs.exceptions

class Transport(object):

    def __init__(self):
        raise NotImplementedError("Abstract method `Transport()` called - this should have been defined in a derived class.")

    def connect(self):
        """
        Open a transport session.
        """
        raise NotImplementedError("Abstract method `Transport.connect()` called - this should have been defined in a derived class.")

    def chmod(self, path, mode):
        """
        Change the mode (permissions) of a file. The permissions are
        UNIX-style and identical to those used by python's `os.chmod`
        function.
        """
        raise NotImplementedError("Abstract method `Transport.chmod()` called - this should have been defined in a derived class.")

    def execute_command(self, command):
        """
        Execute a command using the available tranport media.
        The command's input and output streams are returned
        as python ``file``-like objects representing exit_status, stdout, stderr.

        :param string command: the command to execute

        :return: the exit_status (int), stdout (ChannelFile), and stderr (ChannelFile) of the executing command

        :raise TransportException: if fails to execute the command

        """
        raise NotImplementedError("Abstract method `Transport.execute_command()` called - this should have been defined in a derived class.")

    def get_remote_username(self):
        """
        Return the user name (as a `str` object) used on the other end
        of the transport.
        """
        raise NotImplementedError("Abstract method `Transport.get_remote_username()` called - this should have been defined in a derived class.")

    def isdir(self, path):
        """
        Return `True` if `path` is a directory.
        """
        raise NotImplementedError("Abstract method `Transport.isdir()` called - this should have been defined in a derived class.")

    def listdir(self, path):
        """
        Return a list containing the names of the entries in the given ``path``.
        The list is in arbitrary order.  It does not include the special
        entries ``.`` and ``..`` even if they are present in the folder.
        This method is meant to mirror ``os.listdir`` as closely as possible.

        :param string path: path to list (defaults to ``.``)
        :return: list of filenames (string)

        """
        raise NotImplementedError("Abstract method `Transport.listdir()` called - this should have been defined in a derived class.")

    def makedirs(self, path, mode=0777):
        """
        Recursive directory creation function. Makes all
        intermediate-level directories needed to contain the leaf
        directory.

        :param string path: Remote path to directory to be created.
        :return: None
        """
        raise NotImplementedError("Abstract method `Transport.makedirs()` called - this should have been defined in a derived class.")


    def open(self, source, mode, bufsize=-1):
        """
        Open a file. The arguments are the same as for python's built-in ``file``
        (aka ``open``).  A file-like object is returned, which closely mimics
        the behavior of a normal python file object.

        :param str source: name of the file to open
        :param str mode: mode to open in, as in Python built-in `open`
        :param bufsize: desired buffering (-1 = default buffer size)

        :return: a `file` object representing the open file

        :raise IOError: if the file could not be opened.
        """
        raise NotImplementedError("Abstract method `Transport.open()` called - this should have been defined in a derived class.")

    def put(self, source, destination):
        """
        Copy the file source to the file or directory destination.
        If destination is a directory, a file with the same basename
        as source is created (or overwritten) in the directory specified.

        Permission bits are copied. source and destination are path
        names given as strings.

        Any exception raised by operations will be passed through.

        :param str source: the file to copy
        :param str destination: the destination file or directory
        """
        raise NotImplementedError("Abstract method `Transport.put()` called - this should have been defined in a derived class.")

    def get(self, source, destination, ignore_nonexisting=False):
        """
        Copy the file source to the file or directory destination.
        If destination is a directory, a file with the same basename
        as source is created (or overwritten) in the directory specified.

        Permission bits are copied. source and destination are path
        names given as strings.

        Any exception raised by operations will be passed through,
        unless the optional third argument `ignore_nonexisting` is
        `True`, in which case exceptions arising from a non-existing
        source or destination path will be ignored.

        :param str source: the file to copy
        :param str destination: the destination file or directory
        """
        raise NotImplementedError("Abstract method `Transport.get()` called - this should have been defined in a derived class.")

    def remove(self, path):
        """
        Removes a file.
        """
        raise NotImplementedError("Abstract method `Transport.remove()` called - this should have been defined in a derived class.")

    def remove_tree(self, path):
        """
        Removes a directory tree.
        """
        raise NotImplementedError("Abstract method `Transport.remove_tree()` called - this should have been defined in a derived class.")

    def close(self):
        """
        Close the transport channel
        """
        raise NotImplementedError("Abstract method `Transport.close()` called - this should have been defined in a derived class.")


# -----------------------------------------------------------------------------
# SSH Transport class
#

import sys

import paramiko

import gc3libs

class SshTransport(Transport):

    def __init__(self, remote_frontend,
                 port=gc3libs.Default.SSH_PORT,
                 username=None):
        self.remote_frontend = remote_frontend
        self.port = port
        self.username = username

        self.ssh = paramiko.SSHClient()
        self.ssh_config = paramiko.SSHConfig()
        self.keyfile = None
        self.sftp = None
        self._is_open = False
        self.transport_channel = None
        
        try:
            config_filename = os.path.expanduser('~/.ssh/config')
            config_file = open(config_filename)
            self.ssh_config.parse(config_file)
            # Check if we have an ssh configuration stanza for this host
            hostconfig = self.ssh_config.lookup(self.remote_frontend)
            self.keyfile = hostconfig.get('identityfile', None)
            config_file.close()
        except IOError:
            # File not found. Ignoring
            pass

    @same_docstring_as(Transport.connect)
    def connect(self):
        try:
            self.transport_channel = self.ssh.get_transport()
            if not self._is_open or self.transport_channel == None or not self.transport_channel.is_active():
                gc3libs.log.debug("Opening SshTransport... ")
                self.ssh.load_system_host_keys()
                self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                gc3libs.log.debug(
                    "Connecting to host '%s' as user '%s' via SSH ...",
                    self.remote_frontend, self.username)
                self.ssh.connect(self.remote_frontend,
                                 timeout=gc3libs.Default.SSH_CONNECT_TIMEOUT,
                                 username=self.username,
                                 allow_agent=True,
                                 key_filename = self.keyfile)
                self.sftp = self.ssh.open_sftp()
                self._is_open = True
        except Exception, ex:
            gc3libs.log.error(
                "Could not create ssh connection to %s: %s: %s",
                self.remote_frontend, ex.__class__.__name__, str(ex))
            self._is_open = False

            # Try to understand why the ssh connection failed.
            if isinstance(ex, paramiko.SSHException):
                if self.keyfile:
                    # ~/.ssh/config has a ItentityFile line for this host
                    if not os.path.isfile(self.keyfile):
                        # but the key does not exists
                        # Note that in this case we should have received an IOError excepetion...
                        gc3libs.log.error(
                            "Key file %s not found. Please check your "
                            "ssh configuration file ~/.ssh/config" % self.keyfile)
                    else:
                        # but it's not working
                        gc3libs.log.error(
                            "Key file %s not accepted by remote host %s. Please check your setup." % (
                                self.keyfile, self.remote_frontend))
                elif not os.path.exists(
                    os.path.expanduser("~/.ssh/id_dsa")) and \
                    not os.path.exists(
                    os.path.expanduser("~/.ssh/id_rsa")):
                    # none of the standard keys exists
                    gc3libs.log.error(
                        "No ssh key found in `~/.ssh/`. Please create an ssh key in order to "
                        "enable passwordless authentication to %s." % self.remote_frontend)
                else:
                    # some of the standard keys are present, but not working.
                    a = paramiko.Agent()
                    if not a.conn:
                        # No ssh-agent is running
                        gc3libs.log.error(
                            "Remote host %s does not accept any of the standard ssh keys (~/.ssh/id_dsa, ~/.ssh/id_rsa). "
                            "Please check your configuration" % self.remote_frontend)
                    else:
                        # ssh-agent is running
                        if a.get_keys():
                            # but none of the keys is working
                            gc3libs.log.error(
                                "ssh-agent is running but none of the keys (%d) is accepted by remote host %s."
                                " Please, check your configuration." % (len(a.get_keys()), self.remote_frontend))
                        else:
                            # but it has no keys inside.
                            gc3libs.log.error(
                                "ssh-agent is running but no key has been added. Please add a key with `ssh-add` command.")

            raise gc3libs.exceptions.TransportError(
                "Failed while connecting to remote host '%s': %s"
                % (self.remote_frontend, str(ex)))


    @same_docstring_as(Transport.chmod)
    def chmod(self, path, mode):
        try:
            # check connection first
            self.connect()
            self.sftp.chmod(path, mode)
        except Exception, ex:
            raise gc3libs.exceptions.TransportError(
                "Error changing remote path '%s' mode to 0%o: %s: %s"
                % (path, mode, ex.__class__.__name__, str(ex)))


    @same_docstring_as(Transport.execute_command)
    def execute_command(self, command):
        try:
            # check connection first
            self.connect()
            gc3libs.log.debug("SshTransport running `%s`... ", command)
            stdin_stream, stdout_stream, stderr_stream = self.ssh.exec_command(command)
            stdout = stdout_stream.read()
            stderr = stderr_stream.read()
            exitcode = stdout_stream.channel.recv_exit_status()
            gc3libs.log.debug("Executed command '%s' on host '%s'; exit code: %d"
                              % (command, self.remote_frontend, exitcode))
            return exitcode, stdout, stderr
        except Exception, ex:
            raise gc3libs.exceptions.TransportError(
                "Failed executing remote command '%s': %s: %s"
                % (command, ex.__class__.__name__, str(ex)))

    @same_docstring_as(Transport.get_remote_username)
    def get_remote_username(self):
        (exitcode, stdout, stderr) = self.execute_command('whoami')
        return stdout.strip()

    @same_docstring_as(Transport.isdir)
    def isdir(self, path):
        # SFTPClient.listdir() raises IOError(errno=2) when called
        # with a non-directory argument
        try:
            # check connection first
            self.connect()
            self.sftp.listdir(path)
            return True
        except IOError, ex:
            if ex.errno == 2:
                return False
            else:
                raise


    @same_docstring_as(Transport.listdir)
    def listdir(self, path):
        try:
            # check connection first
            self.connect()
            return self.sftp.listdir(path)
        except Exception, ex:
            raise gc3libs.exceptions.TransportError("Could not list directory '%s' on host '%s': %s: %s"
                                            % (path, self.remote_frontend,
                                               ex.__class__.__name__, str(ex)))

    @same_docstring_as(Transport.makedirs)
    def makedirs(self, path, mode=0777):
        dirs = path.split('/')
        if '..' in dirs:
            raise gc3libs.exceptions.InvalidArgument(
                "Path component '..' not allowed in `SshTransport.makedirs()`")
        dest = ''
        for dir in dirs:
            if dir in ['', '.']:
                continue
            dest += '/' + dir
            try:
                # check connection first
                self.connect()
                self.sftp.mkdir(dest, mode)
            except IOError:
                # sftp.mkdir raises IOError if the directory exists;
                # ignore error and continue
                pass


    @same_docstring_as(Transport.put)
    def put(self, source, destination):
        gc3libs.log.debug("SshTransport.put(): local source: '%s';"
                          " remote destination: '%s'; remote host: '%s'."
                          % (source, destination, self.remote_frontend))
        try:
            destdir = os.path.dirname(destination)
            self.makedirs(destdir)
        except Exception, ex:
            raise gc3libs.exceptions.TransportError(
                "Could not make directory '%s' on host '%s': %s: %s"
                % (destdir, self.remote_frontend,
                   ex.__class__.__name__, str(ex)))
        try:
            # check connection first
            self.connect()
            self.sftp.put(source, destination)
        except Exception, ex:
            raise gc3libs.exceptions.TransportError(
                "Could not upload '%s' to '%s' on host '%s': %s: %s"
                % (source, destination, self.remote_frontend,
                   ex.__class__.__name__, str(ex)))


    @same_docstring_as(Transport.get)
    def get(self, source, destination, ignore_nonexisting=False):
        try:
            gc3libs.log.debug("SshTranport.get(): remote source %s; remote host: %s; local destination: %s."
                              % (source, self.remote_frontend, destination))
            if self.isdir(source):
                # recursively descend it
                for name in self.listdir(source):
                    # don't use `os.path.join` here, ``/`` is
                    # the right separator to use; see
                    # http://code.fabfile.org/issues/show/306
                    self.get(source + '/' + name, destination + '/' + name)
            else:
                # check connection first
                self.connect()
                self.sftp.get(source, destination)
        except Exception, ex:
            # IOError(errno=2) means the remote path is not existing
            if not (ignore_nonexisting
                    and isinstance(ex, IOError) and ex.errno == 2):
                raise gc3libs.exceptions.TransportError(
                    "Could not download '%s' on host '%s' to '%s': %s: %s"
                    % (source, self.remote_frontend, destination,
                       ex.__class__.__name__, str(ex)))

    @same_docstring_as(Transport.remove)
    def remove(self, path):
        try:
            gc3libs.log.debug("SshTransport.remove(): path: %s; remote host: %s" % (path, self.remote_frontend))
            # check connection first
            self.connect()
            self.sftp.remove(path)
        except IOError, ex:
            raise gc3libs.exceptions.TransportError("Could not remove '%s' on host '%s': %s: %s"
                                            % (path, self.remote_frontend,
                                               ex.__class__.__name__, str(ex)))

    @same_docstring_as(Transport.remove_tree)
    def remove_tree(self, path):
        try:
            gc3libs.log.debug("Running method 'remove_tree';"
                              " remote path: %s remote host: %s"
                              % (path, self.remote_frontend))
            # Note: At the moment rmdir does not work as expected
            # self.sftp.rmdir(path)
            # easy workaround: use SSHClient to issue an rm -rf comamnd
            _command = "rm -rf '%s'" % path
            exit_code, stdout, stderr = self.execute_command(_command)
            if exit_code != 0:
                raise Exception("Remote command '%s' failed with code %d: %s"
                                % (_command, exit_code, stderr))
        except Exception, ex:
            raise gc3libs.exceptions.TransportError("Could not remove tree '%s' on host '%s': %s: %s"
                                            % (path, self.remote_frontend,
                                               ex.__class__.__name__, str(ex)))

    @same_docstring_as(Transport.open)
    def open(self, source, mode, bufsize=-1):
        try:
            # check connection first
            self.connect()
            return self.sftp.open(source, mode, bufsize)
        except Exception, ex:
            raise gc3libs.exceptions.TransportError("Could not open file '%s' on host '%s': %s: %s"
                                            % (source, self.remote_frontend,
                                               ex.__class__.__name__, str(ex)))

    @same_docstring_as(Transport.close)
    def close(self):
        """
        Close the transport channel
        """
        gc3libs.log.info("Closing SshTransport to host '%s'... " % self.remote_frontend)

        if self.sftp is not None:
            self.sftp.close()
        if self.ssh is not None:
            self.ssh.close()
        self._is_open = False
        # gc3libs.log.debug("Closed SshTransport to host '%s'"
        # % self.remote_frontend)


# -----------------------------------------------------------------------------
# Local Transport class
#

import subprocess
import shutil


class LocalTransport(Transport):

    _is_open = False
    _process = None

    def __init__(self):
        pass

    def get_proc_state(self, pid):
        """
        Getting process state.
        params: pid - process id

        return:
        0 (process terminated)
        1 (process running)
        -N (when available, process terminated with N exit code)

        raise: IOError if status file cannot be accessed
        TransportError if any other Exception is raised
        """

        if (not self._process is None) and (_process.pid == pid):
            return self._process.poll()
        else:

            statfile = os.path.join(self.procloc, str(pid), "stat")

            try:
                if not os.path.exists(statfile):
                    return '0'
                fd = open(statfile,'r')
                status = fd.readline().split(" ")[2]
                fd.close()
                if status in 'RSDZTW':
                    # process still runing
                    return 1
                else:
                    # unknown state
                    gc3libs.log.warning('Unhandled process state [%s]' % status)
                    return 1

            except IOError:
                raise
            except Exception, ex:
                log.error('Error while trying to read status file. Error type %s. message %s' % (ex.__class__, ex.message))
                raise gc3libs.exceptions.TransportError(x.message)


    @same_docstring_as(Transport.connect)
    def connect(self):
        gc3libs.log.debug("Opening LocalTransport... ")
        self._is_open = True

    @same_docstring_as(Transport.chmod)
    def chmod(self, path, mode):
        try:
            os.chmod(path, mode)
        except Exception, ex:
            raise gc3libs.exceptions.TransportError(
                "Error changing local path '%s' mode to 0%o: %s: %s"
                % (path, mode, ex.__class__.__name__, str(ex)))

    def execute_command_and_detach(self, command):
        assert self._is_open is True, \
            "`Transport.execute_command()` called" \
            " on `Transport` instance closed / not yet open"

        try:
            _process = subprocess.Popen(command, close_fds=True, stdout=subprocess.PIPE, shell=True)
            return _process.pid
        except Exception, ex:
            raise gc3libs.exceptions.TransportError("Failed executing command '%s': %s: %s"
                                     % (command, ex.__class__.__name__, str(ex)))


    def get_pid(self):
        if not self._process is None:
            return self._process.pid
        else:
            return -1

    @same_docstring_as(Transport.execute_command)
    def execute_command(self, command):
        assert self._is_open is True, \
            "`Transport.execute_command()` called" \
            " on `Transport` instance closed / not yet open"
        try:
            self._process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True, shell=True)
            stdout, stderr = self._process.communicate()
            exitcode = self._process.returncode
            gc3libs.log.debug("Executed local command '%s', got exit status: %d",
                              command, exitcode)
            return exitcode, stdout, stderr
        except Exception, ex:
            raise gc3libs.exceptions.TransportError("Failed executing command '%s': %s: %s"
                                     % (command, ex.__class__.__name__, str(ex)))


    @same_docstring_as(Transport.get_remote_username)
    def get_remote_username(self):
        return getpass.getuser()

    @same_docstring_as(Transport.isdir)
    def isdir(self, path):
        return os.path.isdir(path)


    @same_docstring_as(Transport.listdir)
    def listdir(self, path):
        assert self._is_open is True, \
            "`Transport.execute_command()` called" \
            " on `Transport` instance closed / not yet open"

        try:
            return os.listdir(path)
        except Exception, ex:
            raise gc3libs.exceptions.TransportError("Could not list local directory '%s': %s: %s"
                                     % (path, ex.__class__.__name__, str(ex)))


    @same_docstring_as(Transport.makedirs)
    def makedirs(self, path, mode=0777):
        try:
            os.makedirs(path, mode)
        except OSError, e:
            if e.errno == errno.EEXIST:
                pass

    @same_docstring_as(Transport.put)
    def put(self, source, destination):
        assert self._is_open is True, \
            "`Transport.execute_command()` called" \
            " on `Transport` instance closed / not yet open"

        try:
            gc3libs.log.debug("Running method 'put';"
                              " source: %s. destination: %s"
                              % (source, destination))
            if source != destination:
                return shutil.copy(source, destination)
            else:
                gc3libs.log.warning("Attempt to copy file over itself"
                                    " ('%s'). Ignoring." % source)
                return True
        except Exception, ex:
            raise gc3libs.exceptions.TransportError("Could not copy '%s' to '%s': %s: %s"
                                     % (source, destination, ex.__class__.__name__, str(ex)))


    @same_docstring_as(Transport.get)
    def get(self, source, destination, ignore_nonexisting=False):
        gc3libs.log.debug("Transport.get() implemented by Transport.put()... ")
        self.put(source, destination)


    @same_docstring_as(Transport.remove)
    def remove(self, path):
        assert self._is_open is True, \
            "`Transport.execute_command()` called" \
            " on `Transport` instance closed / not yet open"

        try:
            gc3libs.log.debug("Removing %s", path)
            return os.remove(path)
        except Exception, ex:
            raise gc3libs.exceptions.TransportError("Could not remove file '%s': %s: %s"
                                     % (path, ex.__class__.__name__, str(ex)))


    @same_docstring_as(Transport.remove_tree)
    def remove_tree(self, path):
        assert self._is_open is True, \
            "`Transport.execute_command()` called" \
            " on `Transport` instance closed / not yet open"

        try:
            gc3libs.log.debug("LocalTransport.remove_tree():"
                              " removing local directory tree '%s'" % path)
            return shutil.rmtree(path)
        except Exception, ex:
            raise gc3libs.exceptions.TransportError("Could not remove directory tree '%s': %s: %s"
                                     % (path, ex.__class__.__name__, str(ex)))

    @same_docstring_as(Transport.open)
    def open(self, source, mode, bufsize=0):
        try:
            return open(source, mode, bufsize)
        except Exception, ex:
            raise gc3libs.exceptions.TransportError(
                "Could not open file '%s' on host localhost: %s: %s"
                % (source, ex.__class__.__name__, str(ex)))

    @same_docstring_as(Transport.close)
    def close(self):
        gc3libs.log.debug("Closing LocalTransport... ")
        self._is_open = False
