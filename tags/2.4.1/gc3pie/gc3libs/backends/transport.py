#! /usr/bin/env python
#
"""
The `Transport` class hierarchy provides an abstraction layer to
execute commands and copy/move files irrespective of whether the
destination is the local computer or a remote front-end that we access
via SSH.
"""
# Copyright (C) 2009-2015 S3IT, Zentrale Informatik, University of Zurich. All rights reserved.
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
__version__ = '2.4.1 version (SVN $Revision$)'


import os
import os.path
import errno
import shutil
import getpass

from gc3libs.utils import same_docstring_as, samefile
import gc3libs.exceptions


class Transport(object):

    def __init__(self):
        raise NotImplementedError(
            "Abstract method `Transport()` called - "
            "this should have been defined in a derived class.")

    def connect(self):
        """
        Open a transport session.
        """
        raise NotImplementedError(
            "Abstract method `Transport.connect()` called - "
            "this should have been defined in a derived class.")

    def chmod(self, path, mode):
        """
        Change the mode (permissions) of a file. The permissions are
        UNIX-style and identical to those used by python's `os.chmod`
        function.
        """
        raise NotImplementedError(
            "Abstract method `Transport.chmod()` called - "
            "this should have been defined in a derived class.")

    def execute_command(self, command, detach=False):
        """
        Execute a command using the available tranport media.

        The command's input and output streams are returned as python
        ``file``-like objects representing exit_status, stdout,
        stderr.

        :param string command: the command to execute

        :param bool detach: if True, do not wait for IO from the
        command, but instead, returns as soon as possible. Default is
        False.

        :return: the exit_status (int), stdout (ChannelFile), and
        stderr (ChannelFile) of the executing command

        :raise TransportException: if fails to execute the command

        """
        raise NotImplementedError(
            "Abstract method `Transport.execute_command()` called - "
            "this should have been defined in a derived class.")

    def exists(self, path):
        """
        Return ``True`` if `path` names an existing filesystem object.
        """
        raise NotImplementedError(
            "Abstract method `Transport.exists()` called - "
            "this should have been defined in a derived class.")

    def get(self, source, destination, ignore_nonexisting=False,
            overwrite=False, changed_only=True):
        """Copy remote `source` to local `destination`.

        Permission bits are copied. Both `source` and `destination`
        are path names given as strings.

        If `source` is a directory, then `destination` must be a
        directory too, and this method will descend `source`
        recursively to copy the entire file and directory structure;
        if `destination` contains non-existing directories they will
        be automatically created.

        If `source` is a instead a file and `destination` is a
        directory, a file with the same basename as source is created
        (or overwritten) in the directory specified.

        Any exception raised by operations will be passed through,
        unless the optional third argument `ignore_nonexisting` is
        `True`, in which case exceptions arising from a non-existing
        source or destination path will be ignored.

        If optional 4th argument `overwrite` is ``False`` (default),
        then existing files at the `destination` location will *not*
        be altered in any way.  If `overwrite` is instead ``True``,
        then the (optional) 5th argument `changed_only` determines
        which files are overwritten:

        - if `changed_only` is ``True`` (default), then only files for
          which the source has a different size or has been modified
          more recently than the destination are copied;

        - if `changed_only` is ``False``, then *all* files in `source`
          will be copied into `destination`, unconditionally.

        Source files that do not exist at `destination` will be
        copied, independently of the `overwrite` and `changed_only`
        settings.

        :param str source: the file or directory to copy
        :param str destination: the destination file or directory
        :param bool ignore_nonexisting: if `True`, no exceptions will
                                        be raised if `source` does not exist
        :param bool overwrite: if `True`, overwrite existing destination files
        :param bool changed_only: if both this and `overwrite` are
                                  `True`, only overwrite those files
                                  such that the source is newer or
                                  different in size than the
                                  destination.

        """
        try:
            if self.isdir(source):
                # `source` is a dir, recursively descend it
                assert os.path.isdir(destination)
                for name in self.listdir(source):
                    # don't use `os.path.join` for remote path names,
                    # ``/`` is the right separator to use; see
                    # http://code.fabfile.org/issues/show/306
                    self.get(source + '/' + name, destination + '/' + name,
                             ignore_nonexisting, overwrite, changed_only)
            else:
                # `source` is a file
                if os.path.exists(destination):
                    if not overwrite:
                        gc3libs.log.debug(
                            "Transport.get(): NOT overwriting local file '%s'"
                            " with remote file '%s' from host '%s'",
                            destination, source, self.remote_frontend)
                        return
                    elif changed_only:
                        sst = self.stat(source)
                        dst = os.stat(destination)
                        if (sst.st_size == dst.st_size
                                and sst.st_mtime <= dst.st_mtime):
                            gc3libs.log.debug(
                                "Transport.get(): Local file '%s'"
                                " has same size and modification time as"
                                " remote file '%s' from host '%s':"
                                " NOT overwriting it.",
                                destination, source, self.remote_frontend)
                            return
                # do the copy
                parent = os.path.dirname(destination)
                if not os.path.exists(parent):
                    os.makedirs(parent)
                self._get_impl(source, destination)
        except Exception as ex:
            # IOError(errno=2) means the remote path is not existing
            if (ignore_nonexisting
                    and isinstance(ex, IOError)
                    and ex.errno == 2):
                pass
            else:
                raise gc3libs.exceptions.TransportError(
                    "Could not download '%s' on host '%s' to '%s': %s: %s"
                    % (source, self.remote_frontend, destination,
                       ex.__class__.__name__, str(ex)))

    def _get_impl(self, source, destination):
        """
        Actual implementation of the `get` functionality.

        This should be overridden in derived classes, to provide
        the actual behavior in the template method `Transport.get`.
        """
        raise NotImplementedError(
            "Abstract method `Transport._get_impl()` called - "
            "this should have been defined in a derived class.")

    def get_remote_username(self):
        """
        Return the user name (as a `str` object) used on the other end
        of the transport.
        """
        raise NotImplementedError(
            "Abstract method `Transport.get_remote_username()` called - "
            "this should have been defined in a derived class.")

    def isdir(self, path):
        """
        Return `True` if `path` is a directory.
        """
        raise NotImplementedError(
            "Abstract method `Transport.isdir()` called - "
            "this should have been defined in a derived class.")

    def listdir(self, path):
        """
        Return a list containing the names of the entries in the given
        ``path``.  The list is in arbitrary order.  It does not
        include the special entries ``.`` and ``..`` even if they are
        present in the folder.  This method is meant to mirror
        ``os.listdir`` as closely as possible.

        :param string path: path to list (defaults to ``.``)
        :return: list of filenames (string)

        """
        raise NotImplementedError(
            "Abstract method `Transport.listdir()` called - "
            "this should have been defined in a derived class.")

    def makedirs(self, path, mode=0o777):
        """
        Recursive directory creation function. Makes all
        intermediate-level directories needed to contain the leaf
        directory.

        :param string path: Remote path to directory to be created.
        :return: None
        """
        raise NotImplementedError(
            "Abstract method `Transport.makedirs()` called - "
            "this should have been defined in a derived class.")

    def open(self, source, mode, bufsize=-1):
        """
        Open a file. The arguments are the same as for python's
        built-in ``file`` (aka ``open``).  A file-like object is
        returned, which closely mimics the behavior of a normal python
        file object.

        :param str source: name of the file to open
        :param str mode: mode to open in, as in Python built-in `open`
        :param bufsize: desired buffering (-1 = default buffer size)

        :return: a `file` object representing the open file

        :raise IOError: if the file could not be opened.
        """
        raise NotImplementedError(
            "Abstract method `Transport.open()` called - "
            "this should have been defined in a derived class.")

    def put(self, source, destination, ignore_errors=False,
            overwrite=False, changed_only=True):
        """
        Copy local `source` to remote `destination`.

        This works exactly like `get`:method: (which see),
        but the locality of `source` and `destination` is swapped.

        In addition, where `get`:method: has an optional 3rd argument
        `ignore_nonexisting`, the `put` method has an optional 3rd
        argument `ignore_errors` which makes it ignore *any* errors
        occurring in remote operations.
        """
        try:
            destdir = os.path.dirname(destination)
            self.makedirs(destdir)
        except Exception as ex:
            raise gc3libs.exceptions.TransportError(
                "Could not make directory '%s' on host '%s': %s: %s"
                % (destdir, self.remote_frontend,
                   ex.__class__.__name__, str(ex)))
        try:
            if os.path.isdir(source):
                # `source` is a directory, recursively descend it
                self.makedirs(destination)
                for entry in os.listdir(source):                 
                    # don't use `os.path.join` for remote path names,
                    # ``/`` is the right separator to use; see
                    # http://code.fabfile.org/issues/show/306
                    self.put(source + '/' + entry,
                             destination + '/' + entry,
                             ignore_errors, overwrite, changed_only)
            else:
                # `source` is a file
                if self.exists(destination):
                    if not overwrite:
                        gc3libs.log.debug(
                            "Transport.put(): NOT overwriting remote file '%s'"
                            " with local file '%s' from host '%s'",
                            destination, source, self.remote_frontend)
                        return
                    elif changed_only:
                        sst = os.stat(source)
                        dst = self.stat(destination)
                        if (sst.st_size == dst.st_size
                                and sst.st_mtime <= dst.st_mtime):
                            gc3libs.log.debug(
                                "Tranport.put(): Remote file '%s'"
                                " has same size and modification time as"
                                " local file '%s' from host '%s':"
                                " NOT overwriting it.",
                                destination, source, self.remote_frontend)
                            return
                # do the copy
                parent = os.path.dirname(destination)
                try:
                    if not self.exists(parent):
                        self.makedirs(parent)
                    self._put_impl(source, destination)
                # according to the docs, Paramiko raises IOError in
                # case operations fail on the remote end (i.e., not
                # for communication problems)
                except IOError:
                    if ignore_errors:
                        pass
                    else:
                        raise
        except Exception as ex:
            raise gc3libs.exceptions.TransportError(
                "Could not upload '%s' to '%s' on host '%s': %s: %s"
                % (source, destination, self.remote_frontend,
                   ex.__class__.__name__, str(ex)))

    def _put_impl(self, source, destination):
        """
        Actual implementation of the `put` functionality.

        This should be overridden in derived classes, to provide
        the actual behavior in the template method `Transport.put`.
        """
        raise NotImplementedError(
            "Abstract method `Transport.put()` called - "
            "this should have been defined in a derived class.")

    def remove(self, path):
        """
        Removes a file.
        """
        raise NotImplementedError(
            "Abstract method `Transport.remove()` called - "
            "this should have been defined in a derived class.")

    def remove_tree(self, path):
        """
        Removes a directory tree.
        """
        raise NotImplementedError(
            "Abstract method `Transport.remove_tree()` called - "
            "this should have been defined in a derived class.")

    def stat(self, path):
        """
        Retrieve information about a filesystem entry.

        The return value is an object whose attributes correspond to
        the attributes of Python's ``stat`` structure as returned by
        `os.stat`, except that it may contain fewer fields for
        compatibility with Paramiko.

        The supported fields are: ``st_mode``, ``st_size``,
        ``st_atime``, and ``st_mtime``.
        """
        raise NotImplementedError(
            "Abstract method `Transport.stat()` called - "
            "this should have been defined in a derived class.")

    def close(self):
        """
        Close the transport channel
        """
        raise NotImplementedError(
            "Abstract method `Transport.close()` called - "
            "this should have been defined in a derived class.")


# -----------------------------------------------------------------------------
# SSH Transport class
#

import types

import paramiko

import gc3libs


class SshTransport(Transport):

    def __init__(self, remote_frontend,
                 ignore_ssh_host_keys=False,
                 ssh_config=None,
                 username=None, port=None,
                 keyfile=None, timeout=None):
        """
        Initialize an `SshTransport` object for operating on host `remote_frontend`.

        Second optional argument `ignore_ssh_host_keys` instructs the
        communication layer *not* to validate the SSH host key and
        ignore the contents of SSH "known hosts" file.  While
        insecure, this is the only way of dealing with cloud-based VMs
        (where the host key is generated during VM creation).

        Third optional argument `ssh_config` specifies the path to an
        OpenSSH configuration file (see man page `ssh_config(5)` for
        details).  If found in that file, any of the following options
        override the GC3Pie built-in default:

        * ``ConnectTimeout`` sets the maximum time GC3Pie will wait
          for a connection to be established, before considering the
          attempt failed.
        * ``HostName`` overrides the host name given by `remote_frontend`
        * ``IdentityFile`` sets the (private) key file to use for
          authentication to the remote host.
        * ``Port`` sets the TCP port to use for connections.
        * ``ProxyCommand`` pipes all SSH I/O through the given command.
        * ``User`` sets the user name to use when connecting.

        Additional arguments ``user``, ``port``, ``keyfile``, and
        ``timeout``, if given, override the above settings.
        """
        self.remote_frontend = remote_frontend
        self.port = port
        self.username = username

        self.ssh = paramiko.SSHClient()
        self.ignore_ssh_host_keys = ignore_ssh_host_keys
        self.sftp = None
        self._is_open = False
        self.transport_channel = None

        # use SSH options, if available
        sshcfg = paramiko.SSHConfig()
        config_filename = os.path.expanduser(ssh_config or gc3libs.Default.SSH_CONFIG_FILE)
        if os.path.exists(config_filename):
            with open(config_filename, 'r') as config_file:
                sshcfg.parse(config_file)
            # check if we have an ssh configuration stanza for this host
            ssh_options = sshcfg.lookup(self.remote_frontend)
        else:
            # no options
            ssh_options = {}

        # merge SSH options from the SSH config file with parameters
        # we were given in this method call
        if 'hostname' in ssh_options:
            self.remote_frontend = ssh_options['hostname']

        if username is None:
            self.username = ssh_options.get('user', None)
        else:
            assert type(username) in types.StringTypes
            self.username = username

        if port is None:
            self.port = ssh_options.get('port', gc3libs.Default.SSH_PORT)
        else:
            self.port = int(port)

        if keyfile is None:
            self.keyfile = ssh_options.get('identityfile', None)
        else:
            assert type(keyfile) in types.StringTypes
            self.keyfile = keyfile

        if timeout is None:
            self.timeout = ssh_options.get('connecttimeout',
                                           gc3libs.Default.SSH_CONNECT_TIMEOUT)
        else:
            self.timeout = float(timeout)

        # support for extra configuration options, not having a direct
        # equivalent in the GC3Pie configuration file
        self.proxy_command = ssh_options.get('proxycommand', None)


    @same_docstring_as(Transport.connect)
    def connect(self):
        try:
            self.transport_channel = self.ssh.get_transport()
            if not self._is_open or self.transport_channel is None or \
                    not self.transport_channel.is_active():
                gc3libs.log.debug("Opening SshTransport... ")
                if not self.ignore_ssh_host_keys:
                    # Disabling check of the server key against "known
                    # hosts" database file. This is needed for EC2
                    # backends in order to fix issue 389, but
                    # introduces a security risk in normal situations,
                    # thus the check is enabled by default. However,
                    # Paramiko can fail to parse `~/.ssh/known_hosts`
                    # (seen on MacOSX) and then raise an
                    # `SSHException` which causes the whole block to
                    # fail.  So, ignore any errors raised by this line
                    # and hope for the best.
                    try:
                        self.ssh.load_system_host_keys()
                    except paramiko.SSHException as err:
                        gc3libs.log.warning(
                            "Could not read 'known hosts' SSH keys (%s: %s)."
                            " I'm ignoring the error and continuing anyway,"
                            " but this may mean trouble later on."
                            % (err.__class__.__name__, err))
                        pass
                else:
                    gc3libs.log.info("Ignoring ssh host key file.")

                self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                if self.proxy_command:
                    proxy = paramiko.ProxyCommand(self.proxy_command)
                    gc3libs.log.debug("Using ProxyCommand for SSH connections: %s", self.proxy_command)
                else:
                    proxy = None
                    gc3libs.log.debug("Using no ProxyCommand for SSH connections.")

                gc3libs.log.debug(
                    "Connecting to host '%s' (port %s) as user '%s' via SSH "
                    "(timeout %ds)...", self.remote_frontend, self.port,
                    self.username, self.timeout)
                self.ssh.connect(self.remote_frontend,
                                 timeout=self.timeout,
                                 username=self.username,
                                 port=self.port,
                                 allow_agent=True,
                                 key_filename=self.keyfile,
                                 sock=proxy)
                self.sftp = self.ssh.open_sftp()
                self._is_open = True
        except Exception as ex:
            gc3libs.log.error(
                "Could not create ssh connection to %s: %s: %s",
                self.remote_frontend, ex.__class__.__name__, str(ex))
            self._is_open = False

            # Try to understand why the ssh connection failed.
            if isinstance(ex, paramiko.SSHException):
                if self.keyfile:
                    # ~/.ssh/config has a ItentityFile line for this host
                    if not os.path.isfile(self.keyfile):
                        # but the key does not exists.
                        # Note that in this case we should have
                        # received an IOError excepetion...
                        gc3libs.log.error(
                            "Key file %s not found. Please check your ssh "
                            "configuration file ~/.ssh/config", self.keyfile)
                    else:
                        # but it's not working
                        gc3libs.log.error(
                            "Key file %s not accepted by remote host %s."
                            " Please check your setup.", self.keyfile,
                            self.remote_frontend)
                elif not os.path.exists(
                    os.path.expanduser("~/.ssh/id_dsa")) and \
                        not os.path.exists(
                            os.path.expanduser("~/.ssh/id_rsa")):
                    # none of the standard keys exists
                    gc3libs.log.error(
                        "No ssh key found in `~/.ssh/`. Please create an ssh"
                        " key in order to enable passwordless authentication"
                        " to %s.", self.remote_frontend)
                else:
                    # some of the standard keys are present, but not working.
                    a = paramiko.Agent()
                    try:
                        running_ssh_agent = a._conn
                    except AttributeError:
                        gc3libs.log.warning('Probably running paramiko '
                                            'version <= 1.7.7.2  ... ')
                        running_ssh_agent = a.conn

                    if not running_ssh_agent:
                        # No ssh-agent is running
                        gc3libs.log.error(
                            "Remote host %s does not accept any of the "
                            "standard ssh keys (~/.ssh/id_dsa, ~/.ssh/id_rsa)."
                            " Please check your configuration",
                            self.remote_frontend)
                    else:
                        # ssh-agent is running
                        if a.get_keys():
                            # but none of the keys is working
                            gc3libs.log.error(
                                "ssh-agent is running but none of the keys"
                                " (%d) is accepted by remote host %s. Please,"
                                " check your configuration.",
                                len(a.get_keys()), self.remote_frontend)
                        else:
                            # but it has no keys inside.
                            gc3libs.log.error(
                                "ssh-agent is running but no key has been"
                                " added. Please add a key with `ssh-add`"
                                " command.")

            raise gc3libs.exceptions.TransportError(
                "Failed while connecting to remote host '%s': %s"
                % (self.remote_frontend, str(ex)))

    @same_docstring_as(Transport.chmod)
    def chmod(self, path, mode):
        try:
            # check connection first
            self.connect()
            self.sftp.chmod(path, mode)
        except Exception as ex:
            raise gc3libs.exceptions.TransportError(
                "Error changing remote path '%s' mode to 0%o: %s: %s"
                % (path, mode, ex.__class__.__name__, str(ex)))

    @same_docstring_as(Transport.execute_command)
    def execute_command(self, command, detach=False):
        try:
            # check connection first
            self.connect()
            if detach:
                command = command + ' &'
            gc3libs.log.debug("SshTransport running `%s`... ", command)
            stdin_stream, stdout_stream, stderr_stream = \
                self.ssh.exec_command(command)
            stdout = ''
            stderr = ''
            if not detach:
                stdout = stdout_stream.read()
                stderr = stderr_stream.read()
            exitcode = stdout_stream.channel.recv_exit_status()
            gc3libs.log.debug(
                "Executed command '%s' on host '%s'; exit code: %d"
                % (command, self.remote_frontend, exitcode))
            return exitcode, stdout, stderr
        except Exception as ex:
            raise gc3libs.exceptions.TransportError(
                "Failed executing remote command '%s': %s: %s"
                % (command, ex.__class__.__name__, str(ex)))

    @same_docstring_as(Transport.exists)
    def exists(self, path):
        try:
            self.connect()
            self.sftp.stat(path)
            return True
        except IOError as err:
            if err.errno == 2:
                return False
            else:
                raise gc3libs.exceptions.TransportError(
                    "Could not stat() file '%s' on host '%s': %s: %s"
                    % (path, self.remote_frontend,
                       err.__class__.__name__, str(err)))
        except Exception as err:
            raise gc3libs.exceptions.TransportError(
                "Could not stat() file '%s' on host '%s': %s: %s"
                % (path, self.remote_frontend,
                   err.__class__.__name__, str(err)))

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
        except IOError as ex:
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
        except Exception as ex:
            raise gc3libs.exceptions.TransportError(
                "Could not list directory '%s' on host '%s': %s: %s"
                % (path, self.remote_frontend, ex.__class__.__name__, str(ex)))

    @same_docstring_as(Transport.makedirs)
    def makedirs(self, path, mode=0o777):
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
    def put(self, source, destination, ignore_errors=False,
            overwrite=False, changed_only=True):
        gc3libs.log.debug("SshTransport.put(): local source: '%s';"
                          " remote destination: '%s'; remote host: '%s'.",
                          source, destination, self.remote_frontend)
        self.connect()  # ensure connection is up

        Transport.put(self, source, destination,
                      ignore_errors, overwrite, changed_only)

    def _put_impl(self, source, destination):
        """
        Copy remote file `source` to local `destination` using SFTP.
        """
        self.sftp.put(source, destination)

    @same_docstring_as(Transport.get)
    def get(self, source, destination, ignore_nonexisting=False,
            overwrite=False, changed_only=True):
        gc3libs.log.debug("SshTranport.get(): remote source %s; "
                          "remote host: %s; local destination: %s.",
                          source, self.remote_frontend, destination)
        self.connect()  # ensure connection is up
        Transport.get(self, source, destination,
                      ignore_nonexisting, overwrite, changed_only)

    def _get_impl(self, source, destination):
        """
        Copy remote file `source` to local `destination` using SFTP.
        """
        self.sftp.get(source, destination)

    @same_docstring_as(Transport.remove)
    def remove(self, path):
        try:
            gc3libs.log.debug(
                "SshTransport.remove(): path: %s; remote host: %s",
                path, self.remote_frontend)
            # check connection first
            self.connect()
            self.sftp.remove(path)
        except IOError as ex:
            raise gc3libs.exceptions.TransportError(
                "Could not remove '%s' on host '%s': %s: %s"
                % (path, self.remote_frontend, ex.__class__.__name__, str(ex)))

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
        except Exception as ex:
            raise gc3libs.exceptions.TransportError(
                "Could not remove tree '%s' on host '%s': %s: %s"
                % (path, self.remote_frontend,
                   ex.__class__.__name__, str(ex)))

    @same_docstring_as(Transport.stat)
    def stat(self, path):
        try:
            self.connect()
            return self.sftp.stat(path)
        except Exception as err:
            raise gc3libs.exceptions.TransportError(
                "Could not stat() file '%s' on host '%s': %s: %s"
                % (path, self.remote_frontend,
                   err.__class__.__name__, str(err)))

    @same_docstring_as(Transport.open)
    def open(self, source, mode, bufsize=-1):
        try:
            # check connection first
            self.connect()
            return self.sftp.open(source, mode, bufsize)
        except Exception as ex:
            raise gc3libs.exceptions.TransportError(
                "Could not open file '%s' on host '%s': %s: %s"
                % (source, self.remote_frontend,
                   ex.__class__.__name__, str(ex)))

    @same_docstring_as(Transport.close)
    def close(self):
        """
        Close the transport channel
        """
        gc3libs.log.info(
            "Closing SshTransport to host '%s'... " % self.remote_frontend)
        if self.sftp is not None and self.sftp.get_channel() is not None:
            self.sftp.close()
            gc3libs.log.info("... sftp connection to '%s' closed",
                             self.remote_frontend)
        if self.ssh is not None and self.ssh.get_transport() is not None:
            self.ssh.close()
            gc3libs.log.info("... ssh connection to '%s' closed",
                             self.remote_frontend)
        self._is_open = False
        # gc3libs.log.debug("Closed SshTransport to host '%s'"
        # % self.remote_frontend)


# -----------------------------------------------------------------------------
# Local Transport class
#

import subprocess


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

        if (self._process is not None) and (self._process.pid == pid):
            return self._process.poll()
        else:

            statfile = os.path.join(self.procloc, str(pid), "stat")

            try:
                if not os.path.exists(statfile):
                    return '0'
                fd = open(statfile, 'r')
                status = fd.readline().split(" ")[2]
                fd.close()
                if status in 'RSDZTW':
                    # process still runing
                    return 1
                else:
                    # unknown state
                    gc3libs.log.warning('Unhandled process state [%s]', status)
                    return 1

            except IOError:
                raise
            except Exception as ex:
                gc3libs.log.error(
                    "Error while trying to read status file. Error"
                    " type %s. message %s" % (ex.__class__, ex.message))
                raise gc3libs.exceptions.TransportError(ex.message)

    @same_docstring_as(Transport.connect)
    def connect(self):
        gc3libs.log.debug("Opening LocalTransport... ")
        self._is_open = True

    @same_docstring_as(Transport.chmod)
    def chmod(self, path, mode):
        try:
            os.chmod(path, mode)
        except Exception as ex:
            raise gc3libs.exceptions.TransportError(
                "Error changing local path '%s' mode to 0%o: %s: %s"
                % (path, mode, ex.__class__.__name__, str(ex)))

    def _execute_command_and_detach(self, command):
        assert self._is_open is True, \
            "`Transport.execute_command()` called" \
            " on `Transport` instance closed / not yet open"

        try:
            _process = subprocess.Popen(command,
                                        close_fds=True,
                                        stdout=subprocess.PIPE,
                                        shell=True)
            return _process.pid
        except Exception as ex:
            raise gc3libs.exceptions.TransportError(
                "Failed executing command '%s': %s: %s"
                % (command, ex.__class__.__name__, str(ex)))

    def get_pid(self):
        if self._process is not None:
            return self._process.pid
        else:
            return -1

    @same_docstring_as(Transport.execute_command)
    def execute_command(self, command, detach=False):
        assert self._is_open is True, \
            "`Transport.execute_command()` called" \
            " on `Transport` instance closed / not yet open"
        if detach:
            return self._execute_command_and_detach(command)
        try:
            self._process = subprocess.Popen(command,
                                             stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE,
                                             close_fds=True, shell=True)
            stdout, stderr = self._process.communicate()
            exitcode = self._process.returncode
            gc3libs.log.debug(
                "Executed local command '%s', got exit status: %d",
                command, exitcode)
            return exitcode, stdout, stderr
        except Exception as ex:
            raise gc3libs.exceptions.TransportError(
                "Failed executing command '%s': %s: %s"
                % (command, ex.__class__.__name__, str(ex)))

    @same_docstring_as(Transport.exists)
    def exists(self, path):
        return os.path.exists(path)

    @same_docstring_as(Transport.get)
    def get(self, source, destination, ignore_nonexisting=False,
            overwrite=False, changed_only=True):
        assert self._is_open is True, \
            "`Transport.get()` called" \
            " on `Transport` instance closed / not yet open"
        Transport.get(self, source, destination,
                      ignore_nonexisting, overwrite, changed_only)

    def _get_impl(self, source, destination):
        """
        Copy local file `source` over `destination`.
        """
        self._copy_skip_same(source, destination)

    @staticmethod
    def _copy_skip_same(source, destination):
        """
        Copy local file `source` over `destination`.
        Raise no error if they point to the same file.
        """
        if samefile(source, destination):
            gc3libs.log.warning(
                "Attempt to copy file '%s' over itself."
                " Ignoring.", source)
            return False
        else:
            return shutil.copy(source, destination)

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
        except Exception as ex:
            raise gc3libs.exceptions.TransportError(
                "Could not list local directory '%s': %s: %s"
                % (path, ex.__class__.__name__, str(ex)))

    @same_docstring_as(Transport.makedirs)
    def makedirs(self, path, mode=0o777):
        try:
            os.makedirs(path, mode)
        except OSError as e:
            if e.errno == errno.EEXIST:
                pass

    @same_docstring_as(Transport.put)
    def put(self, source, destination, ignore_errors=False,
            overwrite=False, changed_only=True):
        assert self._is_open is True, \
            "`Transport.put()` called" \
            " on `Transport` instance closed / not yet open"
        Transport.put(self, source, destination,
                      ignore_errors, overwrite, changed_only)

    def _put_impl(self, source, destination):
        """
        Copy local file `source` over `destination`.
        """
        self._copy_skip_same(source, destination)

    @same_docstring_as(Transport.remove)
    def remove(self, path):
        assert self._is_open is True, \
            "`Transport.execute_command()` called" \
            " on `Transport` instance closed / not yet open"

        try:
            gc3libs.log.debug("Removing %s", path)
            return os.remove(path)
        except Exception as ex:
            raise gc3libs.exceptions.TransportError(
                "Could not remove file '%s': %s: %s"
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
        except Exception as ex:
            raise gc3libs.exceptions.TransportError(
                "Could not remove directory tree '%s': %s: %s"
                % (path, ex.__class__.__name__, str(ex)))

    @same_docstring_as(Transport.stat)
    def stat(self, path):
        try:
            return os.stat(path)
        except Exception as err:
            raise gc3libs.exceptions.TransportError(
                "Could not stat() file '%s' on host localhost: %s: %s"
                % (path, err.__class__.__name__, str(err)))

    @same_docstring_as(Transport.open)
    def open(self, source, mode, bufsize=0):
        try:
            return open(source, mode, bufsize)
        except Exception as ex:
            raise gc3libs.exceptions.TransportError(
                "Could not open file '%s' on host localhost: %s: %s"
                % (source, ex.__class__.__name__, str(ex)))

    @same_docstring_as(Transport.close)
    def close(self):
        gc3libs.log.debug("Closing LocalTransport... ")
        self._is_open = False
