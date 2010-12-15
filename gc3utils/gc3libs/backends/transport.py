#! /usr/bin/env python
#
"""
The `Transport` class hierarchy provides an abstraction layer to
execute commands and copy/move files irrespective of whether the
destination is the local computer or a remote front-end that we access
via SSH.
"""
# Copyright (C) 2009-2010 GC3, University of Zurich. All rights reserved.
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
import os.path

from gc3libs.utils import same_docstring_as


class Transport(object):

    def __init__(self):
        raise NotImplementedError("Abstract method `Transport()` called - this should have been defined in a derived class.")

    def connect(self):
        """
        Open a transport session.
        """
        raise NotImplementedError("Abstract method `Transport.open()` called - this should have been defined in a derived class.")

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

    def listdir(self, path):
        """
        Return a list containing the names of the entries in the given ``path``.
        The list is in arbitrary order.  It does not include the special
        entries C{'.'} and C{'..'} even if they are present in the folder.
        This method is meant to mirror ``os.listdir`` as closely as possible.
        
        :param string path: path to list (defaults to C{'.'})
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

    def put(self, source, destinaton):
        """
        Copy the file source to the file or directory destination.
        If destination is a directory, a file with the same basename 
        as source is created (or overwritten) in the directory specified. 
        Permission bits are copied. source and destinaton are path 
        names given as strings.
        Any exception raised by operations will be passed through.  
        
        :param str source: the file to copy
        :param str destinaton: the destination file or directory
        """
        raise NotImplementedError("Abstract method `Transport.put()` called - this should have been defined in a derived class.")

    def get(self, source, destinaton):
        """
        Copy the file source to the file or directory destinaton.
        If destination is a directory, a file with the same basename 
        as source is created (or overwritten) in the directory specified. 
        Permission bits are copied. source and destination are path 
        names given as strings.
        Any exception raised by operations will be passed through.

        :param str source: the file to copy
        :param str destinaton: the destination file or directory
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

import paramiko
import gc3libs.Default as Default
import gc3libs
import gc3libs.Exceptions as Exceptions
import sys

class SshTransport(Transport):

    ssh = None
    sftp = None
    _is_open = False

    def __init__(self, remote_frontend, port=Default.SSH_PORT, username=None):
        self.remote_frontend = remote_frontend
        self.port = port
        self.username = username

    @same_docstring_as(Transport.connect)
    def connect(self):
        try:
            if not self._is_open:
                self.ssh = paramiko.SSHClient()
                self.ssh.load_system_host_keys()
                self.ssh.connect(self.remote_frontend,timeout=Default.SSH_CONNECT_TIMEOUT,username=self.username, allow_agent=True)
                self.sftp = self.ssh.open_sftp()
                self._is_open = True
                gc3libs.log.info("SshTransport remote_frontend: %s port: %d username: %s connection status [ conected ]" % (self.remote_frontend, self.port, self.username))
        except:
            gc3libs.log.error("Could not create ssh connection to %s" % host)
            raise Exceptions.TransportError("Failed while connecting to remote host: %s. Error type %s, %s"
                                            % (self.remote_frontend, sys.exc_info()[0], sys.exc_info()[1]))

    @same_docstring_as(Transport.execute_command)
    def execute_command(self, command):
        try:
            stdin_stream, stdout_stream, stderr_stream = self.ssh.exec_command(command)
            stdout = stdout_stream.read()
            stderr = stderr_stream.read()
            exitcode = stdout_stream.channel.recv_exit_status()
            gc3libs.log.info('execute_command: %s. exit status: %d' % (command, exitcode))
                
            return exitcode, stdout, stderr
        except:
            gc3libs.log.error('Failed while executing remote command: %s' % command)
            raise Exceptions.TransportError("Failed while executing remote command: %s. Error type %s, %s" 
                                            % (command, sys.exc_info()[0], sys.exc_info()[1]))
        
    @same_docstring_as(Transport.listdir)
    def listdir(self, path):
        try:
            return self.sftp.listdir(path)
        except Exception, x:
            gc3libs.log.error("Failed method listdir. remote path: %s. remote host: %s." 
                              % (path, self.remote_frontend), exc_info=True)
            raise Exceptions.TransportError("Failed method listdir on %s. Error type %s, %s"
                                            % (path, sys.exc_info()[0], sys.exc_info()[1]))

    @same_docstring_as(Transport.makedirs)
    def makedirs(self, path, mode=0777):
        dirs = path.split('/')
        if '..' in dirs:
            raise InvalidArgument("Path component '..' not allowed in `SshTransport.makedirs()`")
        dest = ''
        for dir in dirs:
            if dir in ['', '.']:
                continue
            try:
                self.sftp.listdir(dest + dir)
            except IOError:
                # sftp.mkdir raises IOError if the directory exists;
                # ignore error and continue
                pass
            dest = os.path.join(dest, dir)

        
    @same_docstring_as(Transport.put)
    def put(self, source, destination):
        try:
            gc3libs.log.debug("Running metohd: put. local source: %s. remote destination: %s. remote host: %s." % (source, destination, self.remote_frontend))
            self.sftp.put(source, destination)
        except:
            gc3libs.log.error("Failed method put. local source: %s remote host: %s" % (source, self.remote_frontend))
            raise Exceptions.TransportError("Failed method put. Error type %s, %s"
                                            % (sys.exc_info()[0], sys.exc_info()[1]))

    @same_docstring_as(Transport.get)
    def get(self, source, destination):
        try:
            gc3libs.log.debug("Running method: get. remote source %s. remote host: %s. local destination/: %s" % (source, self.remote_frontend, destination))
            self.sftp.get(source, destination)
        except:
            gc3libs.log.error("Failed method get. remote source: %s remote host: %s local destination: %s" % (source, self.remote_frontend, destination))
            raise Exceptions.TransportError("Failed method get. Error type %s, %s"
                                            % (sys.exc_info()[0], sys.exc_info()[1]))

    @same_docstring_as(Transport.remove)
    def remove(self, path):
        try:
            gc3libs.log.debug("Running method: remove. path: %s. remote host: %s" % (path, self.remote_frontend))
            self.sftp.remove(path)
        except IOError, x:
            gc3libs.log.error("Failed method remove. remote file: %s remote host: %s" % (path, self.remote_frontend))
            raise Exceptions.TransportError("Failed method remove. Error type %s, %s"
                                            % (sys.exc_info()[0], sys.exc_info()[1]))
        
    @same_docstring_as(Transport.remove_tree)
    def remove_tree(self, path):
        try:
            gc3libs.log.debug("Running metohd: remove_tree. remote path: %s remote host: %s" % (path, self.remote_frontend))
            # Note: At the moment rmdir does not work as expected
            # self.sftp.rmdir(path)
            # easy workaround: use SSHClient to issue an rm -rf comamnd
            _command = "rm -rf '%s'" % path
            exit_code, stdout, stderr = self.execute_command(_command)
            if exit_code != 0:
                gc3libs.log.error("remote command %s failed with code %d. stdout: %s. stderr: %s" % (_command, exit_code, stdout, stderr))
                raise Exception("remote command %s failed with code %d. stdout: %s. stderr: %s" % (_command, exit_code, stdout, stderr))
        except:
            gc3libs.log.error("Failed metohd remove_tree. remote folder: %s remote host: %s" % (path, self.remote_frontend))
            raise Exceptions.TransportError("Failed method remove. Error type %s, %s"
                                            % (sys.exc_info()[0], sys.exc_info()[1]))

    @same_docstring_as(Transport.open)
    def open(self, source, mode, bufsize=-1):
        try:
            return self.sftp.open(source, mode, bufsize)
        except:
            gc3libs.log.error("Failed method open. remote file: %s. remote host: %s" % (source, self.remote_frontend))
            raise Exceptions.TransportError("Failed method remove. Error type %s, %s"
                                            % (sys.exc_info()[0], sys.exc_info()[1]))
                       
    @same_docstring_as(Transport.close)
    def close(self):
        """
        Close the transport channel
        """
        gc3libs.log.debug("Closing sftp and ssh connections... ")
        if self.sftp is not None:
            self.sftp.close()
        if self.ssh is not None:
            self.ssh.close()
        self._is_open = False
        gc3libs.log.info("SshTransport status [ closed ]")


# -----------------------------------------------------------------------------
# Local Transport class
#

import subprocess
import shlex
import shutil


class LocalTransport(Transport):

    _is_open = False

    def __init__(self):
        pass

    @same_docstring_as(Transport.open)
    def open(self):
        self._is_open = True

    @same_docstring_as(Transport.execute_command)
    def execute_command(self, command):
        try:
            if self._is_open is False:
                raise Exception("Transport not open")

            subprocess_command = shlex.split(command)
            p = subprocess.Popen(subprocess_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)

            exitcode = p.comunicate()

            stdout = p.stdout.read()
            stderr = p.stderr.read()

            gc3libs.log.info('execute_command: %s. exit status: %d' % (command, exitcode))

            return exitcode, stdout, stderr

        except:
            gc3libs.log.error('Failed while executing command: %s' % command)
            raise TransportException("Failed while executing command: %s. Error type %s, %s" 
                                     % (command, sys.exc_info()[0], sys.exc_info()[1]))
        
    @same_docstring_as(Transport.listdir)
    def listdir(self, path):
        try:
            if self._is_open is False:
                raise Exception("Transport not open")

            return os.listdir(path)

        except:
            gc3libs.log.error('Failed method listdir on %s' % path, exc_info=True)
            raise TransportException("Failed method listdir. path: %s. Error type %s, %s"
                                     % (path, sys.exc_info()[0], sys.exc_info()[1]))

    @same_docstring_as(Transport.makedirs)
    def makedirs(self, path, mode=0777):
        os.path.makedirs(path, mode)

    @same_docstring_as(Transport.put)
    def put(self, source, destinaton):
        try:
            if self._is_open is False:
                raise Exception("Transport not open")

            gc3libs.log.debug("Running metohd: put. source: %s. destination: %s" % (source, destination))
            if source != destination:
                return shutil.copy(source, destination)
            else:
                gc3libs.log.warning("Trying to copy file over identical source and destination.")
                return True
        except:
            gc3libs.log.critical("Failed method put. source: %s. destination: %s" % (source, destinaton))
            raise TransportException("Failed method put. source: %s destination: %s. Error type %s, %s"
                                     % (source, destination, sys.exc_info()[0], sys.exc_info()[1]))


    @same_docstring_as(Transport.get)
    def get(self, source, destinaton):
        gc3libs.log.debug("GET implemented with PUT... ")
        self.put(source,destination)

    @same_docstring_as(Transport.remove)
    def remove(self, path):
        try:
            if self._is_open is False:
                raise Exception("Transport not open")

            gc3libs.log.debug("Removing %s", path)
            return os.remove(path)
        except:
            gc3libs.log.critical("Failed while removing file %s " % path)
            raise TransportException("Failed while removing file %s. Error type %s, %s"
                                     % (path, sys.exc_info()[0], sys.exc_info()[1]))

    @same_docstring_as(Transport.remove_tree)
    def remove_tree(self, path):
        try:
            if self._is_open is False:
                raise Exception("Transport not open")

            gc3libs.log.debug("Running method: remove_tree. path: %s" % path)
            return os.removedirs(path)
        except:
            gc3libs.log.critical("Failed method remove_tree. path: %s" % path)
            raise TransportException("Failed while removing folder %s. Error type %s, %s"
                                     % (path, sys.exc_info()[0], sys.exc_info()[1]))

    @same_docstring_as(Transport.close)
    def close(self):
        self._is_open = False
        gc3libs.log.info("SshTransport status [ closed ]")

# -----------------------------------------------------------------------------
