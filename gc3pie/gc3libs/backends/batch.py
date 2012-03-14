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
__version__ = 'development version (SVN $Revision$)'

from gc3libs.backends import LRMS
from gc3libs.utils import same_docstring_as


# Define some commonly used functions

def get_qsub_jobid(output, regexp):
    """Parse the output of the submission command. Regexp must be
    provided by the caller."""
    for line in output.split('\n'):
        match = regexp.match(line)
        if match:
            return match.group('jobid')
    raise gc3libs.exceptions.InternalError("Could not extract jobid from qsub output '%s'" 
                        % qsub_output.rstrip())
    

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

def make_remote_and_local_path_pair(transport, job, remote_relpath, local_root_dir, local_relpath):
    """
    Return list of (remote_path, local_path) pairs corresponding to 
    """
    # see https://github.com/fabric/fabric/issues/306 about why it is
    # correct to use `posixpath.join` for remote paths (instead of `os.path.join`)
    remote_path = posixpath.join(job.ssh_remote_folder,
                                 batch.generic_filename_mapping(job.lrms_jobname, job.lrms_jobid,
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
    @same_docstring_as(LRMS.peek)
    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        job = app.execution
        assert job.has_key('ssh_remote_folder'), \
            "Missing attribute `ssh_remote_folder` on `Job` instance passed to `PbsLrms.peek`."

        if size is None:
            size = sys.maxint

        _filename_mapping = batch.generic_filename_mapping(job.lrms_jobname, job.lrms_jobid, remote_filename)
        _remote_filename = os.path.join(job.ssh_remote_folder, _filename_mapping)

        try:
            self.transport.connect()
            remote_handler = self.transport.open(_remote_filename, mode='r', bufsize=-1)
            remote_handler.seek(offset)
            data = remote_handler.read(size)
            # self.transport.close()
        except Exception, ex:
            # self.transport.close()
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

    @same_docstring_as(LRMS.validate_data)
    def close(self):
        self.transport.close()
        
    

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="sge",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
