#! /usr/bin/env python
#
#   codeml.py -- Simple interface to the CODEML application
#
#   Copyright (C) 2010, 2011 GC3, University of Zurich
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
Simple interface to the CODEML application.
"""
__version__ = '1.0rc6 (SVN $Revision$)'
# summary of user-visible changes
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'


import fnmatch
import logging
import os
import os.path
import re

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.utils


## The CODEML application

class CodemlApplication(gc3libs.Application):
    """
    Run a CODEML job with the specified '.ctl' files.
    
    The given '.ctl' input files are parsed and the '.phy' and
    '.nwk' files mentioned therein are added to the list of files
    to be copied to the execution site.
    """
    
    def __init__(self, *ctls, **kw):
        # optinal keyword argument 'codeml', defaulting to "codeml"
        codeml = kw.get('codeml', 'codeml')
        # we're submitting CODEML jobs thorugh the support script
        # "codeml.pl", so do the specific setup tailored to this
        # script' usage
        codeml_pl = resource_filename(Requirement.parse("gc3pie"), 
                                      "gc3libs/etc/codeml.pl")

        # need to send the binary and the PERL driver script
        inputs = { codeml_pl:'codeml.pl', codeml:'codeml' }
        # output file paths are read from the '.ctl' file below
        outputs = [ ]
        # for each '.ctl' file, extract the referenced "seqfile" and
        # "treefile" and add them to the input list
        for ctl in ctls:
            try:
                # try getting the seqfile/treefile path before we
                # append the '.ctl' file to inputs; if they cannot be
                # found, we do not append the '.ctl' either...
                for (key, path) in CodemlApplication.aux_files(ctl).items():
                    if key in ['seqfile', 'treefile'] and path not in inputs:
                        inputs[path] = os.path.basename(path)
                    if key == 'outfile' and path not in outputs:
                        outputs.append(path)
                inputs[ctl] = os.path.basename(ctl)
            # if the phy/nwk files are not found, `aux_files` raises
            # an exception; catch it here and ignore the '.ctl' file
            # as well.
            except RuntimeError, ex:
                gc3libs.log.warning("Ignoring input file '%s':"
                                    " cannot find seqfile and/or treefile referenced in it: %s"
                                    % (ctl, str(ex)))
        gc3libs.Application.__init__(
            self,
            executable = os.path.basename(codeml_pl),
            arguments = [ os.path.basename(ctl) for ctl in ctls ],
            inputs = inputs,
            outputs = outputs,
            stdout = 'codeml.stdout.txt',
            stderr = 'codeml.stderr.txt',
            # an estimation of wall-clock time requirements can be
            # derived from the '.phy' input file, use it to set the
            # `required_walltime` attribute, so we do not risk jobs
            # being killed because they exceed allotted running time
            #required_walltime = ...,
            **kw
            )

    # split a line 'key = value' around the middle '=' and ignore spaces
    _assignment_re = re.compile('\s* = \s*', re.X)
    _aux_file_keys = [ 'seqfile', 'treefile', 'outfile' ]
    
    # aux function to get thw seqfile and treefile paths
    @staticmethod
    def aux_files(ctl_path):
        """
        Return full path to the seqfile and treefile referenced in
        the '.ctl' file given as arguments.
        """
        dirname = os.path.dirname(ctl_path)
        def abspath(filename):
            if os.path.isabs(filename):
                return filename
            else:
                return os.path.join(dirname, filename)
        result = { }
        ctl = open(ctl_path, 'r')
        for line in ctl.readlines():
            # remove comments (from '*' to end-of line)
            line = line.split('*')[0]
            # remove leading and trailing whitespace
            line = line.strip()
            # ignore empty lines
            if len(line) == 0:
                continue
            key, value = CodemlApplication._assignment_re.split(line, maxsplit=1)
            if key not in CodemlApplication._aux_file_keys:
                continue
            elif key in [ 'seqfile', 'treefile' ]:
                result[key] = abspath(value)
            elif key == 'outfile':
                result[key] = value
            # shortcut: if we already have all files, there's no need
            # for scanning the file any more.
            if len(result) == len(CodemlApplication._aux_file_keys):
                ctl.close()
                return result
        # if we get to this point, the ``seqfile = ...`` and
        # ``treefile = ...`` lines were not found; signal this to the
        # caller by raising an exception
        ctl.close()
        raise RuntimeError("Could not extract path to seqfile and/or treefile from '%s'"
                           % ctl_path)

    #def terminated(self):
    #    if job failed because of recoverable condition:
    #        self.submit()


    def postprocess(self, download_dir):
        """
        Set the exit code of a `CodemlApplication` job by inspecting its
        ``.mlc`` output files.

        An output file is valid iff its last line of each output file
        reads ``Time used: HH:M``.

        The exit status of the whole job is set to one of these values:

        *  0 -- all files processed successfully
        *  1 -- some files were *not* processed successfully
        *  2 -- no files processed successfully
        * 127 -- the ``codeml`` application did not run at all.
         
        """
        # XXX: when should we consider an application "successful" here?
        # In the Rosetta ``docking_protocol`` application, the aim is to get
        # at least *some* decoys generated: as long as there are a few decoys
        # in the output, we do not care about the rest.  Is this approach ok
        # in Codeml/Selectome as well?
        
        # Except for "signal 125" (submission to batch system failed),
        # any other error condition may result in some output files having
        # been computed/generated, so let us continue in those cases and
        # not care about the exit signal...
        if self.execution.signal == 125:
            # submission failed, job did not run at all
            self.execution.exitcode = 127
            return

        # if output files were *not* uploaded to a remote server,
        # then check if they are OK and set exit code based on this
        if self.output_base_url is not None:
            # form full-path to the stdout files
            outputs = [ os.path.join(download_dir, filename) 
                        for filename in fnmatch.filter(os.listdir(download_dir), '*.mlc') ]
            if len(outputs) == 0:
                # no output retrieved, did ``codeml`` run at all?
                self.execution.exitcode = 127
                return
        # count the number of successfully processed files; note that
        # `self.outputs` contains the list of output files *plus*
        # stdout and stderr (if distinct)
        total = len(self.outputs) - gc3libs.utils.ifelse(self.join, 1, 2)
        failed = 0
        stdout_file = open(os.path.join(download_dir, self.stdout), 'r')
        ok_count = gc3libs.utils.count(stdout_file,
                                       lambda line: line.startswith('Time used: '))
        stdout_file.close()
        # set exit code and informational message
        if ok_count == total:
            self.execution.exitcode = 0
            self.info = "All files processed successfully, output downloaded to '%s'" % download_dir
        elif ok_count == 0:
            self.execution.exitcode = 2
            self.info = "No files processed successfully, output downloaded to '%s'" % download_dir
        else:
            self.execution.exitcode = 1
            self.info = "Some files *not* processed successfully, output downloaded to '%s'" % download_dir
        return
            
