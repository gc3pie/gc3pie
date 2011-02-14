#! /usr/bin/env python
#
#   gcodeml.py -- Front-end script for submitting multiple CODEML jobs to SMSCG.
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
Front-end script for submitting multiple CODEML jobs to SMSCG.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gcodeml --help`` for program usage instructions.
"""
__version__ = '$Revision$'
# summary of user-visible changes
__changelog__ = """
  2011-02-08:
    * Initial release, forked off the ``ggamess`` sources.
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'


import fnmatch
import logging
import os
import os.path
import re

import gc3libs
from gc3libs.cmdline import SessionBasedScript


## The CODEML application

class CodemlApplication(gc3libs.Application):
    """
    Run a CODEML job with the specified '.ctl' files.
    
    The given '.ctl' input files are parsed and the '.phy' and
    '.nwk' files mentioned therein are added to the list of files
    to be copied to the execution site.
    """
    
    def __init__(self, *ctls, **kw):
        # need to send the binary and the PERL driver script
        inputs = [ 'codeml.pl', 'codeml' ]
        # for each '.ctl' file, extract the referenced "seqfile" and
        # "treefile" and add them to the input list
        for ctl in ctls:
            try:
                # try getting the seqfile/treefile path before we
                # append the '.ctl' file to inputs; if they cannot be
                # found, we do not append the '.ctl' either...
                for path in CodemlApplication.seqfile_and_treefile(ctl).values():
                    if path not in inputs:
                        inputs.append(path)
                inputs.append(ctl)
            # if the phy/nwk files are not found,
            # `seqfile_and_treefile` raises an exception; catch it
            # here and ignore the '.ctl' file as well.
            except RuntimeError, ex:
                # FIXME: this is using the root logger to spit out messages!!
                logging.warning("Cannot find seqfile and/or treefile referenced in '%s'"
                                " - ignoring this input." % ctl)
        gc3libs.Application.__init__(
            self,
            executable = 'codeml.pl',
            arguments = [ os.path.basename(ctl) for ctl in ctls ],
            inputs = inputs,
            outputs = [ (os.path.basename(path)[:-4] + '.mlc')
                        for path in ctls ],
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

    # aux function to get thw seqfile and treefile paths
    @staticmethod
    def seqfile_and_treefile(ctl_path):
        """
        Return full path to the seqfile and treefile referenced in
        the '.ctl' file given as arguments.
        """
        dirname = os.path.dirname(ctl_path)
        def abspath(filename):
            if os.path.isabs(filename):
                return filename
            else:
                return os.path.realpath(os.path.join(dirname, filename))
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
            if key in [ 'seqfile', 'treefile' ]:
                result[key] = abspath(value)
            # shortcut: if we already have both 'seqfile' and
            # 'treefile', there's no need for scanning the file
            # any more.
            if len(result) == 2:
                ctl.close()
                return result
        # if we get to this point, the ``seqfile = ...`` and
        # ``treefile = ...`` lines were not found; signal this to the
        # caller by raising an exception
        ctl.close()
        raise RuntimeError("Could not extract path to seqfile and/or treefile from '%s'"
                           % ctl_path)


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

        total = len(self.outputs)
        # form full-path to the output files
        outputs = [ os.path.join(download_dir, filename) 
                    for filename in fnmatch.filter(os.listdir(download_dir), '*.mlc') ]
        if len(outputs) == 0:
            # no output retrieved, did ``codeml`` run at all?
            self.execution.exitcode = 127
            return
        # count the number of successfully processed files
        failed = 0
        for mlc in outputs:
            output_file = open(mlc, 'r')
            last_line = output_file.readlines()[-1]
            output_file.close()
            if not last_line.startswith('Time used: '):
                failed += 1
        # set exit code and informational message
        if failed == 0:
            self.execution.exitcode = 0
            self.info = "All files processed successfully, output downloaded to '%s'" % download_dir
        elif failed < total:
            self.execution.exitcode = 1
            self.info = "Some files *not* processed successfully, output downloaded to '%s'" % download_dir
        else:
            self.execution.exitcode = 2
            self.info = "No files processed successfully, output downloaded to '%s'" % download_dir
        return


## the script itself

class GCodemlScript(SessionBasedScript):
    """
    Scan the specified INPUTDIR directories recursively for '.ctl' files,
    and submit a CODEML job for each input file found; job progress is
    monitored and, when a job is done, its '.mlc' file is retrieved back
    into the same directory where the '.ctl' file is (this can be
    overridden with the '-o' option).
    
    The `gcodeml` command keeps a record of jobs (submitted, executed and
    pending) in a session file (set name with the '-s' option); at each
    invocation of the command, the status of all recorded jobs is updated,
    output from finished jobs is collected, and a summary table of all
    known jobs is printed.  New jobs are added to the session if new input
    files are added to the command line.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; `gcodeml` will delay submission
    of newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = CodemlApplication,
            input_filename_pattern = '*.ctl'
            )

# run it
if __name__ == '__main__':
    GCodemlScript().run()
