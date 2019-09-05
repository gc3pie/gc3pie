#! /usr/bin/env python

"""
Simple interface to the CODEML application.
"""

#   codeml.py -- Simple interface to the CODEML application
#
#   Copyright (C) 2010, 2011, 2012, 2019  University of Zurich. All rights reserved.
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


from __future__ import absolute_import, print_function, unicode_literals
from builtins import str
from builtins import range
__changelog__ = """
Summary of user-visible changes:
* 16-08-2011 AM: Extract aln_info from the .phy file and record it
* 14-12-2011 SM: Parse `Time used:` line with hours now
* 21-11-2011 RM: Mark job as successful if the output file parses OK
* 29-04-2011 HS: changed to use RTE
* 04-05-2011 AK: import sys module
                 print DEBUG statements (full paths of driver and application)
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'


import fnmatch
import logging
import os
import os.path
import re
import sys

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.utils


# The CODEML application

class CodemlApplication(gc3libs.Application):

    """
    Run a CODEML job with the specified '.ctl' files.

    The given '.ctl' input files are parsed and the '.phy' and
    '.nwk' files mentioned therein are added to the list of files
    to be copied to the execution site.
    """

    application_name = 'codeml'

    DEFAULT_CODEML_VERSION = '4.4.3'

    def __init__(self, *ctls, **extra_args):
        # optional keyword argument 'codeml', defaulting to None
        codeml = extra_args.get('codeml', None)
        version = extra_args.get('version', self.DEFAULT_CODEML_VERSION)
        # we're submitting CODEML jobs thorugh the support script
        # "codeml.pl", so do the specific setup tailored to this
        # script' usage
        codeml_pl = resource_filename(Requirement.parse("gc3pie"),
                                      "gc3libs/etc/codeml.pl")

        # need to send the PERL driver script, and the binary only
        # if we're not using the RTE
        inputs = {codeml_pl: 'codeml.pl'}

        # include comdel in the list of executables
        if 'executables' in extra_args:
            extra_args['executables'].append('codeml.pl')
        else:
            extra_args['executables'] = ['codeml.pl']

        if codeml is None:
            # use the RTE
            rte = ('APPS/BIO/CODEML-%s' % version)
            if 'tags' in extra_args:
                extra_args['tags'].append(rte)
            else:
                extra_args['tags'] = [rte]
        else:
            # use provided binary
            inputs[codeml] = 'codeml'

        # set result dir: where the expected output files
        # will be copied as part of 'terminated'
        if 'result_dir' in extra_args:
            self.result_dir = extra_args['result_dir']

        # output file paths are read from the '.ctl' file below
        outputs = []
        # for each '.ctl' file, extract the referenced "seqfile" and
        # "treefile" and add them to the input list
        for ctl in ctls:
            try:
                # try getting the seqfile/treefile path before we
                # append the '.ctl' file to inputs; if they cannot be
                # found, we do not append the '.ctl' either...
                for (key, path) in list(CodemlApplication.aux_files(ctl).items()):
                    if key in ['seqfile', 'treefile'] and path not in inputs:
                        inputs[path] = os.path.basename(path)

                    if key == 'seqfile':
                        # Parse phy files and fill `aln_info` attribute
                        try:
                            fd = open(path)
                            aln_infos = fd.readline().strip().split()
                            self.aln_info = {
                                'n_seq': int(aln_infos[0]),
                                'aln_len': int(aln_infos[1]),
                            }
                            fd.close()
                        except Exception as ex:
                            gc3libs.log.warning(
                                "Unable to parse `n_seq` and `aln_len` values"
                                " from `.phy` file `%s`: %s", path, str(ex))
                            try:
                                fd.close()
                            except:
                                pass

                    elif key == 'outfile' and path not in outputs:
                        outputs.append(path)

                inputs[ctl] = os.path.basename(ctl)
            # if the phy/nwk files are not found, `aux_files` raises
            # an exception; catch it here and ignore the '.ctl' file
            # as well.
            except RuntimeError as ex:
                gc3libs.log.warning(
                    "Ignoring input file '%s':"
                    " cannot find seqfile and/or treefile referenced in it: %s" %
                    (ctl, str(ex)))
        gc3libs.Application.__init__(
            self,
            arguments=["./codeml.pl"] +
            [os.path.basename(ctl) for ctl in ctls],
            inputs=inputs,
            outputs=outputs,
            stdout='codeml.stdout.txt',
            stderr='codeml.stderr.txt',
            # an estimation of wall-clock time requirements can be
            # derived from the '.phy' input file, use it to set the
            # `required_walltime` attribute, so we do not risk jobs
            # being killed because they exceed allotted running time
            # required_walltime = ...,
            **extra_args
        )

        # these attributes will get their actual value after
        # `terminated()` has run; pre-set them here to an invalid
        # value so they show up in `ginfo` output.
        self.hostname = None
        self.cpuinfo = None
        self.time_used = None

        self.exists = [None] * len(ctls)
        self.valid = [None] * len(ctls)
        self.time_used = [None] * len(ctls)

    # split a line 'key = value' around the middle '=' and ignore spaces
    _assignment_re = re.compile(r'\s* = \s*', re.X)
    _aux_file_keys = ['seqfile', 'treefile', 'outfile']

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
        result = {}
        ctl = open(ctl_path, 'r')
        for line in ctl.readlines():
            # remove comments (from '*' to end-of line)
            line = line.split('*')[0]
            # remove leading and trailing whitespace
            line = line.strip()
            # ignore empty lines
            if len(line) == 0:
                continue
            key, value = CodemlApplication._assignment_re.split(
                line, maxsplit=1)
            if key not in CodemlApplication._aux_file_keys:
                continue
            elif key in ['seqfile', 'treefile']:
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
        raise RuntimeError(
            "Could not extract path to seqfile and/or treefile from '%s'" %
            ctl_path)

    @staticmethod
    def parse_output_file(path):
        if not os.path.exists(path):
            return 'no file'
        output_file = open(path, 'r')
        time_used_found = False
        for line in output_file:
            match = CodemlApplication._TIME_USED_RE.match(line)
            if match:
                if match.group('hours') is not None:
                    hours = int(match.group('hours'))
                else:
                    hours = 0
                minutes = int(match.group('minutes'))
                seconds = int(match.group('seconds'))
                time_used_found = True
                break
        if time_used_found:
            return (hours * 3600 + minutes * 60 + seconds)
        else:
            return 'invalid'

    # stdout starts with `HOST: ...` and `CPU: ` lines, but there's a
    # variable number of spaces so we need a regexp to match
    _KEY_VALUE_SEP = re.compile(r':\s*')

    _TIME_USED_RE = re.compile(
        r'Time \s+ used:\s+ ((?P<hours>[0-9]*):)?(?P<minutes>[0-9]+):(?P<seconds>[0-9]+)',
        re.X)
    _H_WHICH_RE = re.compile(r'H(?P<n>[01]).mlc')

    # perform the post-processing and set exit code.
    # This is the final result verification of a single codeml H0/H1 tuple
    # (job)
    def terminated(self):
        """
        Set the exit code of a `CodemlApplication` job by inspecting its
        ``.mlc`` output files.

        An output file is valid iff its last line of each output file
        reads ``Time used: MM:SS`` *or* ``Time used: HH:MM:SS``

        The exit status of the whole job is a bit field composed as follows:

        =======  ====================
        bit no.  meaning
        =======  ====================
        0        H1.mlc valid (0=valid, 1=invalid)
        1        H1.mlc present (0=present, 1=no file)
        2        H0.mlc valid (0=valid, 1=invalid)
        3        H0.mlc present (0=present, 1=not present)
        7        error running codeml (1=error, 0=ok)
        =======  ====================

        The special value 127 is returned in case ``codeml`` did not
        run at all (Grid or remote cluster error).

        So, exit code 0 means that all files processed successfully,
        code 1 means that ``H0.mlc`` has not been downloaded (for whatever reason).

        TODO:
          * Check if the stderr is empty.
        """
        # Except for "signal 125" (submission to batch system failed),
        # any other error condition may result in some output files having
        # been computed/generated, so let us continue in those cases and
        # not care about the exit signal...
        if self.execution.signal == 125:
            # submission failed, job did not run at all
            self.execution.exitcode = 127
            return

        # form full-path to the stdout files
        download_dir = self.output_dir

        # check whether 'download_dir' exists at all
        if not os.path.isdir(download_dir):
            # output folder not available
            self.execution.exitcode = 127
            return

        outputs = [
            os.path.join(
                download_dir,
                filename) for filename in fnmatch.filter(
                os.listdir(download_dir),
                '*.mlc')]
        if len(outputs) == 0:
            # no output retrieved, did ``codeml`` run at all?
            self.execution.exitcode = 127
            return

        # if output files were *not* uploaded to a remote server,
        # then check if they are OK and set exit code based on this
        if self.output_base_url is None:
            failed = 0
            for output_path in outputs:
                match = CodemlApplication._H_WHICH_RE.search(output_path)
                if match:
                    n = int(match.group('n'))
                else:
                    gc3libs.log.debug(
                        "Output file '%s' does not match pattern 'H*.mlc' -- ignoring.")
                    continue  # with next output_path
                duration = CodemlApplication.parse_output_file(output_path)
                if duration == 'no file':
                    self.exists[n] = False
                    self.valid[n] = False
                    failed += 1
                elif duration == 'invalid':
                    self.exists[n] = True
                    self.valid[n] = False
                    failed += 1
                else:
                    self.exists[n] = True
                    self.valid[n] = True
                    self.time_used[n] = duration

        # if output files parsed OK, then move them to 'result_dir'
        if self.result_dir:
            import shutil
            for output_path in outputs:
                try:
                    shutil.copy(output_path,
                                os.path.join(self.result_dir,
                                             os.path.basename(output_path)))
                    os.remove(output_path)
                except Exception as ex:
                    gc3libs.log.error(
                        "Failed while transferring output file " +
                        "%s " %
                        output_path +
                        "to result folder %s. " %
                        self.result_dir +
                        "Error type %s. Message %s. " %
                        (type(ex),
                         str(ex)))
                    failed += 1
                    continue
        else:
            gc3libs.log.warning(
                "No 'result_dir' set. Leaving results in original" +
                " output folder %s" %
                self.output_dir)

        # if output files parsed OK, then override the exit code and
        # mark the job as successful
        if failed == 0:
            self.execution.returncode = (0, 0)

        # set object attributes based on tag lines in the output
        stdout_path = os.path.join(download_dir, self.stdout)
        if os.path.exists(stdout_path):
            stdout_file = open(stdout_path, 'r')
            for line in stdout_file:
                line = line.strip()
                if line.startswith('HOST:'):
                    tag, self.hostname = CodemlApplication._KEY_VALUE_SEP.split(
                        line, maxsplit=1)
                elif line.startswith('CPU:'):
                    tag, self.cpuinfo = CodemlApplication._KEY_VALUE_SEP.split(
                        line, maxsplit=1)
                    break
            stdout_file.close()

        # set exit code and informational message
        rc = 0
        for n in range(len(self.exists)):
            if not self.exists[n]:
                rc |= 1
            rc *= 2
            if not self.valid[n]:
                rc |= 1
            rc *= 2
        self.execution.exitcode = rc

        # all done
        return
