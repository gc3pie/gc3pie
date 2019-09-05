#! /usr/bin/env python
#
#   gcodeml.py -- Front-end script for submitting multiple CODEML jobs to SMSCG.
#
#   Copyright (C) 2010-2012  University of Zurich. All rights reserved.
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

from __future__ import absolute_import, print_function

__version__ = '2.0.5'
# summary of user-visible changes
__changelog__ = """
  2013-02-18:
    * Output files places in same folder as inputs.
  2012-12-05:
    * Allow requesting a specific version of CODEML/PAML
      via a command-line option.
  2012-06-20:
    * Save extended job information if the store allows it
      (presently, only `SqlStore` does).  The definition of
      the extra information table is hard-coded in this script,
      in the `GCodemlScript._make_session` method.
  2011-07-22:
    * Re-submit a CODEML job if it exits with nonzero exit code.
    * If no option ``-x``/``--codeml-executable`` is given on the
      command-line, `gcodeml` will now search for the
      ``APPS/BIO/CODEML-4.4.3`` application tag and use the remotely
      provided executable.
  2011-03-22:
    * New option ``-x`` to set the path to the ``codeml`` executable.
  2011-03-21:
    * Changed argument processing logic: no longer submit each
      ``.ctl`` file as a separate jobs, instead assume all the
      files in a single directory are related and bundle them into
      a single job.
    * New option ``-O`` to upload output files to a GridFTP
      server, instead of downloading them to a local destination.
  2011-02-08:
    * Initial release, forked off the ``ggamess`` sources.
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'



# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == '__main__':
    import gcodeml
    gcodeml.GCodemlScript().run()


# std module imports
import os
import os.path
import re
import sys

import sqlalchemy as sqla

# gc3 library imports
import gc3libs
import gc3libs.exceptions
import gc3libs.persistence.sql
import gc3libs.session
import gc3libs.utils

from gc3libs.application.codeml import CodemlApplication
from gc3libs.cmdline import SessionBasedScript, executable_file
from gc3libs.persistence.accessors import GET, GetValue
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask


## retry policy

class CodemlRetryPolicy(RetryableTask):

    def retry(self):
        # return True or False depending whether the application
        # should be re-submitted or not.
        # The actual CodemlApplication is available as `self.task`,
        # so for instance `self.task.valid[0]` is `True` iff the
        # H0.mlc file is present and processed correctly.
        # gc3libs.log.debug("CodemlRetryPolicy called!")
        # for now, do the default (see: gc3libs/__init__.py)
        to_retry = RetryableTask.retry(self)
        gc3libs.log.debug("CodemlRetryPolicy called with retry [%s]" % str(to_retry))
        return to_retry


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
            # `CodemlRetryPolicy` is the top-level object here,
            # so only print information about it.
#            stats_only_for = CodemlRetryPolicy,
             stats_only_for = gc3libs.application.codeml.CodemlApplication,
            )


    def setup_options(self):
        # add some options to the std ones
        self.add_param("-O", "--output-url", action="store",
                       dest="output_base_url", default="",
                       help="Upload output files to this URL,"
                       "which must be a protocol that ARC supports."
                       "(e.g., 'gsiftp://...')")
        self.add_param("-R", "--verno", metavar="VERNO",
                       dest="verno", default=CodemlApplication.DEFAULT_CODEML_VERSION,
                       help="Request the specified version of CODEML/PAML"
                       " (default: %(default)s).")
        self.add_param("-x", "--codeml-executable", metavar="PATH",
                       action="store", dest="codeml",
                       type=executable_file, default=None,
                       help=("Local path to the CODEML executable."
                             " By default, request the CODEML-%s run time tag"
                             " and use the remotely-provided application."
                           % CodemlApplication.DEFAULT_CODEML_VERSION))
        # change default for the "-o"/"--output" option
        self.actions['output'].default = 'NAME'


    def new_tasks(self, extra):
        """Implement the argument -> jobs mapping."""
        ## process additional options
        if self.params.codeml is not None:
            if not os.path.isabs(self.params.codeml):
                self.params.codeml = os.path.abspath(self.params.codeml)
            gc3libs.utils.check_file_access(self.params.codeml, os.R_OK|os.X_OK)

        ## collect input directories/files
        def contain_ctl_files(paths):
            for path in paths:
                if path.endswith('.ctl'):
                    return True
            return False

        input_dirs = set()
        total_n_ctl_files = 0

        for path in self.params.args:
            self.log.debug("Now processing input argument '%s' ..." % path)
            if not os.path.isdir(path):
                raise gc3libs.exceptions.InvalidUsage(
                    "Argument '%s' is not a directory path." % path)

            # recursively scan for input files
            for dirpath, dirnames, filenames in os.walk(path):
                if contain_ctl_files(filenames):
                    input_dirs.add(os.path.realpath(dirpath))

        self.log.debug("Gathered input directories: '%s'"
                       % str.join("', '", input_dirs))

        for dirpath in input_dirs:
            # gather control files; other input files are automatically
            # pulled in by CodemlApplication by parsing the '.ctl'
            ctl_files = [ os.path.join(dirpath, filename)
                          for filename in os.listdir(dirpath)
                          if filename.endswith('.ctl') ]

            total_n_ctl_files += len(ctl_files) # AK: DEBUG
            # check if the dir contains exactly two control files (*.H0.ctl and *.H1.ctl)
            if len(ctl_files) != 2:
                raise gc3libs.exceptions.InvalidUsage(
                    "The input directory '%s' must contain exactly two control files." % dirpath)

            self.log.debug("Gathered control files: '%s':" % str.join("', '", ctl_files))

            kwargs = extra.copy()

            ## create new CODEML application instance

            # Python 2.4 does not allow named arguments after a
            # variable-length positional argument list (*args), so we
            # need to pass the named arguments as part of the `extra_args`
            # dictionary.
            kwargs['codeml'] = self.params.codeml
            kwargs['version'] = self.params.verno
            kwargs['requested_memory'] = self.params.memory_per_core
            kwargs['requested_cores'] = self.params.ncores
            kwargs['requested_walltime'] = self.params.walltime

            jobname = (os.path.basename(dirpath) or dirpath) + '.out'

            if self.params.output == 'NAME':
                # This is the 'default' setting
                # gcodeml overrides it
                # Results in same folder as inputs
                kwargs['output_dir'] = os.path.join(dirpath,'.compute')
                kwargs['result_dir'] = dirpath
            else:
                # command line option takes precendence
                # in this case, just set output_dir to what has been specified
                # as '--output' option
                kwargs['output_dir'] = self.make_directory_path(self.params.output, jobname)

            # FIXME: should this reflect the same policy as 'output_dir' ?
            # set optional arguments (path to 'codeml' binary, output URL, etc.)
            if self.params.output_base_url != "":
               kwargs['output_base_url'] = self.params.output_base_url

            app = CodemlApplication(*ctl_files, **kwargs)

            # yield new job
            yield (
                jobname, # unique string tagging this job; duplicate jobs are discarded
                CodemlRetryPolicy, # the task constructor
                [ # the following parameters are passed to the
                    # `CodemlRetryPolicy` constructor:
                    app,     # = task; the codeml application object defined above
                    3        # = max_retries; max no. of retries of the task
                ],
                # extra keyword parameters to `CodemlRetryPolicy`
                dict()
                )

        self.log.debug("Total number of control files: %d", total_n_ctl_files) # AK: DEBUG

    def _make_session(self, session_uri, store_url):
        return gc3libs.session.Session(
            session_uri,
            store_url,
            extra_fields = {
                # NB: enlarge window to at least 150 columns to read this table properly!
                sqla.Column('class',              sqla.TEXT())    : (lambda obj: obj.__class__.__name__)                                              , # task class
                sqla.Column('name',               sqla.TEXT())    : GetValue()             .jobname                                                   , # job name
                sqla.Column('executable',         sqla.TEXT())    : GetValue(default=None) .arguments[0]                        ,#.ONLY(CodemlApplication), # program executable
                sqla.Column('output_path',        sqla.TEXT())    : GetValue(default=None) .output_dir                        ,#.ONLY(CodemlApplication), # fullpath to codeml output directory
                sqla.Column('input_path',         sqla.TEXT())    : _get_input_path                                                                   , # fullpath to codeml input directory
                sqla.Column('mlc_exists_h0',      sqla.TEXT())    : GetValue(default=None) .exists[0]                         ,#.ONLY(CodemlApplication), # exists codeml *.H0.mlc output file
                sqla.Column('mlc_exists_h1',      sqla.TEXT())    : GetValue(default=None) .exists[1]                         ,#.ONLY(CodemlApplication), # exists codeml *.H1.mlc output file
                sqla.Column('mlc_valid_h0',       sqla.TEXT())    : GetValue(default=None) .valid[0]                          ,#.ONLY(CodemlApplication), #.attrid codeml *.H0.mlc output file
                sqla.Column('mlc_valid_h1',       sqla.TEXT())    : GetValue(default=None) .valid[1]                          ,#.ONLY(CodemlApplication), #.attrid codeml *.H1.mlc output file
                sqla.Column('cluster',            sqla.TEXT())    : GetValue(default=None) .execution.resource_name           ,#.ONLY(CodemlApplication), # cluster/compute element
                sqla.Column('worker',             sqla.TEXT())    : GetValue(default=None) .hostname                          ,#.ONLY(CodemlApplication), # hostname of the worker node
                sqla.Column('cpu',                sqla.TEXT())    : GetValue(default=None) .cpuinfo                           ,#.ONLY(CodemlApplication), # CPU model of the worker node
                sqla.Column('codeml_walltime_h0', sqla.INTEGER()) : GetValue()             .time_used[0]                      ,#.ONLY(CodemlApplication), # time used by the codeml H0 run (sec)
                sqla.Column('codeml_walltime_h1', sqla.INTEGER()) : GetValue()             .time_used[1]                      ,#.ONLY(CodemlApplication), # time used by the codeml H1 run (sec)
                sqla.Column('aln_len',            sqla.TEXT())    : GetValue()             .aln_info['aln_len']                                    , # alignement length
                sqla.Column('seq',                sqla.TEXT())    : GetValue()             .aln_info['n_seq']                                      , # num of sequences
                sqla.Column('requested_walltime', sqla.INTEGER()) : _get_requested_walltime_or_none                           , # requested walltime, in hours
                sqla.Column('requested_cores',    sqla.INTEGER()) : GetValue(default=None) .requested_cores                   ,#.ONLY(CodemlApplication), # num of cores requested
                sqla.Column('tags',               sqla.TEXT())    : GetValue()             .tags[0]                           ,#.ONLY(CodemlApplication), # run-time env.s (RTE) requested; e.g. 'APPS/BIO/CODEML-4.4.3'
                sqla.Column('used_walltime',      sqla.INTEGER()) : GetValue(default=None) .execution.used_walltime           ,#.ONLY(CodemlApplication), # used walltime
                sqla.Column('lrms_jobid',         sqla.TEXT())    : GetValue(default=None) .execution.lrms_jobid              ,#.ONLY(CodemlApplication), # arc job ID
                sqla.Column('original_exitcode',  sqla.INTEGER()) : GetValue(default=None) .execution.original_exitcode       ,#.ONLY(CodemlApplication), # original exitcode
                sqla.Column('used_cputime',       sqla.INTEGER()) : GetValue(default=None) .execution.used_cputime            ,#.ONLY(CodemlApplication), # used cputime in sec
                # returncode = exitcode*256 + signal
                sqla.Column('returncode',         sqla.INTEGER()) : GetValue(default=None) .execution.returncode              ,#.ONLY(CodemlApplication), # returncode attr
                sqla.Column('queue',              sqla.TEXT())    : GetValue(default=None) .execution.queue                   ,#.ONLY(CodemlApplication), # exec queue _name_
                sqla.Column('time_submitted',     sqla.FLOAT())   : GetValue(default=None) .execution.timestamp['SUBMITTED']  ,#.ONLY(CodemlApplication), # client-side submission (float) time
                sqla.Column('time_terminated',    sqla.FLOAT())   : GetValue(default=None) .execution.timestamp['TERMINATED'] ,#.ONLY(CodemlApplication), # client-side termination (float) time
                sqla.Column('time_stopped',       sqla.FLOAT())   : GetValue(default=None) .execution.timestamp['STOPPED']    ,#.ONLY(CodemlApplication), # client-side stop (float) time
                sqla.Column('retried',            sqla.INTEGER()) : GetValue(default=None) .retried                           .ONLY(CodemlRetryPolicy), # num of times job has been retried
                })

# auxiliary getter functions, used in `GCodemlScript_make_session` above
def _get_input_path_CodemlApplication(job):
    # we know that each CodemlApplication has only two .ctl
    # files as input, and they belong in the same directory,
    # so just pick the first one
    for src_url in job.inputs:
        if src_url.path.endswith('.ctl'):
            return os.path.dirname(src_url.path)

def _get_input_path(job):
    if isinstance(job, gc3libs.application.codeml.CodemlApplication):
        return _get_input_path_CodemlApplication(job)
    elif isinstance(job, CodemlRetryPolicy):
        return _get_input_path_CodemlApplication(job.task)
    else:
        return None

def _get_requested_walltime_or_none(job):
    if isinstance(job, gc3libs.application.codeml.CodemlApplication):
        return job.requested_walltime.amount(hours)
    else:
        return None
