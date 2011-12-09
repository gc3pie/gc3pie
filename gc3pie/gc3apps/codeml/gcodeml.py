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
__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
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


# workaround for Issue 95,
# see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import gcodeml


# std module imports
import os
import os.path
import re
import sys

# gc3 library imports
import gc3libs
from gc3libs.application.codeml import CodemlApplication
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.exceptions
import gc3libs.utils


## retry policy

class CodemlRetryPolicy(gc3libs.RetryableTask, gc3libs.utils.Struct):

    def retry(self):
        # return True or False depending whether the application
        # should be re-submitted or not.
        # The actual CodemlApplication is available as `self.task`,
        # so for instance `self.task.valid[0]` is `True` iff the
        # H0.mlc file is present and processed correctly.
        # gc3libs.log.debug("CodemlRetryPolicy called!")
        # for now, do the default (see: gc3libs/__init__.py)
        to_retry = gc3libs.RetryableTask.retry(self)
        gc3libs.log.debug("CodemlRetryPolicy called with retry [%s]" % str(to_retry))
        # return gc3libs.RetryableTask.retry(self)
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
#            stats_only_for = gcodeml.CodemlRetryPolicy,
             stats_only_for = gc3libs.application.codeml.CodemlApplication,
            )


    def setup_options(self):
        # add some options to the std ones
        self.add_param("-O", "--output-url", action="store",
                       dest="output_base_url", default="",
                       help="Upload output files to this URL,"
                       "which must be a protocol that ARC supports."
                       "(e.g., 'gsiftp://...')")
        self.add_param("-x", "--codeml-executable", metavar="PATH",
                       action="store", dest="codeml",
                       type=executable_file, default=None, 
                       help="Local path to the CODEML executable."
                       " By default, request the CODEML-4.4.3 run time tag"
                       " and use the remotely-provided application.")
        # change default for the "-o"/"--output" option
        self.actions['output'].default = 'PATH/NAME'


    def new_tasks(self, extra):
        """Implement the argument -> jobs mapping."""
        ## process additional options
        if self.params.codeml is not None:
            if not os.path.isabs(self.params.codeml):
                self.params.codeml = os.path.abspath(self.params.codeml)
            gc3libs.utils.test_file(self.params.codeml, os.R_OK|os.X_OK)

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
            # set optional arguments (path to 'codeml' binary, output URL, etc.)
            kwargs = extra.copy()
            if self.params.output_base_url != "":
               kwargs['output_base_url'] = self.params.output_base_url

            ## create new CODEML application instance

            # Python 2.4 does not allow named arguments after a
            # variable-length positional argument list (*args), so we
            # need to pass the named arguments as part of the `kw`
            # dictionary.
            kwargs['codeml'] = self.params.codeml
            kwargs['requested_memory'] = self.params.memory_per_core
            kwargs['requested_cores'] = self.params.ncores
            kwargs['requested_walltime'] = self.params.walltime
            # Use the `make_directory_path` method (from
            # `SessionBasedScript`) to expand strings like ``PATH``,
            # ``NAME``, etc. in the template.  The ``PATH`` will be
            # set from the directory containing the first ``.ctl``
            # file.
            jobname = (os.path.basename(dirpath) or dirpath) + '.out'
            kwargs['output_dir'] = self.make_directory_path(self.params.output, jobname, *ctl_files)

            app = CodemlApplication(*ctl_files, **kwargs)

            # yield new job
            yield (
                jobname, # unique string tagging this job; duplicate jobs are discarded
                gcodeml.CodemlRetryPolicy, # the task constructor
                [ # the following parameters are passed to the
                    # `CodemlRetryPolicy` constructor:
                    jobname, # = name; used for display purposes
                    app,     # = task; the codeml application object defined above
                    3        # = max_retries; max no. of retries of the task
                ],
                # extra keyword parameters to `CodemlRetryPolicy`
                dict()
                )

        self.log.debug("Total number of control files: %d", total_n_ctl_files) # AK: DEBUG

# run it
if __name__ == '__main__':
    GCodemlScript().run()
