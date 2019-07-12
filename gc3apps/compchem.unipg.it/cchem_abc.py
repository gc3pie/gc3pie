#! /usr/bin/env python
#
# Copyright (C) 2011-2012  University of Zurich. All rights reserved.
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
"""
"""

from __future__ import absolute_import, print_function

__author__ = 'Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>, Riccardo Murri <riccardo.murri@gmail.com>'
# summary of user-visible changes
__changelog__ = """
  2012-02-06:
    * Support parameter studies from the command-line.
  2011-06-27:
    * Defined ABCApplication and basic SessionBasedScript
"""
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == '__main__':
    import cchem_abc
    cchem_abc.ABCWorkflow().run()


# stdlib imports
import itertools
import os
import os.path
import string
import sys


## interface to Gc3libs
import gc3libs
from gc3libs import Application, Run, Task, RetryableTask
import gc3libs.exceptions
from gc3libs.cmdline import SessionBasedScript, executable_file, nonnegative_int
import gc3libs.utils
from gc3libs.workflow import RetryableTask



class ABCApplication(Application):
    application_name = 'abc'
    def __init__(self, abc_executable, *input_files, **extra_args):

        gc3libs.Application.__init__(
            self,
            executable = os.path.basename(abc_executable),
            arguments = [ ], # ABC should find files on its own(??)
            inputs = [abc_executable] + list(input_files),
            outputs = gc3libs.ANY_OUTPUT,
            join = True,
            stdout = 'abc.log',
            **extra_args
            )


class ABCWorkflow(SessionBasedScript):
    """
    Sample parameter-study script.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            )


    def setup_options(self):
        self.add_param("-D", "--parameter", "--substitute",
                       action="append", dest="subst", default=[],
                       help="Parameters to substitute in input files."
                       " Each parameter substitution has the form 'NAME=LOW:HIGH:STEP'"
                       " where NAME is the parameter name to be substituted"
                       " in the input files, LOW and HIGH are the minimum"
                       " and the maximum values, and STEP is the increment."
                       " The ':STEP' part can be omitted if STEP is 1.")
        self.add_param("-K", "--retry",
                       nargs='?',    # takes one optional argument
                       type=nonnegative_int, metavar='MAX',
                       const=3,      # returned if '-K' is there, but with no argument
                       default=None, # returned if '-K' is *not* there
                       action="store", dest="retry",
                       help="Retry failed jobs up to MAX times."
                       " If MAX is 0, then retry until the jobs succeeds."
                       " By default, failed jobs are *not* retried.")


    def parse_args(self):
        self.subst_names = [ ]
        self.subst_values = [ ]
        for subst in self.params.subst:
            try:
                name, spec = subst.split('=')
                if spec.count(':') == 2:
                    low, high, step = spec.split(':')
                elif spec.count(':') == 1:
                    low, high = spec.split(':')
                    step = '1' # parsed to int or float later on
                else:
                    raise ValueError()
                # are low, high, step to floats or ints?
                if ('.' in low) or ('.' in high) or ('.' in step):
                    low = float(low)
                    high = float(high)
                    step = float(step)
                else:
                    low = int(low)
                    high = int(high)
                    step = int(step)
            except ValueError:
                raise gc3libs.exceptions.InvalidUsage(
                    "Invalid argument '%s' after -D/--parameter option."
                    " Parameter substitutions must have the form 'NAME=LOW:HIGH:STEP',"
                    " where LOW, HIGH and STEP are (integer or floating-point) numbers."
                    % (subst,))
            self.subst_names.append(name)
            self.subst_values.append(gc3libs.utils.irange(low, high, step))
            gc3libs.log.info(
                "Parameter %s ranges from %s (incl.) to %s (excl.) in increments of %s",
                name, low, high, step)

        self.abc_executable = self.params.args[0]
        if not os.path.isabs(self.abc_executable):
            self.abc_executable = os.path.abspath(self.abc_executable)
        gc3libs.utils.check_file_access(self.abc_executable, os.R_OK|os.X_OK,
                                gc3libs.exceptions.InvalidUsage)

        # build list of input files (w/ absolute path names)
        self.input_files = [ ]
        for path in self.params.args[1:]:
            if not os.path.isabs(path):
                path = os.path.abspath(path)
            gc3libs.utils.check_file_access(path, os.R_OK)
            if os.path.isdir(path):
                raise gc3libs.exceptions.InvalidUsage(
                    "Argument '%s' should be a file, but is a directory instead."
                    % (path,))
            self.input_files.append(path)


    def new_tasks(self, extra):

        # use the name of the ABC executable as job name
        basename = "ABC_" + os.path.basename(self.abc_executable)
        if '.' in basename:
            r = basename.rindex('.')
            basename = basename[:r]

        # create a set of input files for each combination of the
        # substitution parameters
        for values in itertools.product(* self.subst_values):
            subst = dict((name, value)
                         for name,value in zip(self.subst_names, values))
            jobname = str.join('_', [basename] + [
                ("%s=%s" % (name, value)) for name,value in subst.iteritems() ])

            if os.path.isdir(jobname):
                # assume job has already been created
                gc3libs.log.info("Directory '%s' already exists;"
                                 " assuming job has already been created.", jobname)
            else:
                inputs = [ self.abc_executable ]
                # create copy of the input files, performing substitutions
                gc3libs.log.info("Creating input files for job '%s' ...", jobname)
                gc3libs.utils.mkdir(jobname)
                for path in self.input_files:
                    template_text = gc3libs.utils.read_contents(path)
                    text = string.Template(template_text).safe_substitute(subst)
                    abc_input_filename = os.path.join(jobname, os.path.basename(path))
                    w = open(abc_input_filename, 'w')
                    w.write(text)
                    w.close()
                    inputs.append(abc_input_filename)
                    gc3libs.log.debug("  ... written file '%s'", abc_input_filename)

                extra_args = extra.copy()
                extra_args['output_dir'] = os.path.join(
                    self.make_directory_path(self.params.output, jobname), 'output')
                if self.params.retry is not None:
                    yield (jobname, RetryableTask, [
                        jobname,
                        ABCApplication(*inputs, **extra_args),
                        self.params.retry,
                        ], extra_args)
                else:
                    yield (jobname, ABCApplication, inputs, extra_args)
