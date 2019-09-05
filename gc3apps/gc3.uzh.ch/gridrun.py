#! /usr/bin/env python
#
"""
Front-end script for making a session out of a generic command-line.

This is only provided as a quick prototyping tool, and should not be
used for any real and production-level purposes: use the GC3Libs API
and develop a custom tool instead!
"""
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

from __future__ import absolute_import, print_function

__author__ = 'Riccardo Murri <riccardo.murri@gmail.com>'
# summary of user-visible changes
__changelog__ = """
  2012-02-22:
    * Initial draft version.
"""
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gridrun
    gridrun.GridRunScript().run()


## stdlib imports
import itertools
import os
import os.path
import string
import sys

## GC3Libs imports
import gc3libs
from gc3libs import Application, Run, Task
import gc3libs.exceptions
from gc3libs.cmdline import SessionBasedScript, executable_file, nonnegative_int
import gc3libs.utils
from gc3libs.workflow import RetryableTask


## helper class

class SmartApplication(Application):
    """
    Just like the regular `Application` class, except the `arguments`
    list is scanned for strings that name actual existing files or
    directories, which are automatically added to the `inputs` list.
    """
    def __init__(self, arguments, inputs=None, *more_args, **extra_args):
        # convert to string here as we want to compare args to file names
        arguments = [ str(x) for x in arguments ]

        # create `inputs` as would be done in the `Application` class ctor
        if inputs is not None:
            inputs = Application._io_spec_to_dict(
                gc3libs.url.UrlKeyDict, inputs, force_abs=True)
        else:
            inputs = gc3libs.url.UrlKeyDict()

        # scan command-line for things that look like actual files
        executable = arguments[0]
        if os.path.exists(executable):
            executable_name = os.path.basename(executable)
            inputs[executable] = executable_name
            arguments[0] = './' + executable_name
        for i, arg in enumerate(arguments[1:], 1):
            if arg not in inputs and os.path.exists(arg):
                inputs[arg] = os.path.basename(arg)
                arguments[i] = os.path.basename(arg)

        # recurse into superclass ctor
        Application.__init__(self, arguments, inputs, *more_args, **extra_args)


## the script itself

class GridRunScript(SessionBasedScript):
    """
Manage a task session created by parameter substitution into a
generic command-line.

The CMD and ARGs words given in the invocation of this command are
concatenated to form a command-line; in the process, every word that
matches a substitution parameter (defined with the '-D' option, see
below) is given an actual value.  The number of resulting actual
 command lines is the Cartesian product of the sets of all possible
values of substitution parameters.  The whole set of actual
command-lines creates a session; every command in the session is
submitted and managed until successful execution.

Note: substitution parameter names must be UPPERCASE.

If CMD is a path to a local file, then that file will be uploaded
to the remote system as the command to be executed.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            )


    def setup_args(self):
        self.add_param('cmd', metavar='CMD', type=executable_file,
                       help="Path to the command to execute.")
        self.add_param('args', nargs='*', metavar='ARG',
                       help="Arguments for the command to run.")


    def setup_options(self):
        self.add_param("-D", "--define",
                       action="append", dest="define", default=[],
                       help="Define parameters to substitute on the command-line."
                       " Parameter substitution always has the form 'NAME=SPEC',"
                       " where the SPEC part has one of the following three forms."
                       " (1) The string 'from:' followed by file name:"
                       " substitution data will be read from the specified file,"
                       " one value per line."
                       " (2) The string 'list:' followed by a comma-separated list"
                       " of arbitrary values."
                       " (3) The string 'range:' followed by a range specification"
                       " of the form 'LOW:HIGH:STEP': LOW and HIGH are the minimum"
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
        if self.params.retry is not None:
            self.stats_only_for = RetryableTask

        self.subst = { }
        for define in self.params.define:
            try:
                name, spec = define.split('=', 1)
                kind, value = spec.split(':', 1)
            except ValueError:
                raise gc3libs.exceptions.InvalidUsage(
                    "Invalid argument '%s' after -D/--define option."
                    " Parameter substitutions must have the form 'NAME=KIND:VALUE'."
                    % (define,))
            kind = kind.lower()
            # (1) file name
            if 'from' == kind:
                gc3libs.utils.check_file_access(value, os.R_OK,
                                        exception=gc3libs.exceptions.InvalidUsage)
                self.subst[name] = [i.strip() for i in open(value, 'r')]
                gc3libs.log.info(
                    "Reading values for %s from file '%s'", name, value)
            # (2) list of values
            elif 'list' == kind:
                self.subst[name] = value.split(',')
                gc3libs.log.info(
                    "Parameter %s takes values: %s", name, self.subst[name])
            # (3) range
            elif 'range' == kind:
                if value.count(':') == 2:
                    low, high, step = value.split(':')
                elif value.count(':') == 1:
                    low, high = value.split(':')
                    step = '1' # parsed to int or float later on
                else:
                    raise gc3libs.exceptions.InvalidUsage(
                        "Invalid argument '%s' after -D/--define option."
                        " Parameter range substitutions must have the form"
                        " 'NAME=range:LOW:HIGH:STEP'."
                        % (define,))
                # are low, high, step to floats or ints?
                if ('.' in low) or ('.' in high) or ('.' in step):
                    low = float(low)
                    high = float(high)
                    step = float(step)
                else:
                    low = int(low)
                    high = int(high)
                    step = int(step)
                self.subst[name] = gc3libs.utils.irange(low, high, step)
                gc3libs.log.info(
                    "Parameter %s ranges from %s (incl.) to %s (excl.) in increments of %s",
                    name, low, high, step)


    def new_tasks(self, extra):

        # use the name of the executable as job name
        basename = os.path.basename(self.params.cmd)

        # fix an ordering of the subst parameters, independent of any runtime variable
        names = sorted(self.subst.iterkeys())

        inputs = { }

        # decide whether CMD indicates a local file or a command
        # to be searched on the remote systems' PATH
        if os.path.exists(self.params.cmd):
            self.log.info("Uploading local file '%s' as executable.", self.params.cmd)
            gc3libs.utils.check_file_access(self.params.cmd, os.R_OK|os.X_OK)
            executable = './' + os.path.basename(self.params.cmd)
            inputs[os.path.abspath(self.params.cmd)] = os.path.basename(self.params.cmd)
        else:
            if not os.path.isabs(self.params.cmd):
                raise RuntimeError(
                    "You cannot execute a command by calling a relative path,"
                    " because the remote execution directory is empty"
                    " except for files we upload there; but there is"
                    " no file named '%s' here, so I don't know what to upload.",
                    self.params.cmd)
            executable = self.params.cmd

        # create a set of input files for each combination of the
        # substitution parameters
        for values in itertools.product(* (self.subst[name] for name in names)):
            subst = dict((name, value) for name,value in zip(names, values))
            jobname = str.join('_', [basename] + [
                ("%s=%s" % (name, value.translate(None,
                                                  r'\/&|=%$#!?<>()`"' + r"'" + '\a\b\n\r')))
                for name,value in subst.iteritems() ])

            # construct argument list, substituting defined parameters
            arguments = [ executable ]
            for arg in self.params.args:
                if arg in subst:
                    arguments.append(subst[arg])
                else:
                    arguments.append(arg)

            extra_args = extra.copy()
            extra_args['outputs'] = gc3libs.ANY_OUTPUT
            extra_args['output_dir'] = self.make_directory_path(self.params.output, jobname)
            extra_args['stdout'] = jobname + '.stdout.txt'
            extra_args['stderr'] = jobname + '.stderr.txt'
            if self.params.retry is not None:
                yield RetryableTask(
                    SmartApplication(arguments, inputs, **extra_args),
                    self.params.retry,
                    jobname=jobname,
                    **extra_args)
            else:
                yield SmartApplication(arguments, inputs,
                                       jobname=jobname, **extra_args)
