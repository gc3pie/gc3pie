#! /usr/bin/env python
#
"""
Repeatedly execute a given command.
"""
# Copyright (C) 2018 University of Zurich.
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
__author__ = 'Riccardo Murri <riccardo.murri@gmail.com>'
# summary of user-visible changes
__changelog__ = """
  2018-09-25:
    * Initial version.
"""
__docformat__ = 'reStructuredText'


# Workaround for Issue 95: import this module and run it first
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == '__main__':
from __future__ import absolute_import
    import repeat
    repeat.Script().run()


# stdlib imports
import os
import os.path
import sys

# interface to Gc3libs
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript
from gc3libs.workflow import SequentialTaskCollection
import gc3libs.utils


## the main class, implementing CLI functionality

class Script(SessionBasedScript):
    # the docstring of this class is printed with `--help`
    """
    Repeatedly execute a given command.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = '1.0',
        )

    def setup_options(self):
        """
        Add command-specific options
        to the general ones provided by `SessionBasedScript`.
        """
        self.add_param("-n", "--times", type=int,
                       help="Number of iterations.")

    def setup_args(self):
        """
        Set up command-line argument parsing.

        Require that a COMMAND parameter is supplied, plus
        (optionally) additional ARGs.  They will be concatenated to
        form the actual command-line to run.
        """
        self.add_param('command', help="Command to run")
        self.add_param('args', nargs='*', metavar='ARG',
                       help="Additional arguments for COMMAND")

    def new_tasks(self, extra):
        """
        Return list of tasks to be executed in a newly-created session.
        """
        return [
            RepeatingCommand(
                self.params.times,
                [self.params.command] + self.params.args,
                **extra.copy()
            ),
        ]



## support classes

# For each input file, a new computation is started, totally
# `self.params.iterations` passes, each pass corresponding to
# an application of the `self.params.executable` function.
#
# This is the crucial point:

class RepeatingCommand(SequentialTaskCollection):
    """
    Perform a (predefined) number of runs of a given command.
    """

    def __init__(self, repeats, cmdline, **extra_args):
        """
        Initialize the repeated runs sequence.

        :param int repeats:
          Number of repetitions; if 0, repeat indefinitely.

        :param List[str] cmdline:
          Command-line to run. Can be a list of (string) arguments, or
          a single string to be passed to the shell to be interpreted
          as a command.
        """

        self.repeats = repeats
        self.cmdline = cmdline

        # create initial task and register it
        initial_task = _CommandLineApp(0, self.cmdline)
        SequentialTaskCollection.__init__(self, [initial_task], **extra_args)


    def next(self, iteration):
        """
        If there are more iterations to go, enqueue the corresponding jobs.

        See: `SequentialTaskCollection.next`:meth: for a description
        of the contract that this method must implement.
        """
        if self.repeats > 0 and iteration > self.repeats:
            last_application = self.tasks[iteration]
            self.execution.returncode = last_application.execution.returncode
            return Run.State.TERMINATED
        else:
            self.add(_CommandLineApp(iteration+1, self.cmdline))
            return Run.State.RUNNING


class _CommandLineApp(Application):
    """
    Run a given command-line.

    This is an auxliary class, to avoid repeating the same
    `Application()` invocation in several places in the code of
    `RepeatingCommand`.

    The class has to be at top-level (as opposed to being defined
    within `RepeatingCommand`) because otherwise Python's `pickle`
    (which is used for persisting GC3Pie tasks) cannot find the class
    source.
    """

    def __init__(self, iteration, exec_args, **extra_args):
        super(CommandLineApp, self).__init__(
            arguments = exec_args,
            inputs = [],  # no input file staged in
            outputs = [],  # no output files collected
            output_dir = os.path.join(
                os.getcwd(), "repeat_run_{0}.d".format(iteration)),
            stdout = "stdout.txt",
            stderr = "stderr.txt",
            **extra_args
        )
