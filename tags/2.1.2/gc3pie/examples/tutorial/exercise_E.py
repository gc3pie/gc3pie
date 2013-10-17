#! /usr/bin/env python
#
"""
Exercise E

* write a a two-stage sequence (which one??? propose a good example
  here)

* make the second stage conditional depending on the return code of
  the first one


"""
# Copyright (C) 2012, GC3, University of Zurich. All rights reserved.
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


import gc3libs
import gc3libs.cmdline
import gc3libs.workflow

class SimpleScript(gc3libs.cmdline.SessionBasedScript):
    """
    This script will run an application which produce a random
    number.

    If this number is even, it will run a new application which will
    write to a file the string ``previous application exited with an
    even exit code``, otherwise it will write ``previous application
    exited with an odd exit code``.
    """
    version = '0.1'
    
    def new_tasks(self, extra):
        yield (
            'TwoStageWorkflow',
            TwoStageWorkflow,
            [],
            extra)


if __name__ == "__main__":
    from exercise_E import SimpleScript
    SimpleScript().run()


class TwoStageWorkflow(gc3libs.workflow.StagedTaskCollection):
    """
    First, run an application which will exit with a random exit status.

    Then, depending on the exit code of the first application, decide
    which application to run next.
    
    """
    def stage0(self):
        return gc3libs.Application(
            arguments = ["bash", "-c", "exit $RANDOM"],
            inputs = [],
            outputs = [],
            output_dir = 'TwoStageWorkflow.stage0',
            stdout = 'stdout.txt',
            stderr = 'stderr.txt',
            )

    def stage1(self):
        # Check exit status of previous task
        if self.tasks[-1].execution.returncode % 2 == 0:
            return gc3libs.Application(
                arguments = [
                    "echo",
                    "previous application exited with an even exit code (%d)" % self.tasks[-1].execution.returncode],
            inputs = [],
            outputs = [],
            output_dir = 'TwoStageWorkflow.stage1',
            stdout = 'stdout.txt',
                )
        else:
            return gc3libs.Application(
                arguments = [
                    "echo",
                    "previous application exited with an odd exit code (%d)" % self.tasks[-1].execution.returncode],
                inputs = [],
                outputs = [],
                output_dir = 'TwoStageWorkflow.stage1',
                stdout = 'stdout.txt',
                )
