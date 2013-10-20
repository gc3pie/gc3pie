#! /usr/bin/env python
#
"""
Exercise F

* implement the RetryableTask class: retry a job until it
  succeeds. Test it with the ``dice.sh`` script that rolls a dice and
  exits successfully iff the dice reads `1`.

Note: instead of using a shell script, we use the bash command::

    exit $[$RANDOM%6+1]

which will exit with an exit code between 1 and 6.
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
            'Dice App',
            DiceApplication,
            [
                
                gc3libs.Application(
                    arguments = ["bash", "-c", "exit $[$RANDOM%6]"],
                    inputs = [],
                    outputs = [],
                    output_dir = 'DiceApp',
                    stderr='stderr.txt',
                    stdout='stdout.txt',
                    **extra
                    )
                ],
            extra)


if __name__ == "__main__":
    from exercise_F import SimpleScript
    SimpleScript().run()


class DiceApplication(gc3libs.workflow.RetryableTask):
    """
    First, run an application which will exit with a random exit status.

    Then, depending on the exit code of the first application, decide
    which application to run next.
    
    """

    def retry(self):
        print "Previous application returned: %s" % self.task.execution.returncode
        if self.task.execution.returncode == 1:
            return False
        else:
            return True
