#! /usr/bin/env python
#
#   ggamess.py -- Front-end script for submitting multiple GAMESS jobs to SMSCG.
#
#   Copyright (C) 2010-2012 GC3, University of Zurich
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
Front-end script for submitting multiple GAMESS jobs to SMSCG.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``ggamess --help`` for program usage instructions.
"""
__version__ = '2.0.0-a1 version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
  2011-11-08:
    * New command line option ``--extbasis`` for using an
      external basis definition with GAMESS.
  2011-10-11:
    * Allow running GAMESS from an AppPot container.
  2010-12-20:
    * Initial release, forked off the ``grosetta`` sources.
  2012-16-8: 
    * Modification of terminated().
    * Correct execution: ./ggamess.py -R 2011R1 test/data/exam01.inp -N 
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'

import os
import gc3libs
from testUtils import GamessTestSuite
from gc3libs.application.gamess import GamessApplication, GamessAppPotApplication
from gc3libs.cmdline import SessionBasedScript, existing_file

class GGamessScript(SessionBasedScript):
    """
Scan the specified INPUT directories recursively for '.inp' files,
and submit a GAMESS job for each input file found; job progress is
monitored and, when a job is done, its '.out' and '.dat' file are
retrieved back into the same directory where the '.inp' file is (this
can be overridden with the '-o' option).

The `ggamess` command keeps a record of jobs (submitted, executed and
pending) in a session file (set name with the '-s' option); at each
invocation of the command, the status of all recorded jobs is updated,
output from finished jobs is collected, and a summary table of all
known jobs is printed.  New jobs are added to the session if new input
files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; `ggamess` will delay submission
of newly-created jobs so that this limit is never exceeded.
    """

    def setup_options(self):
        self.add_param("-A", "--apppot", metavar="PATH",
                       dest="apppot",
                       type=existing_file, default=None,
                       help="Use an AppPot image to run GAMESS."
                       " PATH can point either to a complete AppPot system image"
                       " file, or to a `.changes` file generated with the"
                       " `apppot-snap` utility.")
        self.add_param("-R", "--verno", metavar="VERNO",
                       dest="verno", default=None,
                       help="Use the specified version of GAMESS"
                       " (second argument to the localgms/rungms script).")
        self.add_param("-e", "--extbas", metavar='FILE',
                       dest='extbas',
                       type=existing_file, default=None,
                       help="Make the specified external basis file available to jobs.")


    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            input_filename_pattern = '*.inp'
            )


    def new_tasks(self, extra):
        self.collectTests(extra)
	# setup AppPot parameters
        use_apppot = False
        apppot_img = None
        apppot_changes = None
        if self.params.apppot:
            use_apppot = True
            if self.params.apppot.endswith('.changes.tar.gz'):
                apppot_changes = self.params.apppot
            else:
                apppot_img = self.params.apppot
        # create tasks
        inputs = self._search_for_input_files(self.params.args)
        for path in inputs:
            parameters = [ path ]
            kwargs = extra.copy()
            kwargs['verno'] = self.params.verno
            if self.params.extbas is not None:
                kwargs['extbas'] = self.params.extbas
            if use_apppot:
                if apppot_img is not None:
                    kwargs['apppot_img'] = apppot_img
                if apppot_changes is not None:
                    kwargs['apppot_changes'] = apppot_changes
                cls = GamessAppPotApplication
            else:
                cls = GamessApplication
            # construct GAMESS job
            yield (
                # job name
                gc3libs.utils.basename_sans(path),
                # application class
                cls,
                # parameters to `cls` constructor, see `GamessApplication.__init__`
                parameters,
                # keyword arguments, see `GamessApplication.__init__`
                kwargs)

    def collectTests(self,extra):
	#kwargs = extra.copy()
	#print kwargs['output_dir']
	#output_dir = self.output_dir
	#print output_dir
	#files = os.listdir("./test/data")
        inputs = self._search_for_input_files(self.params.args)
 	print 'INPUTS', inputs
	#print self.outputs
	testSet = GamessTestSuite(".")	
	#testSet.scanGAMESSinputFile("./test/data/exam04.inp","test/exam04.out")
	for fileNameInput in inputs:
		fileName = os.path.split(fileNameInput)
		fileName = 'test/' + fileName[1]
		#print fileName
		fileName = os.path.splitext(fileName)
		fileNameOutput = fileName[0] + '.out'
		if os.path.exists(fileNameOutput):
			testSet.scanGAMESSinputFile(fileNameInput, fileNameOutput)
		else:
			gc3libs.log.info("File %s does not exist", fileNameOutput)
	testSet.runAll()
	

# TODO: Search for inp files in dir. Collect a list of tests. Execute them.
    def terminated(self):
	pass
# run it
if __name__ == '__main__':
    GGamessScript().run()
