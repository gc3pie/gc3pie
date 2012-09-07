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
__version__ = '2.0.0-a2 version (SVN $Revision$)'
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
  2012-16-23: 
    * Pre-release of beta version 
""" 
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'

import os
import sys
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
	self.add_param("-t", "--test", action="store_true", #metavar='TEST', 
		       dest="test", default=False,
		       help="Execute tests defined in the input files.")
	
    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            input_filename_pattern = '*.inp'
            )


    def new_tasks(self, extra):
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
# This method is called after the session has been completed and the results are generated. The method is triggered with option -t.
    
    def after_main_loop(self):
	print "SELF.", self.params.test
	if self.params.test is False:
		gc3libs.log.debug("ggamess.py: Tests were not executed")
		print "NOT RUNNING TESTs"
		return 

	#print "PARAMS", self.params
	# build job list
        inputs = self._search_for_input_files(self.params.args)
	#print "OLD", inputs 
	
	#inputs = [task. for task in self.session_uri.path]
	#input_list = []
	#for app in self.session:
	#	print "myI",app.outputs
	#	print "myO",app.inputs
	#	print "myOutDir", app.output_dir
	
	# transform set with input files to a list	
	input_list = [myinput for myinput  in inputs]
	#for myinput in inputs:
	#	input_list.append(myinput)
	#output_dirs = []
	#jobs = list(self.session)

	#list of output directories
	#for job in jobs:
	# 	output_dirs.append(job.output_dir) 
	output_dirs = [job.output_dir for job in self.session]
	#sort both list to make sure they correspond to the same files
	input_list.sort()
 	output_dirs.sort()
 	#print 'INPUTSsearch', input_list
 	#print 'OUTPUTs', output_dirs
	
	testSet = GamessTestSuite(".")	
	output_list = self.get_output_files_to_analyze(input_list, output_dirs)
	for file_input, file_output in zip(input_list,output_list):
		#print "I/O", file_input, file_output
		testSet.generate_tests(file_input, file_output)
	testSet.runAll()

# Given a list of input files and list of output dirs from the session, generate a list of possible paths to output files
    def get_output_files_to_analyze(self, myinputs, myoutput_dirs):
	list_of_files_to_analyze = []
	for fileNameInput, output_dir in zip(myinputs, myoutput_dirs):
		#print fileNameInput
		fileName = os.path.split(fileNameInput)
		if len(fileName) > 1:
			fileName = os.path.join(output_dir, fileName[1])
			#print "1stage", fileName
			fileName = os.path.splitext(fileName)
			fileNameOutput = fileName[0] + '.out'
			#print "2stage",fileNameOutput
			if os.path.exists(fileNameOutput):
				list_of_files_to_analyze.append(fileNameOutput)
			else:
				gc3libs.log.info("File %s does not exist", fileNameOutput)
				continue
		else:
			raise IOError("ggamess.py: Incorrect path of %s.", fileName)
	return list_of_files_to_analyze
	
# run it
if __name__ == '__main__':
    GGamessScript().run()
