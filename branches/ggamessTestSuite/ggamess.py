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
__version__ = 'development version (SVN $Revision$)'
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
#import GamessTestApplication

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
	self.LFailedTestDesc = []  
	self.LFailedTestNames = []  
	self.NumberOfCorrectTests = 0
	self.log = []

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
                # Added Gamess Test Application
		cls = GamessTestApplication
		log = cls.logTest 
                #cls = GamessApplication
            # construct GAMESS job
            #print "CLS %s", cls
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
# If any of the jobs terminated test is launched.    
    def after_main_loop(self):
	if self.params.test is False:
		gc3libs.log.debug("ggamess.py: Tests were not executed")
		print "NOT RUNNING TESTs"
		return 
	#if len(self.listOfTests) == 0:
	#	gc3libs.log.debug("The test list is empty.")
	#	raise RuntimeError("The list with tests is empty. The tests will not be run.")
	
	#print "PARAMS", self.params
	# build job list
        #inputs = self._search_for_input_files(self.params.args)
	#print "OLD", inputs 
	myoutputs = []
	testSet = GamessTestSuite(".")	
	numberOfTests = len(testSet.listOfTests)
	print numberOfTests
	anyterminated = False
	#import pdb;pdb.set_trace()
	if not self.session:
		raise RuntimeError("The session is empty.")
		 
	for app in self.session:
		dir(app)
		print self.session.list_names()
		output_abs_path = os.path.join(app.output_dir, app.outputs[app.stdout].path)
		myoutputs.append(output_abs_path)
	 	for message in app.logTest:
	 		dir(message)
		#print output_abs_path	
	#if anyterminated is True:
	#	testSet.runAll()
	#else:
	#	gc3libs.log.debug("None submitted job has TERMINATED.")
		
# Given a list of input files and list of output dirs from the session, generate a list of possible paths to output files
    def get_output_files_to_analyze(self, myinputs, myoutput_dirs):
	list_of_files_to_analyze = []
	gc3libs.log.info("Checking the results of your test GAMESS calculations, the output files (exam??.out) will be taken from %s directory.", TestSet.dirLog)
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
# This class overrides GamessApplication class and triggers a test in terminated().

#TODO: GamessTestApplcation class is only used when ggamess.py -N was provided 

class GamessTestApplication(GamessApplication):
	def __init__(self, logTest, inp_file_path, *other_input_files, **kw):
		print "LOG"
		self.logTest = logTest
		GamessApplication.__init__(self, 
					   inp_file_path, 
					   *other_input_files,
					   **kw
					  )
    #Methods to print the class name
	def __str__(self):
		return self.jobname
	
	def terminated(self):
		GamessApplication.terminated()
		self.test = GamessTestSuite(".")
		self.NumberOfTests += 1 
		#if self.execution.exitcode == 0:
		self.NumberOfCorrectTests = self.NumberOfCorrectTests + 1	
		file_input = self.inp_file_path
		#import pdb;pdb.set_trace()
		file_output = os.path.join(self.output_dir, self.outputs[self.stdout].path)
		gc3libs.log.info("TERMINATED IN: %s OUT %s", file_input, file_output)
		#self.test.add(file_input,file_input)
		test.generate_tests(file_input, file_output)
		test.runAll()
		self.log.append(test.log)
		#else:
			#self.LFailedTestNames.append(file_output)
		#	self.log.append("The file %s DID NOT terminated normally.", file_output)
			#gc3libs.log.debug("The file %s DID NOT terminated normally.", file_output)
			
# run it
if __name__ == '__main__':
    GGamessScript().run()
