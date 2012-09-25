#! /usr/bin/env python
#   testScenarios.py -- Front-end script for implementation of syntax used in Test GAMESS Suite.
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
#   GNU General Public License for more delasts.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
"""
TestScenarios.py class extracts test execution within GAMESS. 
Tests are executed if INP files include their definition in an appropriate syntax (for more information about this syntax see testScenarios.py).

"""
__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
  2012-09-05: * Initial release of GAMESS Test Suite 
  2012-09-06: * Created a class template for test implementation (testScenario.py) and derived TestNextLine and TestLine classes as sample implementations 
  2012-09-25: * First version of Gamess Test Suite. 
"""
__author__ = 'Lukasz Miroslaw <lukasz.miroslaw@uzh.ch>'
__docformat__ = 'reStructuredText'

import re	
import os.path
import gc3libs
#This class will be used to store the patterns logic. 
# Each pattern is assigned to a file. Each file can have multiple patterns. 

class Test:

# Description of parameters:
# positionInLine: selected number within the line that represents a field to be extracted   
# 
# The function prepares data structures for the followint task:
# selects a group of lines that match regexp 
# within these lines selects the target line that contains the tested value.
# pattern - a regexp that extracts lines of interest
# matchedLinePosition = last | first | "" | "2" | 2, information about which matched line to select. 
# positionInLine =  "4" | 4, information about which column WITHIN the line to select. 
# value - value to be compared with
# tol - tolerance between value and value extracted from the log file

	def __init__(self, name):
		self.name = name #file name, such as exam01.log
		self.DEBUG = False
	    #Predefined tolerances

		self.tolC =  0.3
		self.tolD =  0.0001
		self.tolE =  0.00000001
		self.tolG =  0.00001
		self.tolH =  0.0001
		self.tolI =  0.0001
		self.tolL =  0.1
		self.tolO =  0.0001
		self.tolP =  0.0001
		self.tolR =  0.0001
		self.tolS =  0.01
		self.tolT =  0.000001
		self.tolV =  0.00000001
		self.tolW =  0.1
		self.tolX =  0.00001
		self.tolerances = {'tolC': self.tolC, 'tolD': self.tolD, 'tolE': self.tolE,'tolG': self.tolG, 'tolH': self.tolH,'tolI':  self.tolI, 'tolL': self.tolL, 'tolO': self.tolO, 'tolP': self.tolP, 'tolR': self.tolR , 'tolS': self.tolS , 'tolT': self.tolT , 'tolV': self.tolV , 'tolW': self.tolW , 'tolX': self.tolX}
# E energy in a.u. :  
# W vibrational frequency (cm-1)
# I IR intensity
# P Mean Alpha Polarization ??
# H heat of formation in kcal/mol 
# G RMS gradient in a.u.
# L localisation sum (debye**2)
# T T1 diagnostic from CC

# D RMS dipole moment  
# X basis set exponent  
# S spin momentum (a.u.)
# O overlap (such as GVB)

# O polarisability
# V velocity (a.u.), such as in DRC
# R distance (in bohr), such as in IRC 
# tols are tolerances, digs are the numbers of sig. digits.
# C percent (%), such as reference weight in MCQDPT

#Compare valLog and valOK against the tolerance.	
	def chkabs(self,valLog, valOK, tol, label):
		val = abs(valOK - valLog)
		deviation = val - tol
		if (val <= tol):
			return (True,0)
		else:
			return (False,deviation)

#Check a given parameter for consistency. Return a correct value.	
	def check_tolerance(self, tolerance):
		# self.tolerances contains entries of a type dict('tolC',self.tolC)
	 	try:
		    return float(tolerance)
		except ValueError:
		    for tol_key in self.tolerances.keys():
		        if tolerance.find(tol_key) > 0:
		            return float(self.tolerances[tol_key])
	
#Check a given parameter for consistency. Return a correct value.	
	def check_position(self, posInLine):
		try:
			selNo = int(posInLine)
			selNo = selNo - 1 #for array indexing
		except ValueError:
			gc3libs.log.debug("Could not convert %s to int, positionInLine must be integer", posInLine)
			selNo = -1
		return selNo
	
# Check a parameter matchedLine for consistency. Return the index of the line of interest as follows: first -> 1, last -> 99, numerical value (also as a string) -> numerical value
	
	def check_matchedLine(self,matchedLine):
		if (type(matchedLine) == type(4) ): # a strange way of checking the integer type
			which = matchedLine
			return which
		elif ("last" in matchedLine.lower().strip()):
			which = 99 #encode the last position. Note that the number of matched lines is unknown at this stage
		elif ("first" in matchedLine.lower().strip()):
			which = 1
		elif (matchedLine == ""): #first is assumed
			which = 1
		else:
			#convert to int
			try:
				return int(matchedLine)
			except ValueError:
				gc3libs.log.debug("ERROR: %s is incorrect. ",matchedLine)
				return 0 
			which = 0 
		return which
			
#Search for pattern in the file. Return a list of lines together with the line numbers. 
	def grep_file(self, filename, pattern, whichFollowing = 0):
		regexp = re.compile(pattern, re.VERBOSE) #  
		FILE = open(filename, 'r')
		lines = FILE.readlines()
		FILE.close()
		#Store line numbers that match regexp
		matchedLineNumbers = []
		matchedLinesTemp = []
		lenLines = len(lines)

		#Enumerate the lines
		for num, line in enumerate(lines):
			match = regexp.search(line.strip())
			if match:
				if line.find("ggamess test") <= 0:
					matchedLineNumbers.append(num)
					matchedLinesTemp.append(line)
		return (matchedLineNumbers,matchedLinesTemp)
		 
 
#Extract a SINGLE line from a map of detected lines with its line number.
	def get_targetLine(self, lineNumbersList, matchedLinesList, whichLine):
		lenT = len(matchedLinesList)
		numberCorrect = 0
		lineCorrect = ""
		if (whichLine <= 0 ):
			return (numberCorrect, lineCorrect)			
		elif (whichLine==99):
			finalWich = lenT-1  
			lineCorrect = matchedLinesList[finalWich]	
			numberCorrect = lineNumbersList[finalWich]
		else: # whichLine > 0): #correct values
				finalWich = whichLine - 1 #starting from 0
				numberCorrect = lineNumbersList[finalWich]
				lineCorrect = matchedLinesList[finalWich].strip()
		return (numberCorrect, lineCorrect)
	
#Extract a target value from a given line 
	def get_valueFromLine(self, line, position):
		list = line.split()
		if (position > len(list)):
			gc3libs.log.debug("Out of boundary, check the position in the target line.")
			return ""
		try: 
			value = float(list[position])
		except ValueError:                                                                                       
			gc3libs.log.debug("Could not convert to float, list(selNo) = %s must be float",list[position])
			return ""                		
		return value


#Run each test. To optimize the execution each file is analyzed only ONCE. Internal lists drive the execution.
#Internal parameters: 
# final_flag is True only when all the tests return True
	def run(self):
		filename = self.name
		final_report = []
		final_flag = True
		gc3libs.log.debug("Test: %s", self.name)
		gc3libs.log.debug("LPattern %s", self.LPattern)
		gc3libs.log.debug("LMatchedLine %s", self.LMatchedLine)
		gc3libs.log.debug("LFollowingLine %s", self.LFollowingLine) 
		gc3libs.log.debug("LPositionInLine %s", self.LPositionInLine) 
		gc3libs.log.debug("LValues %s", self.LValues)
		gc3libs.log.debug("LTolerances %s", self.LTolerances) 
		if (len(self.LPattern) == 0):
			gc3libs.log.debug("Empty test. Skipping.")
			final_report.append("Empty test. Skipping.")
			return (False, '')
	 	pattern = self.LPattern
		whichLine = self.LMatchedLine
		followingLine = self.LFollowingLine
		positionInLine = self.LPositionInLine
		value = self.LValues
		tolerance = self.LTolerances
		label = self.LLabel	
		tolerance = self.check_tolerance(tolerance)	
		gc3libs.log.debug("CHECKED tolerance %s", tolerance)
		
		#Extract the position of the value within the target line	
		pos = self.check_position(positionInLine)
		gc3libs.log.debug("CHECKED positionInLine %s", pos)
		which = self.check_matchedLine(whichLine)
		gc3libs.log.debug("CHECKED MatchedLine %s", which)
		if which == 0:
			gc3libs.log.debug("ERROR: which %s is incorrect.") 
			final_report.append("ERROR: which parameter is incorrect.") 	
			return (False, final_report)
			
		#Extract a list of lines that match a regexp and line indices as lists. 
		(numList,linesFound) = self.grep_file(filename, pattern) 
		
		# Nothing Found
		if (len(linesFound) == 0):
			gc3libs.log.debug("Nothing found with your regexp.")
			final_report.append("Nothing found with your regular expression: " + pattern) 	
			final_flag = final_flag and False
			return (False, final_report)
	
		(numLine, targetLine) = self.get_targetLine(numList, linesFound, which)
	
		#case grepAndFollow                                                                               	
		if followingLine is not None:
			whichFollowing  = self.check_matchedLine(followingLine)
			gc3libs.log.debug("CHECKED followingLine %s", whichFollowing)
			if whichFollowing is None:
				gc3libs.log.debug("ERROR: whichFollowing parameter is incorrect.",whichFollowing) 
				log = "ERROR: whichFollowing parameter is incorrect."
				final_report.append(log)
        			return (False, final_report)
			targetLine = self.grep_next(filename, numLine, whichFollowing)
			gc3libs.log.debug("MATCHED LINE: %s", targetLine)
		else:		
			(numLine, targetLine) = self.get_targetLine(numList, linesFound, which)
			gc3libs.log.debug("MATCHED LINE: %s", targetLine)
	
		#Extract the value from matched line
		valL = self.get_valueFromLine(targetLine, pos)
		
		if valL is None: #Empty:
			gc3libs.log.debug("ERROR: Cannot retrieve the float value from line %s.", targetLine)
			final_report.append("ERROR: Cannot retrieve the float value from line " + targetLine)
			return (False, final_report)
		else:
			gc3libs.log.debug("Comparing %s against %s.", valL, value)
			(flag, deviation) = self.chkabs(valL, value, tolerance, label)
			if deviation > 0:
				deviation = label + str(deviation)
				final_report.append("ERROR: wnen compared " + str(valL)+" against " + str(value) + " deviation found: " + deviation)
			final_flag = final_flag and flag
		return (final_flag,final_report)
"""
The user edits GAMESS input files by introducing two types of testing scenario encoded in
a special syntax. These two classes define the test and implement a method that prepares the class object to be executed. 
There are two testing scenario that have been provided:

1. Extraction of a specific value from a given target line (see TestLine class and
grepAndAnalyze method).

2. Extraction of a specific value from the line AFTER a target line (see TestNextLine
and grepAndFollow method).

Target lines that contain the value of interest is extracted with a regular expression (regex statement).
If regex extracts many lines the user has a chance to specify which one to select (option head or tail). Once the line has been selected it is divided into separable strings a string that represent the value of interest is selected with positionInLine parameter.

"""
class TestNextLine(Test):

	def grepAndFollow(self, pattern, matchedLinePosition, followingLinePosition, positionInLine, value, tol, name):
		pattern = re.sub(r"[ \"]", r"", pattern) 

		self.LPattern = pattern                    	
		self.LMatchedLine = matchedLinePosition 
		self.LFollowingLine = followingLinePosition
		self.LPositionInLine = positionInLine
		self.LValues = value
		self.LTolerances  = tol
		self.LLabel = name

	#Extract num+whichFollowing line number from the file. 
	def grep_next(self, filename, num, whichFollowing):	
		FILE = open(filename, 'r')
		lines = FILE.readlines()
		FILE.close()
		lenLines = len(lines)
		if (whichFollowing > 0 ): 
			sum = whichFollowing+num
			if (sum < lenLines-1):
				targetLine = lines[num+whichFollowing]
			else:
				gc3libs.log.debug("grepNext: Out of boundary. ")
				return None						
		return targetLine	


class TestLine(Test):	
	def grepLinesAndAnalyze(self, pattern, matchedLinePosition, positionInLine, value, tol, name):
		pattern = re.sub(r"[ \"]", r"", pattern) 

		self.LPattern = pattern                    	
		self.LMatchedLine = matchedLinePosition 
		self.LFollowingLine = None 
		self.LPositionInLine = positionInLine
		self.LValues = value
		self.LTolerances  = tol
		self.LLabel = name

	
