import re	
import os.path
	
#This class will be used to storne the patterns logic. Each pattern is assigned to a file. Each file can have 
# multiple patterns. 
#TODO: Each pattern can be assigned a label such as energy, gradient, etc. (do we need this?)
class GamessPattern:
	
	def __init__(self, name):
		self.name = name #file name, such as exam01.log

		self.LPattern = [] #patterns 
		self.LMatchedLine = [] #head | tail - select first or last line from the lines that match 
		self.LFollowingLine = [] #head | tail - select first or last line in a file AFTER matched lines
		self.LPositionInLine = [] #extract numerical value from the line at given position
		self.LValues = [] #target Values 
		self.LTolerances = [] #tolerances
		self.LLabel = [] #Label, such as energy, gradient, etc. 
		self.DEBUG = False
		
	def debug(self):
		self.DEBUG = True
		
#positionInLine: selected number within the line that represents a field to be extracted   
	
#The function prepares data structures for the followint task:
# selects a group of lines that match regexp 
# within these lines selects the target line that contains the tested value.
# pattern - a regexp that extracts lines of interest
# matchedLinePosition = tail | head | "" | "2" | 2, information about which matched line to select. 
# positionInLine =  "4" | 4, information about which column WITHIN the line to select. 
# value - value to be compared with
# tol - tolerance between value and value extracted from the log file

	def grepLinesAndAnalyze(self, pattern, matchedLinePosition, positionInLine, value, tol, name):
		self.LPattern.append(pattern)  
		self.LMatchedLine.append(matchedLinePosition)
		self.LFollowingLine.append("") #Skip this field
		self.LPositionInLine.append(positionInLine)
		self.LValues.append(value)
		self.LTolerances.append(tol)
		self.LLabel.append(name)
		

#The function prepares data structures for the followint task:
# selects a group of lines that match regexp 
# within lines FOLLOWED by the matched ones selects the target line that contains the tested value.
# pattern - a regexp that extracts lines of interest
# matchedLinePosition = tail | head | "" | "2" | 2, information about which matched line to select. 
# followingLinePosition = tail | head | "" | "2" | 2, information about which line to select from the file. 
# positionInLine =  "4" | 4, information about which column WITHIN the line to select. 
# value - value to be compared with
# tol - tolerance between value and value extracted from the log file		
	def grepAndFollow(self, pattern, matchedLinePosition, followingLinePosition, positionInLine, value, tol, name):
		self.LPattern.append(pattern)  
		self.LMatchedLine.append(matchedLinePosition) 
		self.LFollowingLine.append(followingLinePosition)
		self.LPositionInLine.append(positionInLine)
		self.LValues.append(value)
		self.LTolerances.append(tol)
		self.LLabel.append(name)
		
#Compare valLog and valOK against the tolerance.	
	def chkabs(self,valLog, valOK, tol, label):
		val = abs(valOK - valLog)
		ret = " " + label + '=%.2E ' %(val)
		if (val > tol):
			#print "Test not passed. |valOK - valLog| = |",valOK," - ", valLog, "| = " ,val , " > tolerance: = ", tol 
			
			return (False,ret)
		else:
			if (self.DEBUG):
				print "... passed"
			return (True,ret)
	
# Analyze the argument in LPositionInLine. Return the index of the column that correspond the a target value. 
	def checkPositionInLine(self, posInLine):
		try:
			selNo = int(posInLine)
			#print selNo, "selNo"
			selNo = selNo - 1 #for array indexing
		except ValueError:
				print "Could not convert to int, positionInLine must be integer"
				selNo = -1
		return selNo
	
# Analyze the argument in LMatched. Return the index of the line of interest. 
# head -> 1, 
# tail -> 99
# numerical value (also as a text) -> numerical value
	
	def checkMatchedLine(self,matchedLine):
		if (type(matchedLine) == type(4) ): # a strange way of checking the integer type
			which = matchedLine
			return which
		elif ("tail" in matchedLine.lower().strip()):
			which = 99 #encode the last position. Note that the number of matched lines is unknown at this stage
		elif ("head" in matchedLine.lower().strip()):
			which = 1
		elif (matchedLine == ""): #head is assumed
			which = 1
		else:
			#try to convert to int
			try:
				which = int(matchedLine)
				return which
			except ValueError:
				print "ERROR: ",matchedLine," is incorrect."
				return -1
			which = -1
		return which
			
#Search for pattern in the file. Return a list of lines together with the line numbers. 
# The line numbers are used later by grepAndFollow()
	
	def grepFile(self, filename, pattern, whichFollowing = 0):
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
				matchedLineNumbers.append(num)
				matchedLinesTemp.append(line)
		return (matchedLineNumbers,matchedLinesTemp)
		 
	#Extract num+whichFollowing line number from the file. 
	def grepNext(self, filename, num, whichFollowing):	
		FILE = open(filename, 'r')
		lines = FILE.readlines()
		FILE.close()
		lenLines = len(lines) 		 
		if (whichFollowing > 0 ): 
 			sum = whichFollowing+num
			if (sum < lenLines-1):
					targetLine = lines[num+whichFollowing]
			else:
				if (self.DEBUG):
					print "Out of boundary. Skipping."
					print "Length of file is:",lenLines, " and the extracted line has a number ", whichFollowing
				return ""						
		return targetLine	

		
#Extract a SINGLE line from a map of detected lines with its line number.
	def getTargetLine(self, lineNumbersList, matchedLinesList, whichLine):
		lenT = len(matchedLinesList)
		numberCorrect = 0
		lineCorrect = ""
		if (whichLine <= 0 ):
			return (numberCorrect, lineCorrect)
			
		elif (whichLine==99):
			finalWich = lenT-1  
			lineCorrect = matchedLinesList[finalWich]	
			#print "LINE CORRECT", lineCorrect
			numberCorrect = lineNumbersList[finalWich]
		else: # whichLine > 0): #correct values
				finalWich = whichLine - 1 #starting from 0
				numberCorrect = lineNumbersList[finalWich]
				lineCorrect = matchedLinesList[finalWich].strip()
		return (numberCorrect, lineCorrect)
	
	
	def getValueFromLine(self, line, position):
		list = line.split()
		if (position > len(list)):
			print "Out of boundary, check the position in the target line."
			return ""
		try:
			value = float(list[position])
		except ValueError:
			print "Could not convert to float, list(selNo) = ",list[position]," must be float"
			return ""
		return value
	
#Run each test. To optimize the execution each file is analyzed only ONCE. Internal lists drive the execution.
# 
#Internal parameters: 
# whichFolowing
# finalFlag is True only when all the tests return True
# finalString contains the test labels with results (Eerr=0.0E)
	def run(self):
		filename = self.name
		finalStr = ""
		finalFlag = True
		testNo = 0
		if (self.DEBUG):
			print self.name, "ENTERING DEBUG MODE.", 
			print "Parameters:"
			print "LPattern", self.LPattern
			print "LMatchedLine", self.LMatchedLine
			print "LFollowingLine", self.LFollowingLine 
			print "LPositionInLine", self.LPositionInLine 
			print "LValues", self.LValues 
			print "LTolerances", self.LTolerances 
			print "\n"
		if (len(self.LPattern) == 0):
			print "Empty test. Skipping."
			return False
		
		for pattern, whichLine, followingLine, positionInLine, value, tolerance, label in zip(self.LPattern, self.LMatchedLine, self.LFollowingLine, self.LPositionInLine, self.LValues, self.LTolerances, self.LLabel):
			testNo = testNo + 1
			#Encode the index of the line of interest
			which = self.checkMatchedLine(whichLine)
			whichFollowing  = self.checkMatchedLine(followingLine)
			
			if (self.DEBUG):
				print "Internal Test No.", testNo
				print "CHECKED MatchedLine", which
				print "CHECKED followingLine", whichFollowing
			#Extract the position of the value within the target line	
			pos = self.checkPositionInLine(positionInLine)
			if (self.DEBUG):
				print "CHECKED positionInLine", pos
			if (whichFollowing < 0):
				print "ERROR: whichFollowing ",whichFollowing, " is incorrect."
				return False
			
			#Extract a list of lines that match a regexp and line indices as lists. 
			(numList,linesFound) = self.grepFile(filename, pattern) 
			
			# Nothing Found
			if (len(linesFound) == 0):
				if (self.DEBUG):
					print "Nothing found with your regexp."
				finalFlag = finalFlag and False
				continue
			
			(numLine, targetLineT) = self.getTargetLine(numList, linesFound, which)
			
			if (followingLine == ""):
				targetLine = targetLineT
			#OR extract the line from the file FOLLOWING the matched position numLine
			else:
				targetLine = self.grepNext(filename, numLine, whichFollowing)
			
			if (self.DEBUG):
				print "MATCHED LINE: ", targetLine
			
	
			#Extract the value from matched line
			valL = self.getValueFromLine(targetLine, pos)
			if (valL == ""):
				print "ERROR: Cannot retrieve the float value from line", targetLine
				return False
			else:
				if (self.DEBUG):
					print "comparing", valL, "against", value;
					
				(flag, str) = self.chkabs(valL, value, tolerance, label)
				
			 	finalStr = finalStr + str
				finalFlag = finalFlag and flag
			
		return (finalFlag,finalStr)
	
	
#Scenarios
#grep a single line and seek for a value withing the line -> grep + awk
#grepAndAnalyze("Text", Nth matchedLine = head (default), position, ...)
#grep a set of lines, select first, last within matches, and analyze the line -> grep + sed
#grepAndAnalyze("Text", position, Nth matchedLine = tail)
#grep a set of lines, select the next one in file and analyze it -> grep + tail + cut -d : -f 1 (select 1 element from a line splitted by ':') + 
#grepAndFollow("Text", position, Nth following line = head)

#grep a set of lines, select LAST/ANY line in the file and analyze it #exam27
#grepAndFollow("Text", position, Nth following line = tail, number?)

#Params:
#- a path to a directory with test files, "path_to_dir"
#- a path to a report file (optional, if missing stdout is used)
#- a path to pre-calculated results as log files or files with a desired pattern

#./testgms path_to_dir path_to_results ../report.log


# The aim of the project is to implement a tool for automated tests in GAMESS (testgms).
# The tool takes a directory with .inp files being tested and generates an output file with a report.
# Tests passed will be indicated as OK, otherwise the differences will be reported.
# 
# Example:
# ./testgms path_to_dir ../report.log path_to_results
# 
# Params:
# - a path to a directory with test files, "path_to_dir"
# - a path to a report file (optional, if missing stdout is used)
# - a path to pre-calculated results as log files or files with a desired pattern
# 
# The report is generated on the basis of:
# 1) existing results that are 100% correct (a gold standard). In this scenario each input file is paired to a complementary log file (exam01.inp is paired with exam01.log, examNUM.inp is paired with examNUM.log) and differences are reported.
# 2) log files with predefined patterns. The user prepares such a file by defining a pattern that must EXACTLY the same. In this scenario the algorithm tries to match a given output with a pattern. Differences are reported.
# 
	
	
#	def grep(self,pattern,fileObj):
#		r=[]
#  		for line in fileObj:
#			if re.search(pattern,line):
#				r.append(line)
#		return r