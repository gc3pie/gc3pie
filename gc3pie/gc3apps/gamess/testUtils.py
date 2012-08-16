#! /usr/bin/env python
#import code

import os.path
import sys
import re
#import pprint
import gc3libs

#import gmsPattern
from testLine import TestLine
from testNextLine import TestNextLine

#This class is responsible for running the tests. Directory with log files.
class GamessTestSuite:
       
	#Predefined tolerances
	
	tolC =  0.3
  	tolD =  0.0001
  	tolE =  0.00000001
  	tolG =  0.00001
  	tolH =  0.0001
  	tolI =  0.0001
  	tolL =  0.1
  	tolO =  0.0001
  	tolP =  0.0001
  	tolR =  0.0001
  	tolS =  0.01
  	tolT =  0.000001
  	tolV =  0.00000001
  	tolW =  0.1
  	tolX =  0.00001    	

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


	def __init__(self, directory):
 		self.L = []
 		self.exQ = []
		self.listOfTests = []
		self.dirLog = directory
		os.chdir(self.dirLog)
	
	def _getInputFiles(self):
		#return os.path.list('*.inp')
		return [exam01.inp, exam39.inp]
		
	def prepareInputFiles(self):
		listOfFiles = _getInputFiles(self)
		for filename in listOfFiles:
			FILE = open(filename, 'w')
			lines = FILE.readlines()	
			posLine = self.grep("\$[A-Za-z]+", FILE)
			FILE.close()
			
			if len(isOK) == 0:
				#LFailedTestNames.append(filename)
				#LFailedTqestDesc.append("The file DID NOT terminated normally.")
				continue
 		 
#		lenLines = len(lines) 		 
#		if (whichFollowing > 0 ): 
# 			sum = whichFollowing+num
#			if (sum < lenLines-1):
#					targetLine = lines[num+whichFollowing]
#			else:
#				if (self.DEBUG):
#					print "Out of boundary. Skipping."
#					print "Length of file is:",lenLines, " and the extracted line has a number ", whichFollowing
#				return ""						
#		return targetLine	
		
	def addTest(self, fileName):
 		#self.L.append(testName)
		self.listOfTests.append(fileName)

	def appendExecutionQueue(self, obj):
		self.exQ.append(obj)

	def grep(self,pattern,fileObj):
		r=[]
  		for line in fileObj:
			if re.search(pattern,line):
				r.append(line)
		return r
	
	def runAll(self):
		gc3libs.log.info("Checking the results of your test GAMESS calculations, the output files (exam??.out) will be taken from %s directory.", TestSet.dirLog)
	    #print TestS
		LFailedTestNames = []  
		LFailedTestDesc = []  
		NumberOfTests = len(self.listOfTests)
		NumberOfCorrectTests = 0
		if len(self.listOfTests) == 0:
		   #self.log.warning("The test list is empty.")
		   gc3libs.log.debug("The test list is empty.")
		   return
		#Each INP file shold have a GAMESS OUT file that terminated normally
		for testName in self.listOfTests:
			filename = os.path.splitext(testName)
			print filename
			if len(filename) > 1:
				filename = filename[0] + '.out' 
				print filename
			#:, *.out) #test.name #getCurrentFileName()
		#gc3libs.log.info("Testing filename %s", filename)	
			#Check if the file exists
			if os.path.exists(filename) == False:
					LFailedTestNames.append(filename)
					LFailedTestDesc.append("The file DOES NOT exist.")
					continue
			#Check if the test terminated normally
			FILE = open(filename, 'r')
			isOK = self.grep("TERMINATED+\s+NORMALLY", FILE)	
			FILE.close()
			if len(isOK) == 0:
				LFailedTestNames.append(filename)
				LFailedTestDesc.append("The file DID NOT terminated normally.")
				continue
			NumberOfCorrectTests = NumberOfCorrectTests + 1	
		
		if len(LFailedTestNames) > 0:
			gc3libs.log.info("Please check carefully each of the following runs as these tests will not be executed:")
			for test, desc in zip(LFailedTestNames, LFailedTestDesc):
				print test, ": ", desc
			gc3libs.log.info("Running the remaining %s",NumberOfCorrectTests," tests. ")
		else:
			gc3libs.log.info("Detected %s",NumberOfCorrectTests, "tests.")
			
		NumberOfIncorrectResults = 0
		for testName, testObj in zip(self.listOfTests, self.exQ):
			if testName in LFailedTestNames:
				continue
			(isCorrect, str) = testObj.run()
			
			finalString = testName + ":" + str 
			if (isCorrect):
				print '%-89s    %s' %(finalString, "Passed.")
			else:
				NumberOfIncorrectResults = NumberOfIncorrectResults + 1
				LFailedTestNames.append(filename)
				LFailedTestDesc.append("!!FAILED")
				print '%-89s    %s' %(finalString, "!!FAILED.")
				 
		if NumberOfCorrectTests != NumberOfTests:
			print "Only", NumberOfCorrectTests,"out of",NumberOfTests, "terminated normally."
		else:
			print NumberOfCorrectTests,"out of",NumberOfTests, "terminated normally."
		if NumberOfIncorrectResults == 0:
			gc3libs.log.info("All job(s) got correct numerical results.")
		else:
			gc3libs.log.info("%d job(s) got incorrect numerical results. Please examine why.", NumberOfIncorrectResults) 


	def scanGAMESSinputFile(self,filenameINP, filenameOUT):
		try:	
			file = open(filenameINP, 'r') #"./data/exam01.inp", 'r')
			foundlines = self.grep("GC3", file)	
			file.close()
		except IOError:
			raise IOError("There is a problem with a file %s.", filenameINP) 
		if os.path.exists(filenameOUT) == False:
			raise IOError("The file %s does not exists.", filenameOUT) 
			
			
		i=0
		paramList = []
		functionList = []
		labelAnalyze ="grepLinesAndAnalyze"
		labelFollow = "grepAndFollow"  
		suffix1 = len(labelAnalyze+"(")
		suffix2 = len(labelFollow+"(")

		#print filename
		for line in foundlines:
		    #print i, line
		    if  line.find(labelAnalyze)> 0:
			pos1 = line.find(labelAnalyze)+suffix1  
			end = line.find(")")
			arg1 = line[pos1:end]
			#print "after grepanalyze:", line[pos1:end]
			paramList.append(arg1)
			functionList.append(labelAnalyze)
			pos1 = -1
			continue
		    if line.find(labelFollow) > 0:
			#print "line", line
			pos2 = line.find(labelFollow) + suffix2
			end = line.find(")")
			arg2 = line[pos2:end]
			#print "after grepandfollow:", line[pos2:end]
			pos2 = -1
			functionList.append(labelFollow)
			paramList.append(arg2)	
			continue 	

		for args,function in zip(paramList, functionList):
			 argList = args.split(",")
			 #print argList
			 for arg in argList:
				arg = re.sub(r"[ \"]", r"", arg) 
			 try:
				if function==labelFollow:
					app = TestNextLine(filenameOUT)
					fn = getattr(app, labelFollow)   
					fn(argList[0], argList[1], argList[2], int(argList[3]), float(argList[4]), argList[5], argList[6])
					self.addExecutionQueue(app)
	   			elif function==labelAnalyze:
					app = TestLine(filenameOUT)
					fn = getattr(app, labelAnalyze)   
					fn(argList[0], argList[1], int(argList[2]), float(argList[3]), argList[4], argList[5])
					self.appendExecutionQueue(app)
				
			 except AttributeError:
				gc3libs.log.debug("Attribute error. arguments %s are incorrect", args)                                                                           		
			 #app.debug() # for testing purposes
			 
#ex1.debug()
#ex1.grepLinesAndAnalyze("FINAL \s+ RHF", "tail", 5, -37.2380397698, GamessTst.tolE, "Eerr")
#ex1.grepLinesAndAnalyze("RMS \s+ G", "tail", 8, .0000004, GamessTst.tolG, "Gerr")

#ex2 = TestNextLine("exam04.out")
#ex2.grepAndFollow("OVERLAP\s+LOW", "head","head", 8, 0.64506, GamessTst.tolO, "Oerr")  
#ex2.grepAndFollow("DEBYE", "head","1", 4, 1.249835, GamessTst.tolD, "Derr") #works with head and index 1

TestSet = GamessTestSuite(".")
TestSetNew = GamessTestSuite(".")
TestSetNew.addTest("exam01.inp") #Add app to a list of tests
TestSetNew.scanGAMESSinputFile("./test/data/exam01.inp","test/exam01.out")
#TestSetNew.scanGAMESSinputFile("./test/data/exam04.inp","test/exam04.out")

#TestSetNew.addTest(ex1)
#TestSetNew.addTest(ex2)
TestSetNew.runAll()
  
#example1 = gmsPattern.GamessPattern("exam01.out")
#example1.grepLinesAndAnalyze("FINAL \s+ RHF", "tail", 5, -37.2380397698, GamessTst.tolE, "Eerr")
#example1.grepLinesAndAnalyze("RMS \s+ G", "tail", 8, .0000004, GamessTst.tolG, "Gerr")
##example1.debug()
#example2 = gmsPattern.GamessPattern("exam02.out")
#example2.grepLinesAndAnalyze("FINAL \s+UHF", "", 5, -37.2810867259, GamessTst.tolG, "Gerr")
#example2.grepLinesAndAnalyze("RMS \s+G", "", 4, 0.027589766, GamessTst.tolG, "Gerr")
#example2.grepLinesAndAnalyze("S-SQUARED", "", 3, 2.013, GamessTst.tolS, "Serr")
#example2.grepLinesAndAnalyze("FINAL+\s LOC", "head", 6, 30.57, GamessTst.tolL, "Lerr")
#example2.grepLinesAndAnalyze("FINAL+\s LOC", "tail", 6, 25.14, GamessTst.tolL, "Lerr")
##example2.debug()

#example3 = gmsPattern.GamessPattern("exam03.out")
#example3.grepLinesAndAnalyze("FINAL\s+ ROHF", "", 5, -37.2778767090, GamessTst.tolE, "Eerr")
#example3.grepLinesAndAnalyze("RMS \s+G", "", 4, 0.027505548, GamessTst.tolG, "Gerr")
#example3.grepAndFollow("DEBYE", "head", "head", 4, 0.025099, GamessTst.tolD, "Derr")

#example4 = gmsPattern.GamessPattern("exam04.out")
#example4.grepLinesAndAnalyze("FINAL \s+GVB", "", 5, -37.2562020559, GamessTst.tolE, "Eerr")
#example4.grepLinesAndAnalyze("RMS \s+G", "", 4, 0.019618475, GamessTst.tolG, "Gerr")
#example4.grepAndFollow("OVERLAP\s+LOW", "head","head", 8, 0.64506, GamessTst.tolO, "Oerr")  
#example4.grepAndFollow("DEBYE", "head","1", 4, 1.249835, GamessTst.tolD, "Derr") #works with head and index 1

#example5 = gmsPattern.GamessPattern("exam05.out")
##example5.debug()
# In exam05: -38.313003683
#example5.grepLinesAndAnalyze("STATE\s\#\s\s\s\s", "", 6, 6.145036726, GamessTst.tolE, "Eerr")
#example5.grepLinesAndAnalyze("RMS \s+G", "", 4, 0.032264079, GamessTst.tolG, "Gerr")
#example5.grepAndFollow("DEBYE", "head","1", 4, 0.691104, GamessTst.tolD, "Derr")  

#MPI_ABORT was invoked on rank 0 in communicator MPI_COMM_WORLD 
#with errorcode 911.
#example6 = gmsPattern.GamessPattern("exam06.out")
#example6.grepLinesAndAnalyze("FINAL \s+MCSCF", "tail", 5, -37.2581791690, GamessTst.tolE, "Eerr")
#example6.grepLinesAndAnalyze("RMS \s+G", "tail", 8, 0.0000012, GamessTst.tolG, "Gerr")
##example6.debug()

#example7 = gmsPattern.GamessPattern("exam07.out")
#example7.grepLinesAndAnalyze("FINAL \s+ RHF", "head", 5, -414.0945320827, GamessTst.tolE, "Eerr")
#example7.grepLinesAndAnalyze("RMS \s+G", "head", 4, 0.023723712, GamessTst.tolG, "Gerr")
#example7.grepAndFollow("DEBYE", "head", "head", 4, 2.535169, GamessTst.tolD, "Derr")
#OR #example4.grepAndFollow("DEBYE", "1", 4, 1.249835, GamessTst.tolD, "Derr") #works with head and index 1
#OR #example4.grepAndFollow("DEBYE", 1, 4, 1.249835, GamessTst.tolD, "Derr") #works with head and index 1
##example7.debug()

#example8 = gmsPattern.GamessPattern("exam08.out")
#example8.grepLinesAndAnalyze("E\(MP2\)", "tail", 2, -75.7060362006, GamessTst.tolE, "Eerr")
#example8.grepLinesAndAnalyze("RMS \s+G", "tail", 4, 0.017449522, GamessTst.tolG, "Gerr")
##example8.grepAndFollow("DEBYE", "tail", "head", 4, 2.329368, GamessTst.tolD, "Derr")
#example8.grepAndFollow("DEBYE", "tail", "1", 4, 2.329368, GamessTst.tolD, "Derr")

##example8.debug()
#set nD=0`grep -n DEBYE $1 | tail -1 | cut -d: -f1 | awk '{ print $1+1 }'`
#set D=`sed -n -e "$nD p" $1 | awk '{ print $4 }'`


#example9 = gmsPattern.GamessPattern("exam09.out")
#example9.grepLinesAndAnalyze("E\(MP2\)", "tail", 12, -75.7109705643, GamessTst.tolE, "Eerr")

#example10 = gmsPattern.GamessPattern("exam10.out")
#example10.grepLinesAndAnalyze("FINAL \s+ RHF", "", 5, -74.9659012171, GamessTst.tolW, "Werr")
#example10.grepLinesAndAnalyze("FREQUENCY\:", "tail", 3, 2170.04, GamessTst.tolE, "Eerr")
#example10.grepLinesAndAnalyze("IR+\s+INTENSITY\:", "tail", 4, 0.17128, GamessTst.tolI, "Ierr")
#example10.grepLinesAndAnalyze("MEAN+\sALPHA+\sPOL", "", 5,0.40079, GamessTst.tolP, "Perr")

#example11 = gmsPattern.GamessPattern("exam11.out")
#example11.grepLinesAndAnalyze("TOTAL \s+  ENERGY", "tail", 4, -91.5814775, 0.000001, "Eerr")
#example11.grepLinesAndAnalyze("PATH\s+ DISTANCE", "tail", 6, 0.89968, GamessTst.tolR, "Rerr")

#example12 = gmsPattern.GamessPattern("exam12.out")
#example12.grepLinesAndAnalyze("FINAL \s R-SVWN", "tail", 5, -76.5841347569, GamessTst.tolE, "Eerr")
#example12.grepLinesAndAnalyze("RMS \s G", "tail", 8, 0.0000007, GamessTst.tolG, "Gerr") #TODO: 8-> 9 causes error!!!

#example13 = gmsPattern.GamessPattern("exam13.out")
#example13.grepLinesAndAnalyze("FINAL\sRHF", "tail", 5, -76.0440311075, GamessTst.tolE, "Eerr")
#example13.grepAndFollow("EFGZZ", "tail", "head", 3, -0.043055, GamessTst.tolG, "Gerr")  
#example13.grepAndFollow("ESU-CM", "tail", "head", 3, -1.296492, GamessTst.tolD, "Derr")  
##example13.debug()

#example14 = gmsPattern.GamessPattern("exam14.out")
#example14.grepLinesAndAnalyze("STATE\s\#\s\s\s\s 1", "", 6, -75.010111355, GamessTst.tolE, "Eerr")
#example14.grepLinesAndAnalyze("STATE\s\#\s\s\s\s 2", "", 6, -74.394581939, GamessTst.tolE, "Eerr")  
#example14.grepLinesAndAnalyze("E[*]BOHR", 2, 7, 0.392614, GamessTst.tolD, "Derr")  
#example14.grepLinesAndAnalyze("E[/]BOHR", 1, 8, 0.368205, GamessTst.tolD, "Derr")  
##example14.debug()
#set Ea=`grep "STATE #    1" $1 | awk '{ print $6 }'`0
#set Eb=`grep "STATE #    2" $1 | awk '{ print $6 }'`0
#set Da=`grep "E[*]BOHR" $1 | head -2 | tail -1 | awk '{ print $7 }'`0
#set Db=`grep "E[/]BOHR" $1 | head -2 | tail -1 | awk '{ print $8 }'`0

#example15 = gmsPattern.GamessPattern("exam15.out")
#example15.grepLinesAndAnalyze("FINAL \s GVB", "tail", 5, -75.5579181058, GamessTst.tolE, "Eerr")

#example16 = gmsPattern.GamessPattern("exam16.out")
#example16.grepLinesAndAnalyze("FINAL \s GVB", "tail", 5, -288.8285729745, GamessTst.tolE, "Eerr")

#example17 = gmsPattern.GamessPattern("exam17.out")
#example17.grepLinesAndAnalyze("FINAL \s+ GVB", "", 5, -38.3334724789, GamessTst.tolE, "Eerr")
#example17.grepLinesAndAnalyze("FREQUENCY\:", "tail", 3, 1224.19, GamessTst.tolW, "Werr")
#example17.grepLinesAndAnalyze("IR+\s+INTENSITY\:", "tail", 4, 0.13317, GamessTst.tolI, "Ierr")
#example17.grepLinesAndAnalyze("MEAN+\sALPHA+\sPOL", "", 5, 0.53018, GamessTst.tolP, "Perr")

#example18 = gmsPattern.GamessPattern("exam18.out")
#example18.grepLinesAndAnalyze("FINAL \s+ RHF", "", 5, -12.6956518722, GamessTst.tolE, "Eerr")
#example18.grepLinesAndAnalyze("FREQUENCY\:", "tail", 2, 913.17, GamessTst.tolW, "Werr")

#Different values
#example19 = gmsPattern.GamessPattern("exam19.out")
#example19.grepLinesAndAnalyze("STATE\s\s\s 1:\sREL", "", 5, -15296.570, 0.05, "EAerr")
#example19.grepLinesAndAnalyze("STATE\s\s\s 2:\sREL", "", 5, -15296.432, 0.05, "EBerr")  
#set Ea0=-15296.570
#set Eb0=-15296.432
#set tolE0=0.05
#set Ea=`grep "STATE   1: REL" $1 | awk '{ print $5 }'`0
#set Eb=`grep "STATE   2: REL" $1 | awk '{ print $5 }'`0

#example20 = gmsPattern.GamessPattern("exam20.out")
#example20.grepLinesAndAnalyze("FINAL \s+ RHF", "tail", 5, -11.3010023066, GamessTst.tolE, "Eerr")
#example20.grepLinesAndAnalyze("P(1)=", "tail", 2, 0.036713, GamessTst.tolW, "Werr")
#TODO: THE FILE DOES NOT EXISTS UNSUPPORTED

#set E0=-11.3010023066
#set X0=0.036713
#set E=`grep "FINAL RHF" $1 | tail -1 | awk '{ print $5 }'`0
#set X=`grep "P(1)=" $1 | tail -1 | tr -d "," | awk '{ print $2 }'`0

#example21 = gmsPattern.GamessPattern("exam21.out")
#example21.grepLinesAndAnalyze("FINAL \s+ GVB", "tail", 5, -39.2510351247, GamessTst.tolE, "Eerr")
#example21.grepLinesAndAnalyze("FREQUENCY\:", 2, 6, 988.81, GamessTst.tolW, "Werr")
#example21.grepLinesAndAnalyze("IR+\s+INTENSITY\:", 2, 7, 4.54558, GamessTst.tolI, "Ierr")
#example21.grepLinesAndAnalyze("MEAN+\sALPHA+\sPOL", "", 5, 2.04654, GamessTst.tolP, "Perr")

#example22 = gmsPattern.GamessPattern("exam22.out")
#example22.grepLinesAndAnalyze("^E\(MP2\)", "head", 2, -94.2315757676, GamessTst.tolE, "Eerr")
#example22.grepLinesAndAnalyze("RMS \s+G", "head", 4, 0.003359469, GamessTst.tolG, "Gerr")
#example22.grepAndFollow("DEBYE", "tail", "head", 4, 2.098487, GamessTst.tolD, "Derr")
##example22.debug()
# set E0=-94.2315757676
# set G0=0.003359469
# set D0=2.098487
# 
# set E=`grep "E(MP2)=  " $1 | awk '{ print $2 }'`0
# set G=`grep "RMS G" $1 | awk '{ print $4 }'`0
# set nD=0`grep -n DEBYE $1 | tail -1 | cut -d: -f1 | awk '{ print $1+1 }'`
# set D=`sed -n -e "$nD p" $1 | awk '{ print $4 }'`

#example23 = gmsPattern.GamessPattern("exam23.out")
#example23.grepLinesAndAnalyze("HEAT+\s+OF+\s+FORM", "tail", 5, -2.79646, GamessTst.tolE, "Eerr")
#example23.grepLinesAndAnalyze("RMS \s+G", "tail", 8, 0.0000187, GamessTst.tolG, "Gerr")

#example24 = gmsPattern.GamessPattern("exam24.out")
#example24.grepLinesAndAnalyze("FINAL \s+ RHF", "", 5, -74.9666740766, GamessTst.tolE, "Eerr")
#example24.grepLinesAndAnalyze("RMS\s+G", "", 4, 0.033467686, GamessTst.tolG, "Gerr")
#example24.grepLinesAndAnalyze("Z\(IND\)", "", 3, -0.03663, GamessTst.tolD, "Derr")
##example24.debug()
# set E0=-74.9666740766
# set G0=0.033467686
# set D0=-0.03663
# 
# set E=`grep "FINAL RHF" $1 | awk '{ print $5 }'`0
# set G=`grep "RMS G" $1 | awk '{ print $4 }'`0
# set D=`grep "Z(IND)" $1 | awk '{ print $3 }'`0

#example25 = gmsPattern.GamessPattern("exam25.out")
#example25.grepLinesAndAnalyze("FINAL\s R-AM1", "tail", 5, -48.7022570475, GamessTst.tolE, "Eerr")
#example25.grepLinesAndAnalyze("RMS \s+G", "tail", 8, 0.0000207, GamessTst.tolG, "Gerr")

#example26 = gmsPattern.GamessPattern("exam26.out")
#example26.grepLinesAndAnalyze("FINAL \s+ RHF", "", 5, -415.2660357291, GamessTst.tolE, "Eerr")
#example26.grepLinesAndAnalyze("DIAGONAL\s SUM\s D", "tail", 4, 28.389125, GamessTst.tolL, "Lerr")

# set E0=-415.2660357291
# set L0=28.389125
# 
# set E=`grep "FINAL RHF" $1 | awk '{ print $5 }'`0
# set L=`grep "DIAGONAL SUM D" $1 | tail -1 | awk '{ print $4 }'`0

#example27 = gmsPattern.GamessPattern("exam27.out")
#example27.grepAndFollow("SQRT\(AMU\)", "tail",  "head", 8, -9.12710, 0.0001, "Eerr")
#example27.grepAndFollow("VEL\(1\)=", "tail", "head", 3, 0.028857623667, GamessTst.tolV, "Verr")  

# set E0=-9.12710
# set V0=0.028857623667
# set tolE0=0.0001
# # customise energy tolerance for DRC
# 
# set nE=0`grep -n "SQRT(AMU)" $1 | tail -1 | cut -d: -f1 | awk '{ print $1+1 }'`
# set E=`sed -n -e "$nE p" $1 | awk '{ print $8 }'`
# set nV=0`grep -n "VEL(1)=" $1 | tail -1 | cut -d: -f1 | awk '{ print $1+3 }'`
# set V=`sed -n -e "$nV p" $1 | awk '{ print $3 }'`

#example28 = gmsPattern.GamessPattern("exam28.out")
#example28.grepLinesAndAnalyze("TOTAL+\s INTER", "tail", 6, -8.96, GamessTst.tolH, "Herr")

#example29 = gmsPattern.GamessPattern("exam29.out")
#example29.grepLinesAndAnalyze("E\(MP2\)=", "tail", 2, -229.3943496933, GamessTst.tolE, "Eerr")
##example29.debug()
#set E0=-229.3943496933
#set E=`grep "E(MP2)=" $1 | tail -1 | awk '{ print $2 }'`0

#example30 = gmsPattern.GamessPattern("exam30.out")
#example30.grepLinesAndAnalyze("FINAL \s RHF", "", 5, -169.0085355753, GamessTst.tolE, "Eerr")
#example30.grepLinesAndAnalyze("RMS \s G", "", 4, 0.008157643, GamessTst.tolG, "Gerr")

#example31 = gmsPattern.GamessPattern("exam31.out")
#example31.grepLinesAndAnalyze("FINAL \s RHF", "tail", 5, -115.0425622162, GamessTst.tolE, "Eerr")
#example31.grepLinesAndAnalyze("RMS \s G", "tail", 8, 0.0000033, GamessTst.tolG, "Gerr")
#example31.grepLinesAndAnalyze("TOTAL \s INTERACTION \s \(DELTA", "tail", 13, -0.0079213676, GamessTst.tolE, "Eerr")

#example32 = gmsPattern.GamessPattern("exam32.out")
#example32.grepLinesAndAnalyze("CR-CCSD\(T\)\_L\s ENERGY\:", "tail", 5, -130.1517479953, GamessTst.tolE, "Eerr")
#example32.grepLinesAndAnalyze("T1 \s DIAGNOSTIC", "", 4, 0.01448788, GamessTst.tolT, "Terr")

#MPI_ABORT was invoked on rank 0 in communicator MPI_COMM_WORLD 
#with errorcode 911.
#example33 = gmsPattern.GamessPattern("exam33.out")
#example33.grepLinesAndAnalyze("FINAL \s MCSCF", "", 5, -93.0223942017, GamessTst.tolE, "Eerr")
#example33.grepLinesAndAnalyze("RMS \s G", "head", 4, 0.045100935, GamessTst.tolG, "Gerr")

#set E0=-93.0223942017
#set G0=0.045100935

#set E=`grep "FINAL MCSCF" $1 | awk '{ print $5 }'`0
#set G=`grep "RMS G" $1 | head -1 | awk '{ print $4 }'`0

#example34 = gmsPattern.GamessPattern("exam34.out")
#example34.grepLinesAndAnalyze("CONVERGED \s STATE", "tail", 5, -113.7017742428, GamessTst.tolE, "Eerr")
#example34.grepLinesAndAnalyze("RMS \s G", "tail", 4, 0.025762549, GamessTst.tolG, "Gerr")

#set E0=-113.7017742428
#set G0=0.025762549

#set E=`grep "CONVERGED STATE" $1 | tail -1 | awk '{ print $5 }'`0
#set G=`grep "RMS G" $1 | tail -1 | awk '{ print $4 }'`0

#example35 = gmsPattern.GamessPattern("exam35.out")
#example35.grepLinesAndAnalyze("FINAL \s ROHF", "tail", 5, -2259.0955118369, 0.00000007, "Eerr")

#example36 = gmsPattern.GamessPattern("exam36.out")
#example36.grepLinesAndAnalyze("FINAL \s MCSCF", "", 5, -77.9753563843, GamessTst.tolE, "Eerr")
#example36.grepLinesAndAnalyze("FREQUENCY:", "2", 4, 319.87, GamessTst.tolW, "Werr") #head -2 | tail -1
#example36.grepLinesAndAnalyze("IR \s INTENSITY:", "2", 5, 1.09748, GamessTst.tolI, "Ierr") 

#example37 = gmsPattern.GamessPattern("exam37.out")
#example37.grepLinesAndAnalyze("cule\:\sEuncorr\(2\)=", "", 7, -224.910612407, GamessTst.tolE, "Eerr")
#example37.grepLinesAndAnalyze("\(2\) \s MAXIMUM\s GRADIENT", "", 9, 0.0267805, GamessTst.tolG, "Gerr")  
##example37.debug()
#set E0=-224.910612407
#set G0=0.0267805

#set E=`grep "cule: Euncorr(2)=" $1 | awk '{ print $7 }'`0
#set G=`grep "(2) MAXIMUM GRADIENT" $1 | awk '{ print $9 }'`0

#example38 = gmsPattern.GamessPattern("exam38.out")
#example38.grepLinesAndAnalyze("FINAL \s RHF", "tail", 5, -116.3976902416, GamessTst.tolE, "Eerr")
#example38.grepLinesAndAnalyze("RMS \s G", "tail", 8, 0.0000001, GamessTst.tolG, "Gerr")

#example39 = gmsPattern.GamessPattern("exam39.out")
#example39.grepAndFollow("Raman\s Intensity \s at \s omega", "head", 8, 3, 122.21, 0.01, "RIerr")
#example39.grepAndFollow("Hyper \s Raman\s Intensity \s at \s omega", "head", 8, 3, 976.50, 0.01, "HRIerr")

#example40 = gmsPattern.GamessPattern("exam40.out")
#example40.grepLinesAndAnalyze("ENERGY \s OF \s FIRST \s STATE", "tail", 6, -38.941516, 0.000001, "E1err")
#example40.grepLinesAndAnalyze("ENERGY \s OF \s SECOND \s STATE", "tail", 6, -38.941513, 0.000001, "E2err")
#example40.grepLinesAndAnalyze("RMS \s EFFECTIVE \s GRADIENT", "tail", 5, 0.000005, 0.000001, "RMSerr")

#example41 = gmsPattern.GamessPattern("exam41.out")
#select 5th line AFTER the matched one
#example41.grepAndFollow("SUMMARY+\s OF+\sTDDFT+\s RESULTS", "head", 5, 4, 8.474, 0.001, "EXCerr eV")
#example41.grepAndFollow("SUMMARY+\s OF+\sTDDFT+\s RESULTS", "head", 5, 8, 0.094, 0.001, "OSCerr")
#set G=`grep "RMS G" $1 | tail -1 | awk '{ print $4 }'`0
#example41.grepLinesAndAnalyze("RMS \s+G", "tail", 4, 0.112201641, GamessTst.tolG, "Gerr")

#example42 = gmsPattern.GamessPattern("exam42.out")
#select 5th line AFTER the matched one
#example42.grepLinesAndAnalyze("CR-CCSD\(T\)\_L\sE=", "head", 5, -92.4930167395, GamessTst.tolG, "Gerr")
#example42.grepLinesAndAnalyze("RMS \s+G", "head", 4, 0.026601131, GamessTst.tolG, "Gerr")

#example43 = gmsPattern.GamessPattern("exam43.out")
#example43.grepLinesAndAnalyze("HEAT\s OF\s FORMATION\s \(298K\)", "tail", 5, -17.83, 0.01, "HEATerr kcal/mol")

#example44 = gmsPattern.GamessPattern("exam44.out")
#example44.grepLinesAndAnalyze("FINAL", "tail", 5, -599.9687803934, GamessTst.tolE, "SCFerr")
#example44.grepLinesAndAnalyze("E\(MP2\)=", "tail", 2, -600.7532099625, GamessTst.tolE, "MP2err")
#example44.debug()
#TestSet.addTest(example1)
#TestSet.addTest(example2)
#TestSet.addTest(example3)
#TestSet.addTest(example4)
#TestSet.addTest(example5) #Failed
#TestSet.addTest(example6) #Terminated abnormally
#TestSet.addTest(example7)
#TestSet.addTest(example8)
#TestSet.addTest(example9) #Failed
#TestSet.addTest(example10)
#TestSet.addTest(example11)
#TestSet.addTest(example12)
#TestSet.addTest(example13)
#TestSet.addTest(example14) #Failed
#TestSet.addTest(example15)
#TestSet.addTest(example16)
#TestSet.addTest(example17)
#TestSet.addTest(example18)
#TestSet.addTest(example19) #Failed
#TestSet.addTest(example20) #File Not found
#TestSet.addTest(example21)
#TestSet.addTest(example22)
#TestSet.addTest(example23)
#TestSet.addTest(example24)
#TestSet.addTest(example25)
#TestSet.addTest(example26) #Terminated abnormally
#TestSet.addTest(example27)
#TestSet.addTest(example28)
#TestSet.addTest(example29)
#TestSet.addTest(example30)
#TestSet.addTest(example31)
#TestSet.addTest(example32)
#TestSet.addTest(example33) #Terminated abnormally
#TestSet.addTest(example34)
#TestSet.addTest(example35)
#TestSet.addTest(example36)
#TestSet.addTest(example37)
#TestSet.addTest(example38)
#TestSet.addTest(example39)
#TestSet.addTest(example40)
#TestSet.addTest(example41)
#TestSet.addTest(example42) #Terminated abnormally
#TestSet.addTest(example43) #Terminated abnormally
#TestSet.addTest(example44)  

#python -m pdb ./gdemo.py

#Separate into 2 classes. 
# ! GC3: <name of class, parameters> # so I know which method to run

 
#NOTES:
#Incorrect grepex expression is not supported. Example:grepLinesAndAnalyze("TOTAL \s INTERACTION \s (DELTA", .. will raise a traceback.
#Special characters such as : < ( * need to be defined as /< /( /*
#head -2 | tail -1 is translated into "2"
#Interesting cases: exam39

#TUTORIAL
# Each test is started with a search for a regular expression. The regular expression
# outputs either a single line or a group of lines. 
# matchedLinePosition defines which matched line to select: first, last or Nth line. (tail | head | "" | "2" | 2)
# and returns ONE final line with target information.
# 
# There are two cases. 
# 1. If you want to extract the values from the target line run grepLinesAndAnalyze.
# 2. If you want to extract the values from the line that FOLLOWS the target line run grepAndFollow.

# positionInLine extracts the value from the target line by extracting the column WITHIN the line.
# Example: positionInLine =  "4" | 4
# Value defines a value which will be compared with the value extracted from the file.
# Tol - acceptable difference between Value and value extracted from the text		
