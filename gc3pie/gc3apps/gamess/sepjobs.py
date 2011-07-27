#! /usr/bin/env python

# TAKES 106 MIN. FOR SEPARATING ~2100 OUTPUT FILES

import os
import re
import sys
import fnmatch
import shutil

def CLEANLIST(itmlst, delstr) :
#	print 'dirlist =',dirlist
#	dellist = []
	delpos = []; p = 0					# list of deleting position, position
	for itm in itmlst :
		if re.match(delstr, itm) :		# "\" to suppress interpret. of "." by python
#			dellist.append(singdir)
			delpos.append(p)
		p += 1
#	print 'dellist=',dellist
#	print 'delpos=',delpos
	ndp = 0								# no. of deleted positions
	for p in delpos :			
		p -= ndp				# convert. pos. in old dirlist to pos. in current one
		itmlst.pop(p)
#		del dirlist(p)			# gives error "can't delete function call"
		ndp += 1
#	print 'cleaned dirlist =',dirlist	
	return itmlst

def SEARCHINPUT(inpdir, fltype, pthlst, inpfllst) :
	fltype = '*.' + fltype 
	print 'file type =',fltype 
	for root, dirlist, filelist in os.walk(os.path.expanduser('~/')) :
	#	print 'root =',root
	#	if os.path.basename(root) == inpdir :
	# pattern matching in dirlist, recusion depth with count '/' in root
	# inherit search state to next loop
		if re.search(inpdir, root) : 
			CLEANLIST(filelist, '\.')
			lst = fnmatch.filter(filelist, fltype)
			if lst != [] :
				pthlst.append(root)
				inpfllst.append(lst)
		CLEANLIST(dirlist, '\.')
	for p in range(0,len(pthlst)) :
		print 'specified directory found\n',pthlst[p]
#		print 'input files found\n',inpfllst[p]
	return pthlst, inpfllst

inpdir = sys.argv[1]						# directory provided by user
fltype = sys.argv[2]						# file type provided by user
sepstr = sys.argv[3]						# string for file separation
relloc1 = float(sys.argv[4])				# 1. relative location of string in file
relloc2 = float(sys.argv[5])				# 2. relative location of string in file
pthlst = []; inpfllst = []					# pathlist, input file list

SEARCHINPUT(inpdir, fltype, pthlst, inpfllst)

sepstr2 = sepstr.strip('.') 
sepstr2 = sepstr2.replace(' ','_')
sepstr2 = sepstr2.replace('/','_') 
ysdir = inpdir.strip('/') + '-' + sepstr2 + '.ys'
nodir = inpdir.strip('/') + '-' + sepstr2 + '.no'

try :
	os.mkdir(ysdir)
	os.mkdir(nodir)
except OSError :
	print 'ysdir and/or nodir already exsist' 
print 'ysdir=',ysdir,'\nnodir=',nodir

bytes_char = 1 				#hrdrng / len(sepstr)
hrdrng = len(sepstr) * bytes_char #sys.getsizeof(sepstr)# hard range of search in bytes

print 'hard range =',hrdrng,'bytes per char.=',bytes_char
sysencode = sys.getdefaultencoding()
print 'default encoding of system:', sys.getdefaultencoding()
p = -1 ; failrt = 0 ; hitrt = 0
for pth in pthlst :
#	print 'GOING INTO PTH=',pth
	btpos = 0 ; btrng = 0 ; p += 1
	for inpfl in inpfllst[p] :
#		print 'GOING INTO INPFL=',inpfl
		inppth = pth + '/' + inpfl
		with open(inppth, 'r') as opnfl :
			flencode = opnfl.encoding
			if flencode == None : 
				flencode = sysencode
			else :
				print 'file encoding:', flencode
			flsize = os.path.getsize(inppth)
#			if btrng == 0 : btrng = flsize - btpos
			btpos = int(flsize * relloc1)
			btrng = int(flsize * (relloc2 - relloc1))
			opnfl.seek(btpos, 0)			# going to byte position in file
			flcont = opnfl.read(btrng)		# reading byte range from byte pos. file  
			opnfl.close()
		mtch = re.search(sepstr, flcont)
		if mtch :
			hitrt += 1
		else :
			with open(inppth, 'r') as opnfl :
				flcont = opnfl.read()
				opnfl.close()
			mtch = re.search(sepstr, flcont)
			if mtch : failrt += 1
	
print 'failure rate=',failrt,'hit rate =', hitrt
			
#		mtchpos = (mtch.end(0) + mtch.start(0)) / 2 * bytes_char	# size of matched string
#			shutil.copy(inppth, ysdir)
#		else : 
#			shutil.copy(inppth, nodir)
	
