    #! /usr/bin/env python
#

# Copyright (C) 2011  University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
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
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>, Benjamin Jonen <benjamin.jonen@bf.uzh.ch>'
# summary of user-visible changes
__changelog__ = """

"""
__docformat__ = 'reStructuredText'

from __future__ import absolute_import, print_function
import gc3libs.debug
import numpy as np
import re, os, sys
import logbook
from threading import Lock

class wrapLogger():
    def __init__(self, loggerName = 'myLogger', streamVerb = 'DEBUG', logFile = 'logFile'):
        self.loggerName = loggerName
        self.streamVerb = streamVerb
        self.logFile    = logFile
        logger = getLogger(loggerName = self.loggerName, streamVerb = self.streamVerb, logFile = self.logFile)
        self.wrappedLog = logger

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['wrappedLog']
        return state
    def __setstate__(self, state):
        self.__dict__ = state
        logger = getLogger(loggerName = self.loggerName, streamVerb = self.streamVerb, logFile = self.logFile)
        self.wrappedLog = logger
        
    def __getattr__(self, attr):
        # see if this object has attr
        # NOTE do not use hasattr, it goes into
        # infinite recurrsion
        if attr in self.__dict__:
            # this object has it
            return getattr(self, attr)
        # proxy to the wrapped object
        return getattr(self.wrappedLog, attr)
    
    def __hasattr__(self, attr):
        if attr in self.__dict__:
            return getattr(self, attr)
        return getattr(self.wrappedLog, attr)
    


def getLogger(loggerName = 'mylogger.log', streamVerb = 'DEBUG', logFile = 'log'):

    # Get a logger instance.
    logger = logbook.Logger(name = loggerName)
    
    # set up logger
    mySH = logbook.StreamHandler(stream = sys.stdout, level = streamVerb.upper(), format_string = '{record.message}', bubble = True)
    mySH.format_string = '{record.message}'
    logger.handlers.append(mySH)
    if logFile:
        myFH = logbook.FileHandler(filename = logFile, level = 'DEBUG', bubble = True)
        myFH.format_string = '{record.message}' 
        logger.handlers.append(myFH)   
    
    try:
        stdErr = list(logbook.handlers.Handler.stack_manager.iter_context_objects())[0]
        stdErr.pop_application()
    except: 
        pass
    return logger


def lower(npStrAr):
    return np.fromiter((x.lower() for x in npStrAr.flat),
                       npStrAr.dtype, npStrAr.size)


def flatten(lst):
    for elem in lst:
        if type(elem) in (tuple, list):
            for i in flatten(elem):
                yield i
        else:
            yield elem


def str2tuple(strIn, conv=int):
    """
    Convert tuple-like strings to real tuples.
    Example::

      >>> str2tuple('(1,2,3,4)')
      (1, 2, 3, 4)

    Enclosing parentheses can be omitted::

      >>> str2tuple('1,2,3')
      (1, 2, 3)

    An optional conversion function can be specified as second argument
    `conv` (defaults to `int`)::

      >>> str2tuple('1,2,3', float)
      (1.0, 2.0, 3.0)
    """
    strIn = strIn.strip()
    if strIn.startswith('('):
        strIn = strIn[1:-1] # removes the leading and trailing brackets
    items = strIn.split(',')
    return tuple(conv(x) for x in items)


class getIndex():
# Python bug http://stackoverflow.com/questions/1152238/python-iterators-how-to-dynamically-assign-self-next-within-a-new-style-class
# Cannot use new type class, that is: class getIndex(object) does not allow setting next method on the fly. 
    """
    Iterator that yields loop indices for an arbitrary nested loop.
    Inputs: base - array of elements in each dimension
    Output: loopIndex - array of current loop index
    """    

    def __init__(self, base, restr = None, direction = 'rowWise'):
        '''
          Direction specifies which direction is incremented. For matrix A = [1 , 2; rowwise means (0,0), (0,1)
          while columnwise means (0,0), (1, 0).                               3 , 4]
        '''
        if isinstance(base, int):
            baseList = []
            baseList.append(base)
            base = baseList
        self.base = np.array(base)
        self.loopIndex = np.zeros(len(self.base), dtype=np.int16)
        self.iteration = 1
        self.direction = direction

        if isinstance(restr, np.ndarray):
            self.restr = restr[0].lower()
        elif restr:      
            self.restr = restr.lower()
        else:
            self.restr = restr
        diagnolStrings = ['diagnol', 'diag']
        if self.restr in diagnolStrings:
 #           getIndex.next = self.nextDiag
            self.next = self.nextDiag
        else:
#            getIndex.next = self.nextStd
            self.next = self.nextStd

        if direction.lower() == 'rowwise':
            self.increment = self.incrementRowWise
        elif direction.lower() == 'columnwise':
            self.increment = self.incrementColumnWise
        else:
            print('cannot understand direction')
            sys.exit()

    def nextDiag(self):
        if self.iteration > 1:
            self.loopIndex += 1
        self.iteration += 1
        if all(self.loopIndex == min(self.base)):
            raise StopIteration
        return self.loopIndex.tolist()

    def __iter__(self):
        return self
    #getIndex(self.base, self.restr, self.direction)

    def incrementRowWise(self):
        for ix in xrange(len(self.base) - 1, -1, -1):
            if self.loopIndex[ix] == self.base[ix] - 1:
                self.loopIndex[ix] = 0
            else:
                self.loopIndex[ix] += 1
                return

    def incrementColumnWise(self):
        for ix in xrange(0, len(self.base), +1):
            if self.loopIndex[ix] == self.base[ix] - 1:
                self.loopIndex[ix] = 0
            else:
                self.loopIndex[ix] += 1
                return

    def lowerTr(self):
        for ix in xrange(0, len(self.base) - 1) :
            if self.loopIndex[ix] < self.loopIndex[ix + 1]:
                return False
        return True

    def diagnol(self):
        isDiagnolIndex = all([x == self.loopIndex[0] for x in self.loopIndex])
        # Skip if not diagnol
        return not isDiagnolIndex  

    def nextStd(self):
        skip = True
        while skip == True:
            if self.iteration > 1:
                self.increment()

            if list(self.loopIndex) == [0] * len(self.base) and self.iteration > 1:
                raise StopIteration

            self.iteration += 1

            if self.restr == 'lowertr':
                skip = self.lowerTr()
            elif self.restr == None or self.restr == 'none':
                skip = False
            else:
                raise gc3libs.exceptions.InvalidArgument(
                    "Unknown restriction '%s'" % self.restr)

        return self.loopIndex.tolist()


@gc3libs.debug.trace
def extractVal(ixVar, vals, index):
    """
    Return variable value to be updated. 
    Functions allows to specify more involved retrieval. 
    """
    return str2vals(vals[ixVar])[index[ixVar]]


@gc3libs.debug.trace
def str2vals(strIn):
    """
    strIn: string containing different vals
    out:   np.array of different vals
    Function can be used to store val vectors in one overall vector and then unpack the string. 
    """
    if 'linspace' in strIn:
        out = np.array([])
        while strIn:
            if re.match('\s*linspace', strIn):
                (linSpacePart, strIn) = re.match('(\s*linspace\(.*?\)\s*)[,\s*]*(.*)', strIn).groups()
                args = re.match('linspace\(([(0-9\.\s-]+),([0-9\.\s-]+),([0-9\.\s-]+)\)', linSpacePart).groups()
                args = [ float(arg) for arg in args] # assume we always want float for linspace
                linSpaceVec = np.linspace(args[0], args[1], args[2])
                out = np.append(out, linSpaceVec)
            elif re.match('\s*[0-9\.]*\s*,', strIn):
                (valPart, strIn) = re.match('(\s*[0-9\.]*\s*)[,\s*]*(.*)', strIn).groups()
                valPart = valPart.strip()
                if '.' in valPart:
                    valPart = float(valPart)
                else:
                    valPart = int(valPart)
                out = np.append(out, valPart)
        return out
    else:
        return str2mat(strIn)

def format_newVal(newVal):
    import re
    if re.match("^[-]*[0-9]*\.[0-9e-]*$", str(newVal)):
#    if '.' in str(newVal):
        newValMat = ('%25.15f' % float(newVal)).strip()
    else:
        try:
            # try to convert to integer, and use decimal repr
            newValMat = str(int(newVal))
        except ValueError: 
            # then it's a string
            newValMat = newVal
    return newValMat



@gc3libs.debug.trace
def update_parameter_in_file(path, varIn, paraIndex, newVal, regexIn):
    _loop_regexps = {
        'bar-separated':(r'([a-z]+[\s\|]+)'
                         r'(\w+)' # variable name
                         r'(\s*[\|]+\s*)' # bars and spaces
                         r'([\w\s\.,;\[\]\-]+)' # value
                         r'(\s*)'),
        'space-separated':(r'(\s*)'
                           r'(\w+)' # variable name
                           r'(\s+)' # spaces (filler)
                           r'([\w\s\.,;\[\]\-]+)' # values
                           r'(\s*)'), # spaces (filler)
    }
    isfound = False
    if regexIn in _loop_regexps.keys():
        regexIn = _loop_regexps[regexIn]
    paraFileIn = open(path, 'r')
    paraFileOut = open(path + '.tmp', 'w')
    for line in paraFileIn:
        #print "Read line '%s' " % line
        if not line.rstrip(): continue
        (a, var, b, oldValMat, c) = re.match(regexIn, line.rstrip()).groups()
        gc3libs.log.debug("Read variable '%s' with value '%s' ...", var, oldValMat)
        if var == varIn:
            isfound = True
            oldValMat = str2mat(oldValMat)
            if oldValMat.shape == (1,):
                newValMat = newVal
            else:
                newValMat = oldValMat
                newValMat[paraIndex] = newVal
                newValMat = mat2str(newValMat)
            gc3libs.log.debug("Will change variable '%s' to value '%s' ...", var, newValMat)
        else:
            newValMat = oldValMat
        paraFileOut.write(a + var + b + newValMat + c + '\n')
    paraFileOut.close()
    paraFileIn.close()
    # move new modified content over the old
    os.rename(path + '.tmp', path)
    if not isfound:
        gc3libs.log.critical('update_parameter_in_file could not find parameter in sepcified file')


def safe_eval(s):
    """
    Try to parse `s` as an integer; if successful, return its integer
    value.  Else try to parse it as a float, and return that value.
    Otherwise, return `s` converted to a string.

    Example::

      >>> safe_eval("42")
      42
      >>> safe_eval("42.0")
      42.0
      >>> safe_eval("forty-two")
      'forty-two'

    """
    # must attempt conversion to `int` before we try `float`,
    # since every `int` literal is also a valid `float` literal
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    return str(s)


@gc3libs.debug.trace
def str2mat(strIn):
    """
    strIn: String containing matrix
    out:   np.array with translated string elements. 
    """
    if (',' not in strIn) and (';' not in strIn):
        # single value
        return np.array([ safe_eval(strIn.strip()) ])
    elif ';' not in strIn: # vector
        # remove wrapping '[' and ']' (if any)
        try:
            start = strIn.index('[')
            end = strIn.rindex(']')
            strIn = strIn[start+1:end]
        except ValueError: # no '[' or ']'
            pass
        # a vector is a ','-separated list of values
        return np.array([ safe_eval(element.strip())
                          for element in strIn.split(',') ])
    else: # matrix
        mat = [ ]
        # remove wrapping '[' and ']' (if any)
        try:
            start = strIn.index('[')
            end = strIn.rindex(']')
            strIn = strIn[start+1:end]
        except ValueError: # no '[' or ']'
            pass
        # a matrix is a ';'-separated list of rows;
        # a row is a ','-separated list of values
        return np.array([ [ safe_eval(val) for val in row.split(',') ]
                          for row in strIn.split(';') ])


def mat2str(matIn, fmt='%.2f '):
    """
    Return a string representation of 2D array `matIn`.
    A matrix is printed as a `;`-separated list of rows,
    where each row is a `,`-separated list of values.
    All enclosed in `[` and `]`.
    """
    # admittedly, this is not that clear...
    return ('[ ' +
            str.join(";", [
                str.join(",",
                         [(fmt % val) for val in row])
                for row in matIn ])
            + ' ]')


def getParameter(fileIn, varIn, regexIn = '(\s*)([a-zA-Z0-9]+)(\s+)([a-zA-Z0-9\.\s,;\[\]\-]+)(\s*)'):
    import re
    _loop_regexps = {
        'bar-separated':(r'([a-z]+[\s\|]+)'
                         r'(\w+)' # variable name
                         r'(\s*[\|]+\s*)' # bars and spaces
                         r'([\w\s\.,;\[\]\-]+)' # value
                         r'(\s*)'),
        'space-separated':(r'(\s*)'
                           r'(\w+)' # variable name
                           r'(\s+)' # spaces (filler)
                           r'([\w\s\.,;\[\]\-]+)' # values
                           r'(\s*)'), # spaces (filler)
    }
    if regexIn in _loop_regexps.keys():
        regexIn = _loop_regexps[regexIn]
    #  print('updateParameter inputs: \n --- \n {0} \n {1} \n {2} \n {3} \n {4} \n ---'.format(fileIn, varIn, paraIndex, newVal, regexIn))
    paraFile = open(fileIn)
    lines = paraFile.readlines()
    paraFile.close()
    for ixLine, line in enumerate(lines):
        (a, var, b, paraVal, c) = re.match(regexIn, line.rstrip()).groups()
    #      print('var=', var)
        if var == varIn:
            return paraVal
    print('variable {} not in parameter file {}'.format(varIn, fileIn))

def walklevel(some_dir, level=1):
    '''
    from http://stackoverflow.com/questions/229186/os-walk-without-digging-into-directories-below
    '''
    some_dir = some_dir.rstrip(os.path.sep)
    assert os.path.isdir(some_dir)
    num_sep = some_dir.count(os.path.sep)
    for root, dirs, files in os.walk(some_dir):
        yield root, dirs, files
        num_sep_this = root.count(os.path.sep)
        if num_sep + level <= num_sep_this:
            del dirs[:]
            
def emptyFun():
    return None 


if __name__ == '__main__':   
    x = getIndex([3,3], 'lowerTr')
    for i in x:
        print(i)