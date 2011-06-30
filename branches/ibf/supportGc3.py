    #! /usr/bin/env python
#

# Copyright (C) 2011 University of Zurich. All rights reserved.
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
__version__ = '$Revision$'
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>, Benjamin Jonen <benjamin.jonen@bf.uzh.ch>'
# summary of user-visible changes
__changelog__ = """

"""
__docformat__ = 'reStructuredText'

import gc3libs.debug
import numpy as np
import re, os

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


class getIndex(object):
    """
    Iterator that yields loop indices for an arbitrary nested loop.
    Inputs: base - array of elements in each dimension
    Output: loopIndex - array of current loop index
    """    

    def __init__(self, base, restr = None):
        if isinstance(base, int):
            baseList = []
            baseList.append(base)
            base = baseList
        self.base = np.array(base)
        self.loopIndex = np.zeros(len(self.base), dtype=np.int16)
        self.iteration = 1
        if isinstance(restr, np.ndarray):
            self.restr = restr[0].lower()
        elif restr:      
            self.restr = restr.lower()
        else:
            self.restr = restr

    def __iter__(self):
        return getIndex(self.base, self.restr)

    def increment(self):
        for ix in xrange(len(self.base) - 1, -1, -1):
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
        for ix in xrange(0, len(self.base) - 1) :
            if self.loopIndex[ix] != self.loopIndex[ix + 1]:
                return True
        return False    

    def next(self):
        if self.iteration > 1:
            self.increment()
        if list(self.loopIndex) == [0] * len(self.base) and self.iteration > 1:
            raise StopIteration
        self.iteration += 1

        if self.restr == 'lowertr':
            skip = self.lowerTr()
        elif self.restr == 'diagnol':
            skip = self.diagnol()
        elif self.restr == None or self.restr == 'none':
            skip = False
        else:
            raise gc3libs.exceptions.InvalidArgument(
                "Unknown restriction '%s'" % self.restr)

        if skip == True:
            return self.next()
        else:
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
    if '.' in str(newVal):
        newValMat = '%.10f' % float(newVal)
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
    paraFileIn = open(path, 'r')
    paraFileOut = open(path + '.tmp', 'w')
    for line in paraFileIn:
        (a, var, b, oldValMat, c) = re.match(regexIn, line.rstrip()).groups()
        gc3libs.log.debug("Read variable '%s' with value '%s' ...", var, oldValMat)
        if var == varIn:
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
