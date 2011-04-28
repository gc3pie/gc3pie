#! /usr/bin/env python
#
"""
Driver script for running the `forwardPremium` application on SMSCG.
"""
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
__docformat__ = 'reStructuredText'
__version__ = '$Revision$'


# std module imports
import numpy as np
import os
import re
import shutil
import sys
import time

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript
#from gc3libs.dag import SequentialTaskCollection, ParallelTaskCollection
import gc3libs.utils

import gc3libs.debug

## auxiliary functions

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


class Squares(object):
    def __init__(self, start, stop):
        self.value = start - 1
        self.stop = stop
    def __iter__(self):
        return self
    def next(self):
        if self.value == self.stop:
            raise StopIteration
        self.value += 1
        return self.value ** 2


def getFullIndex(ixMeta, metaIndices, groupIndices, groups, paraProps, vals):
    """
    Returns current index of vals for each variable. 
    The current groupIndex is extended with values for special variables. 
    Currently these are: 
      1) group = -1 variables. 
      2) Indicator switch variables. 
    """
    # --- Establish index list ----
    # -----------------------------
    index = []
    for ixGroup, group in enumerate(np.unique(groups)):
        meta = metaIndices[ixMeta]
        groupSelector = (groups == group)
        groupVarIndices = np.where(groups == group)[0]
        if group != -1:
            groupIndex = groupIndices[group][meta[group]]
            curGroupIndex = 0
            extendedGroupIndex = []
            for groupVarIndex in groupVarIndices:
                paraProp = paraProps[groupVarIndex]
                if not paraProp == 'swIndicator':
                    extendedGroupIndex.append(groupIndex[curGroupIndex])
                    curGroupIndex += 1
                else:
                    values = vals[groupVarIndex].split(',')
                    if ixMeta > 0 and groupIndices[group][metaIndices[ixMeta - 1][group]] != groupIndex: # group has changed indices
                        extendedGroupIndex.append(0)
                    elif ixMeta == 0:
                        extendedGroupIndex.append(0)
                    else:
                        extendedGroupIndex.append(1)
            index.append(extendedGroupIndex)      
        else: # group == -1
            groupMinus1 = groups < 0
            if sum(groupMinus1) > 1:
                self.log.warning("more than one -1 variable not supported")
            values = vals[groupMinus1][0].split(',')
            nValues = len(values)
            if ixMeta >= nValues:
                index.append(int(nValues - 1))
            else:
                index.append(int(ixMeta))
    return index


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
                if '.' in str(newVal):
                    newValMat = '%.3f' % float(newVal)
                else:
                    try:
                        # try to convert to integer, and use decimal repr
                        newValMat = str(int(newVal))
                    except ValueError:
                        # then it's a string
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


## the GPremiumScript class

class GPremiumScript(SessionBasedScript):
    """
Read `.loop` files and execute the `forwardPremium` program accordingly.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = '0.2',
            # only '.loop' files are considered as valid input
            input_filename_pattern = '*.loop',
            )

    def setup_options(self):
        self.add_param("-b", "--initial", metavar="DIR",
                       dest="initial",
                       help="Include directory contents in any job's input."
                       " Use this to specify the initial guess files.")
        self.add_param("-n", "--dry-run",
                       dest = 'dryrun', action="store_true", default = False,
                       help="Take the loop for a test run")
        self.add_param("-x", "--executable", metavar="PATH",
                       dest="executable", default=os.path.join(
                           os.getcwd(), "forwardPremiumOut"),
                       help="Path to the `forwardPremium` executable binary"
                       "(Default: %(default)s)")


    def parse_args(self):
        """
        Check validity and consistency of command-line options.
        """
        if not os.path.exists(self.params.executable):
            raise gc3libs.exceptions.InvalidUsage(
                "Path '%s' to the 'forwardPremium' executable does not exist;"
                " use the '-x' option to specify a valid one."
                % self.params.executable)
        if os.path.isdir(self.params.executable):
            self.params.executable = os.path.join(self.params.executable,
                                                  'forwardPremium')
        gc3libs.utils.test_file(self.params.executable, os.R_OK|os.X_OK,
                                gc3libs.exceptions.InvalidUsage)
        

    def process_para_file(self, path_to_para_loop):
        """
        Create sets of parameter substituions for the ``forwardPremium``
        input files and yield a unique identifier for each set.  The
        "recipe" for building such sets is given in the ``para.loop``
        file pointed to by argument `path_to_para_loop`.

        In more detail, `process_para_file` is a generator function, that:
         1. parses the `para.loop` file;
         2. computes the distinct sets of substitutions that should be
            applied to ``forwardPremium`` input files; one such set of
            substitutions corresponds to a single ``forwardPremium`` run;
         3. creates and yields a "unique name" string that shall identify
            *this* particular combination of parameters and input files.

        Each generator iteration yields a pair `(jobname,
        substitutions)`, where `jobname` is a unique string and
        `substitutions` is a dictionary mapping file names (as
        specified in the ``paraFile`` column of the ``para.loop``
        file) to quadruples `(var, index, val, regex)`: into each
        input file, the variable `var` (corresponding to the group
        `paraIndex` in `regex`) is given value `val`.

        :param: `path_to_para_loop`: complete pathname of a file in
            ``para.loop`` format.

        """
        params = self._read_para(path_to_para_loop)
        num_params = len(params)
        self.log.debug("Read %d parameters from file '%s'",
                       num_params, path_to_para_loop)

        variables     = params['variables']
        indices       = params['indices']
        paraFiles     = params['paraFiles']
        paraProps     = params['paraProps']
        groups        = np.array(params['groups'], dtype = np.int16)
        groupRestrs   = params['groupRestrs']
        paraFileRegex = params['paraFileRegex']

        # XXX: why not just use `vals = np.copy(params['vals'])` ??
        vals = np.empty((num_params,), 'U100')
        for ixVals, paraVals in enumerate(params['vals']):
            vals[ixVals] = paraVals

        # remap group numbers so that they start at 0 and increase in
        # steps of 1
        groups = self._remap_groups(groups)
        num_groups = len(np.unique(groups))

        self.log.debug("Number of groups: %d", num_groups)
        self.log.debug('variables: %s' % variables)
        self.log.debug('indices: %s' % indices)
        self.log.debug('vals: %s' % vals)
        self.log.debug('paraFiles: %s' % paraFiles)
        self.log.debug('groups: %s' % groups)
        self.log.debug('groupRestrs: %s' % groupRestrs)  
        self.log.debug('paraFileRegex: %s' % paraFileRegex)

        # check parameter files for consistency
        #
        # NOTE: the weird use of booleans as array indices is a
        # feature of NumPy, see:
        # http://docs.scipy.org/doc/numpy/reference/arrays.indexing.html
        for group in groups:
          groupSelector = (groups == group)
          groupRestriction = np.unique(groupRestrs[groupSelector])
          nGroupRestriction = len(groupRestriction)
          nGroupVariables = sum(groupSelector)
          nSwIndicator = sum(paraProps[groupSelector] == 'swIndicator')
          nRelevGroupVariables = nGroupVariables - nSwIndicator
          if nGroupRestriction != 1:
              raise gc3libs.exceptions.InvalidUsage(
                  "Group restrictions '%s' are inconsistent for group '%s'"
                  % (groupRestriction, group))
          elif nRelevGroupVariables == 1 and groupRestriction[0].lower() == 'lowertr':
              raise gc3libs.exceptions.InvalidUsage(
                  "No sense in using 'lower triangular' restriction"
                  " with just one variable.")

        # Set up groups
        groupBase = []
        groupIndices = []
        metaBase = []
        for ixGroup, group in enumerate(np.unique(groups[groups >= 0])):
            # ixGroup is used as index for groups
            self.log.debug('At gpremium L200, ixGroup=%s', ixGroup)
            self.log.debug('At gpremium L201, group=%s', group)
            groupBase.append([])
            # Select vars belonging to group 'group'. Leave out switch indicator vars
            # --------------------------------
            relevGroups = (groups == group)
            swVars = (paraProps == 'swIndicator')
            groupSelector = relevGroups
            for ix in range(0, len(relevGroups)):
                if relevGroups[ix] and not swVars[ix]:
                    groupSelector[ix] = True
                else:
                    groupSelector[ix] = False
            #  -------------------------------
            groupRestr = np.unique(groupRestrs[groupSelector])
            self.log.debug('At gpremium L%d, groupRestr=%s', 492, groupRestr)
            self.log.debug('At gpremium L%d, groupSelector=%s', 493, groupSelector)
            if len(groupRestr) != 1:
                raise gc3libs.exceptions.InvalidUsage(
                    "Groups have different restrictions")
            for groupVals in vals[groupSelector]:
                values = str2vals(groupVals)
                groupBase[group].append(len(np.array(values)))
                self.log.debug('At gpremium L%d, groupvals=%s', 500, values)
            groupIndices.append(list(getIndex(groupBase[ixGroup], groupRestr)))
            self.log.debug('At gpremium L%d, groupIndices=%s', 503, groupIndices[ixGroup])
            metaBase.append(len(groupIndices[ixGroup]))

        # Combine groups without restriction
        metaIndices = list(getIndex(metaBase, None))
        nMetaIndices = len(metaIndices)

        self.log.debug('Summary after establishing groups:')
        self.log.debug('  groupbase: %s', groupBase)
        self.log.debug('  groupindices: %s', groupIndices)
        self.log.debug('  metabase: %s', metaBase)
        self.log.debug('  metaind: %s', metaIndices)

        self.log.debug("Starting enumeration of independent runs...")
        for ixMeta, meta in enumerate(metaIndices):
            self.log.debug("Loop iteration(ixMeta) %d of %d (%.2f%%)",
                           ixMeta+1, nMetaIndices,
                           100.0 * ((1+ixMeta) / nMetaIndices))

            index = getFullIndex(ixMeta, metaIndices,
                                 groupIndices, groups, paraProps, vals)
            self.log.debug("Index before flattening: %s", index)
            index = list(flatten(index))
            self.log.debug('Flattened index: %s', index)

            for ixVar in range(0, len(variables)):
                self.log.debug('variable #%d is %s', ixVar, variables[ixVar])

            runDescription = os.path.basename(path_to_para_loop)[:-5]
            substs = gc3libs.utils.defaultdict(list)
            for ixVar, var in enumerate(variables):
                self.log.debug('variable: %s', variables[ixVar])
                var = variables[ixVar]
                group = groups[ixVar]
                paraFile = paraFiles[ixVar]
                adjustIndex = indices[ixVar]
                val = extractVal(ixVar, vals, index)
                regex = paraFileRegex[ixVar]
                paraIndex = str2tuple(indices[ixVar])
                self.log.debug('paraIndex: %s', paraIndex)
                substs[paraFile].append((var, val, paraIndex, regex))
                if (group >= 0) and paraProps[ixVar] != 'swIndicator': 
                    if ixVar < len(variables):
                        runDescription += '_'
                    runDescription += '%s=%s' % (var, val)
            yield (runDescription, substs)
        

    def new_tasks(self, extra):
        inputs = self._search_for_input_files(self.params.args)

        for path in inputs:
            para_loop = path
            path_to_base_dir = os.path.dirname(para_loop)
            self.log.debug("Processing loop file '%s' ...", para_loop)
            for jobname, substs in self.process_para_file(para_loop):
                self.log.debug("Job '%s' defined by substitutions: %s.",
                               jobname, substs)
                executable = os.path.basename(self.params.executable)
                inputs = { self.params.executable:executable }
                # make a "stage" directory where input files are collected
                path_to_stage_dir = self.make_directory_path(
                    self.params.output, jobname, path_to_base_dir)
                input_dir = path_to_stage_dir #os.path.join(path_to_stage_dir, 'input')
                gc3libs.utils.mkdir(input_dir)
                prefix_len = len(input_dir) + 1
                # 1. files in the "initial" dir are copied verbatim
                if self.params.initial is not None:
                    gc3libs.utils.copytree(self.params.initial, input_dir)
                # 2. apply substitutions to parameter files
                for (path, changes) in substs.iteritems():
                    for (var, val, index, regex) in changes:
                        update_parameter_in_file(os.path.join(input_dir, path),
                                                 var, index, val, regex)
                # 3. build input file list
                for dirpath,dirnames,filenames in os.walk(input_dir):
                    for filename in filenames:
                        # cut the leading part, which is == to path_to_stage_dir
                        remote_path = os.path.join(dirpath[prefix_len:], filename)
                        inputs[os.path.join(dirpath, filename)] = remote_path
                # all contents of the `output` directory are to be fetched
                outputs = { 'output/':'' }
                kwargs = extra.copy()
                kwargs['stdout'] = 'forwardPremiumOut.log'
                kwargs['join'] = True
                kwargs['output_dir'] = os.path.join(path_to_stage_dir, 'output')
                # hand over job to create
                yield (jobname, gc3libs.Application,
                       ['./' + executable, [], inputs, outputs], kwargs) 

    ##
    ## Internal methods
    ##

    def _remap_groups(self, groups):
        """
        Remap group numbers so that:
          1. they start at 0;
          2. there's no gap between two consecutive group numbers.
        """
        group_nrs_map = dict()
        max_group_nr_seen = -1
        for group_nr in np.unique(groups):
            max_group_nr_seen += 1
            group_nrs_map[group_nr] = max_group_nr_seen
        def remap_group_nrs(nr):
            return group_nrs_map[nr]
        return np.apply_along_axis(np.vectorize(remap_group_nrs),
                                   0, groups)

    # format of the `.loop` files
    #  1. comments lines have `#` as first non-whitespace char
    _loop_comment_re = re.compile(r'^ \s* (\#|$)', re.X)
    #  2. columns are separated by 3 or more spaces
    _loop_colsep_re = re.compile(r'\s\s\s+')
    # 3. allowed regexps; each regexp must have exactly 5 groups
    # (named 'a', 'var', 'b', 'val', and 'c'), of which only
    # 'var' and 'val' are signficant; the other ones are copied verbatim.
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

    # -- Read para.loop  --
    def _read_para(self, path):
        """
        Read the ``para.loop`` file at `path` and return array of
        parameters.

        Argument `path` can be the full path to a file, which is assumed
        to be in ``para.loop`` syntax, or the path to a directory, in
        which case the the file ``path/para.loop`` is read.
        """
        if os.path.isdir(path):
            path = os.path.join(path, 'para.loop')

        paraLoopFile = open(path, 'r')

        # skip first line (header)
        paraLoopFile.readline()

        # map column name to NumPy type
        dt = {
            'names':[
                'variables',
                'indices',
                'paraFiles',
                'paraProps',
                'groups',
                'groupRestrs',
                'vals',
                'paraFileRegex',
                ],
            'formats':[
                'U100', # variables
                'U100', # indices
                'U100', # paraFiles
                'U100', # paraProps
                'U100', # groups
                'U100', # groupRestrs
                'U100', # vals
                'U100', # paraFileRegex
                ],
            }

        data = []
        for line in paraLoopFile:
            if GPremiumScript._loop_comment_re.match(line):
                continue
            columns = list(GPremiumScript._loop_colsep_re.split(line.rstrip()))
            columns[7] = self._loop_regexps[columns[7]]
            columns = tuple(columns)
            self.log.debug("_read_para: got columns: %s", columns)
            data.append(np.array(columns, dtype=dt))
        params = np.array(data, dtype=dt)
        paraLoopFile.close()
        
        # by default numpy sorts capital letters with higher priority.
        # Sorting gives me a unique position for a variable in each input vector! 
        ind = np.lexsort((lower(params['paraFiles']),
                          lower(params['variables']),
                          params['groups']),
                         axis = 0)
        return params[ind]


## run script

if __name__ == '__main__':
    GPremiumScript().run()

