#! /usr/bin/env python2.6
#

"""
Script for running over different parameter combinations. 
"""

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
import re, os
import numpy as np
from supportGc3 import lower, flatten, str2tuple, getIndex, extractVal, str2vals
from supportGc3 import format_newVal, update_parameter_in_file, safe_eval, str2mat, mat2str, getParameter

import shutil

import logbook, sys
from supportGc3 import wrapLogger

class paraLoop(object):
    '''
      Main class implementing a general loop algorithm. Loop over an arbitrary number of variables and values. 
      Additionally the script allows for grouping variables and then specifying a restricted set of parameter
      combinations thereby avoiding to go through all combinations. 
      Should be subclassed together with a SessionBasedScript. 
    '''
    
    
    def __init__(self, verbosity):
        self.logger = wrapLogger(loggerName = __name__ + 'logger', streamVerb = verbosity.upper(), logFile = __name__ + '.log')

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
        self.logger.debug("Read %d parameters from file '%s'" % (num_params, path_to_para_loop))

        variables     = params['variables']
        indices       = params['indices']
        paraFiles     = params['paraFiles']
        paraProps     = params['paraProps']
        groups        = np.array(params['groups'], dtype = np.int16)
        groupRestrs   = params['groupRestrs']
        paraFileRegex = params['paraFileRegex']

        # XXX: why not just use `vals = np.copy(params['vals'])` ??
        vals = np.copy(params['vals'])
        #vals = np.empty((num_params,), 'U100')
        #for ixVals, paraVals in enumerate(params['vals']):
            #vals[ixVals] = paraVals

        # remap group numbers so that they start at 0 and increase in
        # steps of 1
        groups = self._remap_groups(groups)
        num_groups = len(np.unique(groups))

        self.logger.debug("Number of groups: %d" % num_groups)
        self.logger.debug('variables: %s' % variables)
        self.logger.debug('indices: %s' % indices)
        self.logger.debug('vals: %s' % vals)
        self.logger.debug('paraFiles: %s' % paraFiles)
        self.logger.debug('groups: %s' % groups)
        self.logger.debug('groupRestrs: %s' % groupRestrs)  
        self.logger.debug('paraFileRegex: %s' % paraFileRegex)

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
            self.logger.debug('At gpremium L200, ixGroup=%s' % ixGroup)
            self.logger.debug('At gpremium L201, group=%s'% group)
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
            self.logger.debug('At gpremium L%d, groupRestr=%s' % (492, groupRestr))
            self.logger.debug('At gpremium L%d, groupSelector=%s' % (493, groupSelector))
            if len(groupRestr) != 1:
                raise gc3libs.exceptions.InvalidUsage(
                    "Groups have different restrictions")
            for groupVals in vals[groupSelector]:
                values = str2vals(groupVals)
                groupBase[group].append(len(np.array(values)))
                self.logger.debug('At gpremium L%d, groupvals=%s' % (500, values))
            groupIndices.append(list(getIndex(groupBase[ixGroup], groupRestr)))
            self.logger.debug('At gpremium L%d, groupIndices=%s' % (503, groupIndices[ixGroup]))
            metaBase.append(len(groupIndices[ixGroup]))

        # Combine groups without restriction
        metaIndices = list(getIndex(metaBase, None))
        nMetaIndices = len(metaIndices)

        self.logger.debug('Summary after establishing groups:')
        self.logger.debug('  groupbase: %s' % groupBase)
        self.logger.debug('  groupindices: %s' % groupIndices)
        self.logger.debug('  metabase: %s' % metaBase)
        self.logger.debug('  metaind: %s' % metaIndices)

        self.logger.debug("Starting enumeration of independent runs...")
        for ixMeta, meta in enumerate(metaIndices):
            self.logger.debug("Loop iteration(ixMeta) %d of %d (%.2f%%)" %
                           (ixMeta+1, nMetaIndices,
                           100.0 * ((1+ixMeta) / nMetaIndices)))

            index = self.getFullIndex(ixMeta, metaIndices,
                                      groupIndices, groups, paraProps, vals)
            self.logger.debug("Index before flattening: %s" % index)
            index = list(flatten(index))
            self.logger.debug('Flattened index: %s' % index)

            for ixVar in range(0, len(variables)):
                self.logger.debug('variable #%d is %s' % (ixVar, variables[ixVar]))

            runDescription = 'para'# os.path.basename(path_to_para_loop)[:-5]
            substs = gc3libs.utils.defaultdict(list)
            for ixVar, var in enumerate(variables):
                self.logger.debug('variable: %s' % variables[ixVar])
                var = variables[ixVar]
                group = groups[ixVar]
                paraFile = paraFiles[ixVar]
                adjustIndex = indices[ixVar]
                val = format_newVal(extractVal(ixVar, vals, index))
                regex = paraFileRegex[ixVar]
                paraIndex = str2tuple(indices[ixVar])
                self.logger.debug('paraIndex: %s' % paraIndex)
                substs[paraFile].append((var, val, paraIndex, regex))
                if (group >= 0) and paraProps[ixVar] != 'swIndicator': 
                    if ixVar < len(variables):
                        runDescription += '_'
                    runDescription += '%s=%s' % (var, val)
            yield (runDescription, substs)



    @staticmethod
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
                    self.logger.warning("more than one -1 variable not supported")
                values = vals[groupMinus1][0].split(',')
                nValues = len(values)
                if ixMeta >= nValues:
                    index.append(int(nValues - 1))
                else:
                    index.append(int(ixMeta))
        return index

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
        
        data = []
        overallMaxFieldLen = 0
        for line in paraLoopFile:
            if self._loop_comment_re.match(line):
                continue
            columns = list(self._loop_colsep_re.split(line.rstrip()))
            columns[7] = self._loop_regexps[columns[7]]
            columns = tuple(columns)
            self.logger.debug("_read_para: got columns: %s" % (str(columns)))
            maxFieldLen = max(map(len, columns))
            if maxFieldLen > overallMaxFieldLen: 
                overallMaxFieldLen = maxFieldLen
#            data.append(np.array(columns, dtype=dt))
            data.append(columns)
            
        paraLoopFile.close()            
        
        # map column name to NumPy type
        fieldFormat = 'U' + str(overallMaxFieldLen)
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
                # U for unicode, see http://docs.scipy.org/doc/numpy/reference/arrays.dtypes.html
                fieldFormat, # variables
                fieldFormat, # indices
                fieldFormat, # paraFiles
                fieldFormat, # paraProps
                fieldFormat, # groups
                fieldFormat, # groupRestrs
                fieldFormat, # vals
                fieldFormat, # paraFileRegex
                ],
        }    
        params = []
        for columns in data:
            params.append(np.array(columns, dtype=dt))
        params = np.array(params, dtype=dt)


        # by default numpy sorts capital letters with higher priority.
        # Sorting gives me a unique position for a variable in each input vector! 
        ind = np.lexsort((lower(params['paraFiles']),
                          lower(params['variables']),
                          params['groups']),
                         axis = 0)
        return params[ind]

    def writeParaLoop( self, variables, indices = None, paraFiles =  None, paraProps = None, groups = None, groupRestrs = None, paraFileRegex = None, vals = None, desPath = 'para.loopTmp'):
        paraLoopFile = open(desPath, 'w')
        print >> paraLoopFile, '%25s %25s %25s %25s %25s %25s %25s %25s\n' % ('variables', 'Index', 'ParameterFile', 'paraProp', 'Group', 'groupRestrs', 'vals', 'ParaFileRegex')
    
        Nvariables = len(variables)
        if Nvariables == 0: 
            print 'No variables specified'
            sys.exit()
    
        if indices == None:
            indices = [ '(0)' ] * Nvariables
    
        if paraProps == None:
            paraProps = [ 'None' ] * Nvariables
    
        if groups == None:
            groups = [ '0' ] *  Nvariables
    
        if groupRestrs == None:
            groupRestrs = [ 'none' ] * Nvariables 
    
        if paraFileRegex == None:
            paraFileRegex = [ 'bar-separated' ] * Nvariables
    
        if paraFiles == None:
            paraFiles =  [ 'input/parameters.in' ] * Nvariables 
    
    
        for ixVar, variable in enumerate(variables):
        #  print >> paraLoopFile, '%-25s %-25s %-25s %-25s %-25s %-25s %-25s %-25s\n' % (variables[ixVar], indices[ixVar], paraFiles[ixVar], paraProps[ixVar], groups[ixVar], groupRestrs[ixVar], vals[ixVar], paraFileRegex[ixVar])
            print >> paraLoopFile, '%s    %s    %s    %s    %s    %s    %s    %s' % (variables[ixVar], indices[ixVar], paraFiles[ixVar], paraProps[ixVar], groups[ixVar], groupRestrs[ixVar], vals[ixVar], paraFileRegex[ixVar])
    
        return desPath




