# make compatible with python2
from __future__ import print_function

import sys, copy
import numpy as np
from wrapLogbook import wrapLogger
import os.path
import re
import shutil

verb = 'INFO'
logger = wrapLogger(loggerName = 'createTableLog', streamVerb = verb, logFile = None)

def rmFilesAndFolders(top):
  # works like rm -r *
  # taken from http://stackoverflow.com/questions/303200/how-do-i-remove-delete-a-folder-that-is-not-empty-with-python
  # Delete everything reachable from the directory named in 'top',
  # assuming there are no symbolic links.
  # CAUTION:  This is dangerous!  For example, if top == '/', it
  # could delete all your disk files.
  import os
  for root, dirs, files in os.walk(top, topdown=False):
    for name in files:
      os.remove(os.path.join(root, name))
    for name in dirs:
      os.rmdir(os.path.join(root, name))
  return


def lower(npStrAr):
  print('here')
  npStrArNew = copy.deepcopy(npStrAr)
  for ixEle, ele in enumerate(npStrAr):
    npStrArNew[ixEle] = ele.lower()
  return npStrArNew

def flatten(lst):
  for elem in lst:
    if type(elem) in (tuple, list):
      for i in flatten(elem):
        yield i
    else:
      yield elem


def getFilePath(fileIn): # use os.path.split instead
  if re.match('/.*', fileIn): # abs. path
    return re.match('/.*/(?![.*/.*])', fileIn).group(0)
  else:                       # rel. path
    return os.getcwd()

def str2tuple(strIn):
  """Convert tuple-like strings to real tuples.
  eg '(1,2,3,4)' -> (1, 2, 3, 4)
  """
  import re
  if re.search('\(', strIn) == None: 
    return eval('(' + strIn.strip() + ',' + ')')
  strIn = strIn.strip()
  items = strIn[1:-1] # removes the leading and trailing brackets
  items = items.split(',')
  L = [int(x.strip()) for x in items] # clean up spaces, convert to ints
  return tuple(L)


def readTable2List(inFile, sep = '  '):
  import re
  f = open(inFile)
  lines = f.readlines()
  table = []
  for line in lines:
    line = re.split(sep + '+', line.rstrip())
    table.append(line)
  printTable(table, '{0:25s}')
  #print(empTarget)
  #print(empTarget['Cor'][()][()])

#def mergeLists(listIn):

def printDict(dictIn, keysIn = None, width = 10, prec = 2):
  print('\n\n')
  if keysIn:
    keys = keysIn
  else:
    keys = list(dictIn.keys())
  nEles = 0
  for key in keys:
    keyLen = len(dictIn[key])
    if keyLen > nEles:
      nEles = keyLen

  for ixRow in range(0, nEles):
    if ixRow == 0:
      for key in keys:
        #fmt = '{:' + str(width) + 's' + '}'
        #print(fmt.format(key), end = '')
        prStr = key.rjust(width, ' ')
        print(prStr, end = '')
      print('')
    for key in keys:
      if nEles > 1:
        try: 
          ele = dictIn[key][ixRow]
        except IndexError:
          ele = None
      else:
        ele = dictIn[key]
      if isinstance(ele, list):
        prStr = ''.join(ele)
        prStr = prStr.rjust(width, ' ')
        #fmt = '{:' + str(width) + 's' + '}'
        print(prStr, end = '')  
      elif is_float(ele):
        fmt = '{:' + str(width) + '.' + str(prec) + 'f}'
        print(fmt.format(float(ele)), end = '')
      elif is_int(ele):
        fmt = '{:' + str(width) + 'd' + '}'
        print(fmt.format(int(ele)), end = '')
      elif isinstance(ele, str):
        prStr = ele.rjust(width, ' ')
        print(prStr, end = '')
      else:
        fmt = '{:' + str(width) + 's' + '}'
        print(fmt.format(ele), end = '')  
    print('')



def printTable(table, width = 10, prec = 2):
  for line in table:
    for ele in line:
#      ele = str(ele)
      if is_int(ele):
        fmt = '{:' + str(width) + 'd' + '}'
        print(fmt.format(int(ele)), end = '')
      elif is_float(ele):
        fmt = '{:' + str(width) + '.' + str(prec) + 'f}'
        print(fmt.format(float(ele)), end = '')
      else:
        fmt = '{:' + str(width) + 's' + '}'
        print(fmt.format(ele), end = '')    
    print('')

def readTable2Dict(inFile, sep = '  '):
  import re
  table = {}
  f = open(inFile)
  header = f.readline().rstrip()
  header = re.split(sep + '+', header)
  for ele in header:
    table[ele] = []
  lines = f.readlines()

  for line in lines:
    line = re.split(sep + '+', line.rstrip())
    for ixEle,ele in enumerate(line):
      table[header[ixEle]].append(line[ixEle])
  del table['']
  #keys = list(table.keys())
  #keys.sort()
  print(table.keys())
  #for ele in table.keys():
    #print(table[ele])
  x = [(k,v) for k,v in table.items()]
  y = []
  for a,b in x:
    y.append(b)
  #print(y)
  z = transposed(y)
  #print(list(z))
  for line in z:
    for ele in line:
      print('{0:15s}'.format(ele), end = '')
    print('')

def table_list2Dict(tableIn):
  dictOut = {}
  keys = tableIn[0]
  dictOut = dictOut.fromkeys(list(keys))

  for key in dictOut:
    dictOut[key] = []
  for row in tableIn[1:]:
    for ixEle, ele in enumerate(row):
      dictOut[keys[ixEle]].append(ele)
  dictOut['keys'] = keys  
  return dictOut

def table_mergeDicts(tableIn1, tableIn2, key1, key2, var, offset):
  import sys
  tableIn1[var] = []
  table2KeyVals = tableIn2[key2]
  for keyVal in tableIn1[key1]:
    if table2KeyVals.count(keyVal) > 1:
      print('not a one to one merge. Input ambigiuos. aborting..')
      sys.exit()
    index = table2KeyVals.index(keyVal)
    tableIn1[var].append(tableIn2[var][index])
  tableIn1['keys'].insert(offset, var)
  return tableIn1

def table_listAppend(table1, table2):
  for key in table2:
    if key == 'keys': continue
    for ele in table2[key]:
      table1[key].append(ele)
  return table1




def importMatlabFile(fileIn, struct_as_record = True):
  import scipy.io as sio
  matLabIn = sio.loadmat(fileIn, squeeze_me=False, struct_as_record=struct_as_record)
  return matLabIn
  #empTarget = matLabIn['empTarget']
  #print(empTarget)





def adjustMatlabImport(nestedMatObj):
  '''
    Function to take a matlab imported structure and trim it down. 
  '''
  import numpy as np
  nObj = nestedMatObj
  outList = []
  primitiveTypes = [float, int, str, np.float64]
  while len(nObj) == 1:
    nObj = nObj[0]
  for ele in nObj:
    col = []
    while len(ele) == 1:
      ele = ele[0]
    if isinstance(ele, str):
      col.append(ele)
    else:
      for ele2 in ele:
        while not type(ele2) in primitiveTypes and len(ele2) == 1:
          ele2 = ele2[0]
        col.append(ele2)
    outList.append(col)
  return outList

def flattenMatStruct(nestedMatObj):
  nestedMatObj = extractList(nestedMatObj)
  keys = nestedMatObj.__dict__.keys()
  for key in keys:
    ele = nestedMatObj.__getattribute__(key)
    nestedMatObj.__setattr__(key, extractList(ele))
  return nestedMatObj

def extractList(structure):
  ''' 
    Recursive function to extract lowest level from a structure. 
  '''
  import numpy as np
  import scipy
  struct = structure
  outList = []
  primitiveTypes = [np.unicode_, float, int, str, np.float64, np.str, np.str_, np.uint8]
  try: 
    primitiveTypes.append(scipy.io.matlab.mio5_params.mat_struct)  
  except:
    pass
  if type(struct) in primitiveTypes: return struct
  while len(struct) == 1:
    struct = struct[0]
    if type(struct) in primitiveTypes: 
      return struct  
  for ele in struct:
    outList.append(extractList(ele))
  return outList


def transposed(lists):
  if not lists: return []
  return map(lambda *row: list(row), *lists)

def is_float(s):
  try:
    float(s)
    return True
  except (ValueError, TypeError):
    return False

def is_int(s):
  try:
    int(s)
    return True
  except (ValueError, TypeError):
    return False

def getUniqueList(seq):
  seen = set()
  seen_add = seen.add
  return [ x for x in seq if x not in seen and not seen_add(x)]

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
    logger.debug("Read variable '%s' with value '%s' ...", var, oldValMat)
    if var == varIn:
      isfound = True
      oldValMat = str2mat(oldValMat)
      if oldValMat.shape == (1,):
        newValMat = str(newVal)
      else:
        newValMat = oldValMat
        newValMat[paraIndex] = newVal
        newValMat = mat2str(newValMat)
      logger.debug("Will change variable '%s' to value '%s' ...", var, newValMat)
    else:
      newValMat = oldValMat
    paraFileOut.write(a + var + b + newValMat + c + '\n')
  paraFileOut.close()
  paraFileIn.close()
  # move new modified content over the old
  os.rename(path + '.tmp', path)
  if not isfound:
    logger.critical('update_parameter_in_file could not find parameter in sepcified file')


class myArrayPrint(object):
  '''
    Pretty prints numpy arrays. Used with np.set_string_function: http://docs.scipy.org/doc/numpy/reference/generated/numpy.set_string_function.html#numpy.set_string_function
    Example::
    
  '''
  def __init__(self, width = 10, prec = 5):
    self.width = str(width)
    self.prec = str(prec)    

  def __call__(self, a):
    # Select format
    if a.dtype == np.int:
      #fmt = '%^' + self.width + 'd'
      fmt = '{0:' + self.width + 'd' + '}'
    elif a.dtype == np.float:
      #fmt = '%^' + self.width + '.' + self.prec + 'f'
      fmt = '{0:>' + self.width + '.' + self.prec + 'f' + '}'
      
    # Construct string
    if a.ndim == 1:
      s = '[ '
      for ele in a:
        #s += fmt % ele
        s += fmt.format(ele)
      s += ' ]'
    elif a.ndim == 2:
      s = '['
      for ixRow, row in enumerate(a):
        if ixRow == 0:
          s += '['
        else:
          s += '[ '
        for ele in row:
         # s += fmt % ele
          s += fmt.format(ele)
        if ixRow < len(a) - 1:
          s += '],\n '
        else: 
          s += ']'
      s += ']'
    
    return s

def fillInputDir(baseDir, input_dir):
    '''
      Copy folder /input and all files in the base dir to input_dir. 
      This is slightly more involved than before because we need to 
      exclude the markov directory which contains markov information
      for all country pairs. 
    '''
    import glob
    inputFolder = os.path.join(baseDir, 'input')
    shutil.copytree(inputFolder , os.path.join(input_dir, 'input'))
    #      gc3libs.utils.copytree(inputFolder , os.path.join(input_dir, 'input'))
    filesToCopy = glob.glob(baseDir + '/*')
    for fileToCopy in filesToCopy:
        if os.path.isdir(fileToCopy): continue
        shutil.copy(fileToCopy, input_dir)




if __name__ == '__main__':
  print('start') 
  import doctest
  doctest.testmod()
  #myArrayPrint(np.array([1.00, 3.015]))
  pprintFun = myArrayPrint(20, 7)
  s = pprintFun(np.array([[1.00, 3.015], [3.21, 84.23]]))
  print(s)
  sys.exit()
  inFile = '/mnt/shareOffice/ForwardPremium/Results/2011-02-11-19-34-11LoopOver[EA,EB]_USJP_nice/overviewSimu'
  #readTable2Dict(inFile)
  #importMatlabFile('/mnt/shareOffice/MatlabMethods/MarkovChainApproximation/MomentMatching/targetsInOut/Base_basket_~JP~US_CtryJP.mat')
  #readTable2List(inFile)
  #readSlUIP()
  #d = {'t': 1, 'a': 3}
  #printDict(d)
  npar = np.array(['dkKK','kdKkd'])
  x = lower(npar)
  print(x)

  print('\ndone')
  
