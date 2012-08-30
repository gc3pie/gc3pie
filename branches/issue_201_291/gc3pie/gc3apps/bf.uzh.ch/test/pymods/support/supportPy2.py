import sys, copy
import numpy as np

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
  printTable(table, '%25s')
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
        sys.stdout.write(prStr)
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
        sys.stdout.write(prStr)
      elif is_float(ele):
        fmt = '%' + str(width) + '.' + str(prec) + 'f'
        sys.stdout.write(fmt % (float(ele)))
      elif is_int(ele):
        fmt = '%' + str(width) + 'd'
        sys.stdout.write(fmt % (int(ele)))
      elif isinstance(ele, str):
        prStr = ele.rjust(width, ' ')
        sys.stdout.write(prStr)
      else:
        fmt = '%' + str(width) + 's'
        sys.stdout.write(fmt%(ele))
    print('')
      
  
    
def printTable(table, width = 10, prec = 2):
  for line in table:
    for ele in line:
#      ele = str(ele)
      if is_int(ele):
        fmt = '%' + str(width) + 'd'
        sys.stdout.write(fmt%(int(ele)))
      elif is_float(ele):
        fmt = '%' + str(width) + '.' + str(prec) + 'f'
        sys.stdout.write(fmt%(float(ele)))
      else:
        fmt = '%' + str(width) + 's'
        sys.stdout.write(fmt%(ele))
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
      sys.stdout.write('%15s'%(ele))
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
  primitiveTypes = [float, int, str, np.float64, np.str, np.str_, scipy.io.matlab.mio5_params.mat_struct, np.uint8]
  if type(struct) in primitiveTypes: return struct
  while len(struct) == 1:
    struct = struct[0]
    if type(struct) in primitiveTypes: return struct  
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
  
def getParameter(fileIn, varIn, regexIn = '(\s*)([a-zA-Z0-9]+)(\s+)([a-zA-Z0-9\s,;\[\]\-]+)(\s*)'):
  import re
#  print('updateParameter inputs: \n --- \n {0} \n {1} \n {2} \n {3} \n {4} \n ---'.format(fileIn, varIn, paraIndex, newVal, regexIn))
  paraFile = open(fileIn)
  lines = paraFile.readlines()
  paraFile.close()
  for ixLine, line in enumerate(lines):
    (a, var, b, paraVal, c) = re.match(regexIn, line.rstrip()).groups()
#    print('var=', var)
    if var == varIn:
      return paraVal
  print('variable %s not in parameter file %s' % (varIn, fileIn))

      
      
if __name__ == '__main__':
  print('start') 
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

  
