#!/usr/bin/env python

if __name__ == "__main__" and __package__ is None:
  import sys
  mod = __import__('pymods')
  sys.modules["pymods"] = mod
  __package__ = "pymods"

import numpy as np
import re
import tempfile
import filecmp
from .support.getIndexPy2 import getIndex
from .support.supportPy2 import flatten, str2tuple, lower
from .support.log import logger, initialize_logging
from optparse import OptionParser
import sys, os, shutil, time, datetime, subprocess, threading
import Queue as queue
import distutils.dir_util, glob

# print python version
print('python version is: ')
print(sys.version)

# Establish file logging
loopTmpFolder = 'tmp/'
logFile = loopTmpFolder  + '/loop.log'
withLog = logger(logFile)

def loop(args = None):
  print('start loop')
  #args = ['-f', "/home/benjamin/workspace/forwardpremium/", '-d']
  
  parser = OptionParser()
  parser.add_option("-f", "--folder", dest = 'confFolder', action = 'store', help = 'Specify folder containing config files')
  parser.add_option("-d", "--dryrun", dest = 'dryrun', action="store_true" ,default = False, help="Take the loop for a test run")
  if args: 
    print('args overwritten')
    print(args)
    (options, args) = parser.parse_args(args)
  else: 
    (options, args) = parser.parse_args()
  opts = options.__dict__
  print(opts)
  print('\ndryrun is ' + str(opts['dryrun']) + '\n')
    #parser.print_help()
    
  ## Setup logger format and output locations
 # withLog2 = initialize_logging(options)
  
  ### Examples
  #withLog2.error("This is an error message.")
  #withLog2.info("This is an info message.")
  #withLog2.debug("This is a debug message.")
    
    
    
  # Create a temporary folder to store the parameter files for all sessions
  if not os.path.exists(loopTmpFolder):
    os.mkdir(loopTmpFolder)
    
  # Store/restore old config directory
  
  if opts['confFolder'] != None:
    confFile = open(os.path.join(loopTmpFolder, 'recentConfFolders'),'a')
    confFile.write("%s\n" % opts['confFolder'])
    confFile.close()
  elif opts['confFolder'] == None:
    try:
      confFile = open(os.path.join(loopTmpFolder, 'recentConfFolders'),'r')
    except IOError:
      print('There is no recentConfFolders file. Please specify conf folder with --folder option. ')
      sys.exit()
    confDirs = confFile.readlines()
    # Take last entry from config file history
    opts['confFolder'] = confDirs[-1].rstrip()
    
    print('\nconfFolder:')
    print(opts['confFolder'])
    print('\n')
   
  generalConfPath2File = os.path.join(opts['confFolder'], 'general.loop')
  paraConfPath2File = os.path.join(opts['confFolder'], 'para.loop')
    
  
  # -- Read para.loop  --
  paraLoopFile = open(paraConfPath2File)
  paraLoopFile.readline()

  lines = paraLoopFile.readlines()
  dt = dtype={'names':['variables', 'indices', 'paraFiles', 'paraProps', 'groups', 'groupRestrs', 'vals', 'paraFileRegex'],
              'formats':['U100', 'U100', 'U100', 'U100', 'U100', 'U100', 'U100', 'U100']}
  params = []
  for ixLine, line in enumerate(lines):
    if line == '\n': 
      continue
    if re.match('\s*#.*', line): continue
    line = line.rstrip()
    line = re.split('\s\s\s+', line.rstrip())
    line = np.array(tuple(line), dtype = dt)
    params.append(line)
    group = 3
  params = np.array(params, dtype = dt)
  print('params \n', params)

  # by default numpy sorts capital letters with higher priority.
  # Sorting gives me a unique position for a variable in each input vector! 
  ind = np.lexsort((lower(params['paraFiles']), lower(params['variables']), params['groups']), axis = 0)
  params = params[ind]


 # sys.exit()

  nParams = len(params)
  withLog.write('number of read parameters:%d\n' % nParams)

  variables     = params['variables']
  indices       = params['indices']
  paraFiles     = params['paraFiles']
  paraProps     = params['paraProps']
  groups        = np.array(params['groups'], dtype = np.int16)
  groupRestrs   = params['groupRestrs']
  paraFileRegex = params['paraFileRegex']

  vals = np.empty((nParams,), 'U100')
  for ixVals, paraVals in enumerate(params['vals']):
    vals[ixVals] = paraVals


 # sys.exit()

  nGroups = len(np.unique(groups))
  print('nGroups', nGroups)

  # Reset groups to start at zero
  minGroups = min(groups[groups >= 0])
  if minGroups != 0:
    groups[groups >= 0] = groups[groups >= 0] - minGroups
    
  # Cut distance between group identifiers down to exactly one. 
  if len(groups[groups >= 0]) > 1:
    for ixGroup in range(0, len(groups[groups >= 0]) - 2 ):
      while groups[groups>=0][ixGroup + 1] - groups[groups>=0][ixGroup] >1:
        groups[groups>=0][ixGroup + 1] -= 1

  print(groups)
#  sys.exit()

  withLog.write('variables: %s\n' % variables)
  withLog.write('indices: %s\n' % indices)
  withLog.write('vals: %s\n' % vals)
  withLog.write('paraFiles: %s\n' % paraFiles)
  withLog.write('groups\n' % groups)
  withLog.write('groupRestrs\n' % groupRestrs)  
  withLog.write('paraFileRegex: %s\n' % paraFileRegex)


#  sys.exit()


  # Read general.loop  
  loopConfFile = open(generalConfPath2File)
  lines = loopConfFile.readlines()
  loopConf = {}
  for ixLine, line in enumerate(lines):
    line = re.split('\s\s\s+', line.rstrip())
    loopConf[line[0]] = line[1].split(', ')
  withLog.write("%s\n" % loopConf)

  # Check parameter files for consitency
  for group in groups:
    groupSelector = groups == group
    groupRestriction = np.unique(groupRestrs[groupSelector])
    nGroupRestriction = len(groupRestriction)
    nGroupVariables = sum(groupSelector)
    nSwIndicator = sum(paraProps[groupSelector] == 'swIndicator')
    nRelevGroupVariables = nGroupVariables - nSwIndicator
    if nGroupRestriction != 1:
      print('grouprestr are inconsistent for group ', group)
      print('groupRestr is', groupRestriction)
      sys.exit() 
    elif nRelevGroupVariables == 1 and groupRestriction[0].lower() == 'lowertr':
      print('lower triangular with one variable makes no sense')
      sys.exit()

#  sys.exit()  

  # Copy parameter files to tmp folder
  ixFirstExeFolder = 0
  ixExePath = 0
  exeFolder = os.path.split(loopConf['executables'][ixFirstExeFolder])[ixExePath]
  tmpParaFiles = {}
  for paraFile in paraFiles:
    if paraFile in tmpParaFiles: continue
    tmpParaFiles[paraFile] = tempfile.NamedTemporaryFile(mode='w', suffix='', prefix='tmp', dir=loopTmpFolder, delete=False).name
    f = open(os.path.join(exeFolder, paraFile))
    fileContents = f.read()
    f.close()
    f = open(tmpParaFiles[paraFile], 'w')
    f.write(fileContents)
    f.close()
    os.chmod(tmpParaFiles[paraFile], 0o660)
    
   ## print(exeFolder + paraFile, file = withLog)
    #des = os.path.split(os.path.join(loopTmpFolder, paraFile))[0]
  ##  print('des', des, file = withLog)
    #if not os.path.exists(des):
      #os.mkdir(des)
    #shutil.copy(os.path.join(exeFolder, paraFile), des)  

  # Construct main results folder
  if not os.path.exists(loopConf['output'][0]):
    os.mkdir(loopConf['output'][0])
  cDate = datetime.date.today()
  cTime = datetime.datetime.time(datetime.datetime.now())
  dateString = '%04d-%02d-%02d-%02d-%02d-%02d' % (cDate.year, cDate.month, cDate.day, cTime.hour, cTime.minute, cTime.second)
  resultsFolder = os.path.join(loopConf['output'][0], dateString)
  resultsFolder += 'LoopOver['
  for ixVar, var in enumerate(variables[groups >= 0]):
    if paraProps[ixVar] == 'swIndicator':
      continue
    if ixVar == len(variables[groups >= 0]) - 1:
      resultsFolder += var + ']'
    else:
      resultsFolder += var + ','
  print('\nResultsfolder')
  withLog.write('resultsFolder: %s\n' % resultsFolder)
  if not opts['dryrun']: 
    os.mkdir(resultsFolder)
    shutil.copy(generalConfPath2File, resultsFolder)
    shutil.copy(paraConfPath2File, resultsFolder)
  
#  sys.exit()

  # Set up groups
  groupBase = []
  groupIndices = []
  metaBase = []
  for ixGroup, group in enumerate(np.unique(groups[groups >= 0])):
    # ixGroup is used as index for groups
    print('\n')
    print('ixGroup', ixGroup)
    print('group', group)
    groupBase.append([])
    # Select vars belonging to group 'group'. Leave out switch indicator vars
    # --------------------------------
    relevGroups = groups == group
    swVars = paraProps == 'swIndicator'
    groupSelector = relevGroups
    for ix in range(0, len(relevGroups)):
      if relevGroups[ix] and not swVars[ix]: groupSelector[ix] = True
      else: groupSelector[ix] = False
    #  -------------------------------
    groupRestr = groupRestrs[groupSelector]
    groupRestr = np.unique(groupRestr)
    withLog.write('groupRestr: %s\n' % groupRestr)
    withLog.write('groupSelector: %s\n' % groupSelector)
    assert len(groupRestr) == 1, 'groups have different restrictions'
    for groupVals in vals[groupSelector]:
      withLog.write('groupvals: %s\n' % str2vals(groupVals))
      groupBase[group].append(len(np.array(str2vals(groupVals))))
      #, dtype = np.float32
    groupIndices.append(list(getIndex(groupBase[ixGroup], groupRestr)))
#    groupIndices.append(np.squeeze(list(getIndex(groupBase[ixGroup], groupRestr))).tolist())

    withLog.write('groupIndices: %s\n' % groupIndices[ixGroup])
    #metaBase.append(len(np.squeeze(groupIndices[ixGroup])))
    metaBase.append(len(groupIndices[ixGroup]))

 # metaIndices = np.squeeze(list(getIndex(metaBase))).tolist()
  # Combine groups without restriction
  metaIndices = list(getIndex(metaBase, None))
  nMetaIndices = len(metaIndices)
  
  withLog.write('\n\n')
  withLog.write('---------')  
  withLog.write('Summary after establishing groups: ')
  withLog.write('groupbase: %s\n' % groupBase)
  withLog.write('groupindices: %s\n' % groupIndices)
  withLog.write('metabase: %s\n' % metaBase)
  withLog.write('metaind: %s\n' % metaIndices)
  withLog.write('---------')  
  withLog.write('\n')
 
  
  print('\n')
  print('Establish processor qeueu:')
  exeQueue = queue.Queue()
  print(list(range(len(loopConf['executables']))))
  for ixQueue in range(len(loopConf['executables'])):
    exeQueue.put(ixQueue)
  print('Queue has size: %d' % exeQueue.qsize())
  print('\n')

  
 # sys.exit()
  
  
  withLog.write('-----Main loop started------')
  for ixMeta, meta in enumerate(metaIndices):
    withLog.write('--\n\n--')
    #print('Main iteration(ixMeta): ', ixMeta, file = withLog)
    withLog.write('Loop iteration(ixMeta) %d out of %d (%.2f%%)' % (ixMeta, nMetaIndices, 100.0 * (ixMeta / nMetaIndices)))
    
    index = getFullIndex(ixMeta, metaIndices, groupIndices, groups, paraProps, vals)
 
    print('index b4 flattening:')
    print(index)
    index = list(flatten(index))
    withLog.write('index:')
    withLog.write(index)
    for ixVar in range(0, len(variables)):
      print('variable nr %d var %s' % (ixVar, variables[ixVar]))
 
    runDescription = ''
    for ixVar, var in enumerate(variables):
      withLog.write('\nvariable: %s\n' % variables[ixVar])

#      print('val', vals[ixVar].split(',')[index[ixVar]].strip(), file = withLog)
      var = variables[ixVar]
      group = groups[ixVar]
      paraFile = paraFiles[ixVar]
      adjustIndex = indices[ixVar]
      val = extractVal(ixMeta, ixGroup, ixVar, vals, index, groups)
      regex = paraFileRegex[ixVar]
     # print('indices', indices[ixVar], file = withLog)
      # print(var, paraFile, adjustIndex, val)
      paraIndex = str2tuple(indices[ixVar])
      withLog.write('paraIndex %d\n' % paraIndex)
      newValMat = updateParameter(fileIn = tmpParaFiles[paraFile], varIn = var, paraIndex = paraIndex, newVal = val, regexIn = regex )
      print('newValMat %s' % newValMat)
      if group >= 0 and paraProps[ixVar] != 'swIndicator': 
        if ixVar < len(variables): runDescription += '_'
        runDescription += '%s=%s' % (var, newValMat.replace(' ',''))
    withLog.write('\nrunDescription: %s\n' % runDescription)
   # print(loopConf['output'][0], file = withLog)
    cDate = datetime.date.today()
    cTime = datetime.datetime.time(datetime.datetime.now())
    dateString = '%04d-%02d-%02d-%02d-%02d-%02d' % (cDate.year, cDate.month, cDate.day, cTime.hour, cTime.minute, cTime.second)
    folderName = os.path.join(resultsFolder, dateString + runDescription)
 
    exe = loopConf['executables'][0]
    if not opts['dryrun']: computeSolution(exe, loopConf['saveFiles'], tmpParaFiles, paraFiles, loopTmpFolder, folderName, exeQueue)
#  if not opts['dryrun']: analyzeLoop(resultsFolder, 'simulation.out')
#  if not opts['dryrun']: shutil.move(logFile, resultsFolder)
  
  # Flush log
  withLog.flush()
  
  # Copy log file to result folder
  if not opts['dryrun']: withLog.saveLogFile(resultsFolder)
  
  # Clean up temporary files
  for paraFile in tmpParaFiles:
    os.remove(tmpParaFiles[paraFile])
    
  shutil.rmtree(loopTmpFolder)
  
    
    
    
  
  
   # print(folderName, file = withLog)
#    pid = _thread.start_new_thread(computeSolution, (loopConf['executables'][0], loopConf['saveFiles'], paraFiles, loopTmpFolder, folderName))
    # while exeQueue.empty():
    #   print('still empty')
    #   time.sleep(10)
    # exe = exeQueue.get()
    # thread = threading.Thread(target=computeSolution, args = (exe, loopConf['saveFiles'], paraFiles, loopTmpFolder, folderName, exeQueue))
    # thread.daemon = False
    # thread.start()
  
  
def getFullIndex(ixMeta, metaIndices, groupIndices, groups, paraProps, vals):
  '''
  Returns current index of vals for each variable. 
  The current groupIndex is extended with values for special variables. 
  Currently these are: 
    1) group = -1 variables. 
    2) Indicator switch variables. 
  '''
  # --- Establish index list ----
  # -----------------------------
  index = []
  for ixGroup, group in enumerate(np.unique(groups)):
   # print('ixGroup', ixGroup)
    meta = metaIndices[ixMeta]
    groupSelector = groups == group
    groupVarIndices = np.where( groups == group )[0]
    print(groupVarIndices)
    
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
    #  index.append(groupIndices[group][meta[group]])      
    elif group == -1:
      groupMinus1 = groups < 0
      if sum(groupMinus1) > 1: print('warning... more than one -1 variable not supported')
      values = vals[groupMinus1][0].split(',')
      nValues = len(values)
      if ixMeta >= nValues: index.append(int(nValues - 1))
      else: index.append(int(ixMeta))
  return index


def extractVal(ixMeta, ixGroup, ixVar, vals, index, groups):
  '''
  Return variable value to be updated. 
  Functions allows to specify more involved retrieval. 
  '''
  #print('extractVal')
  splitVals = str2vals(vals[ixVar])
  return splitVals[index[ixVar]]



def computeSolution(exe, saveFiles, tmpParaFiles, paraFiles, loopTmpFolder, folderName, exeQueue, outStream = sys.stdout):
#  print('ident', _thread.get_ident())
  # print('start compute')
  # time.sleep(20)
  # exeQueue.put(exe)
  # print('done with compute')

  #def generateCmd():
    #cpCmd = 'cp -R '
    #outputFiles = saveFiles
    #for ixOutputFile, outputFile in enumerate(outputFiles):
      #outputFiles[ixOutputFile] = os.path.join(exeFolder, outputFiles[ixOutputFile])
      #cpCmd += outputFiles[ixOutputFile].replace(';','\;') + ' '
    #cpCmd += folderName + '/'
    #return cpCmd
  
  exeFolder = os.path.split(exe)[0]
  mainExecutable = os.path.split(exe)[1]

  # Copy paraFiles from temporary folder to exe folder
  for paraFile in paraFiles:
    #if filecmp.cmp(tmpParaFiles[paraFile], os.path.join(exeFolder, paraFile)): continue
    shutil.copy(tmpParaFiles[paraFile], os.path.join(exeFolder, paraFile))  
    
  withLog.write('call: %s\n' % mainExecutable)
  #os.execv(mainExecutable, [mainExecutable])
  p = subprocess.Popen('./' + mainExecutable, cwd = exeFolder, shell = False, stdout = subprocess.PIPE)
  while True:
    try:
      line = p.stdout.readline()
      #line = line.decode('utf_8')
    except Exception as e:
      print('cannot decode')
      pass
    if not line: break
    withLog.write(line)
    
#, stdout = withLog
#  print(p.stdout.read().decode("utf-8")) 
  p.wait()
#  p.communicate()
#  os.system() 

  # Check return code
  if p.returncode != 0:
    print('creating folder %s' % folderName)
    os.mkdir(folderName + '_FAILED')
    return p.returncode
  else:
    os.mkdir(folderName)    
    saveOutputFiles(exeFolder, saveFiles, folderName)
    
    #

  # folderName = folderName.replace(';','\;')
  # cpCmd = generateCmd(saveFiles)
  # os.system(cpCmd) 
  # os.system('rar a -ag+YYMMDDHHMM -ep1 ' +  folderName + '/code.rar ' + exeFolder + '/*.* ' + exeFolder + '/markov/src input/ > /tmp/xyz' )

#  os.execvp('rar', ['raree', 'a', '-ag+YYMMDDHHMM', '-ep1', folderName + '/code.rar', exeFolder + '/*.* ', exeFolder + '/markov/src', 'input/']) 
#  sys.exit()
  

def saveOutputFiles(inFolderName, relPsaveFiles, outFolderName):
  '''
    inFolderName: Root directory of computation folder
    relPsaveFiles: The relative file names from the root computation folder
    outFolderName: The loop ouptut folder. 
    Could make this function check if inFolderName is an ssh path. If so 
  '''
#   print('in saveoutputfiles')
#   print('saveFiles\n', saveFiles)
  absPsaveFiles = [ os.path.join(inFolderName, saveFile) for saveFile in relPsaveFiles ]
  for ixSaveFile, relPsaveFile in enumerate(relPsaveFiles):
    if os.path.isdir(absPsaveFiles[ixSaveFile]):
#      print('call disutils.dir')
#      print('from ' + os.path.join(inFolderName, relPsaveFile) + ' to ' + os.path.join(outFolderName, relPsaveFile))
      distutils.dir_util.copy_tree(os.path.join(inFolderName, relPsaveFile), os.path.join(outFolderName, relPsaveFile))
    elif re.match('(.*[*].*)', relPsaveFile): 
      #print('found globFile\n', relPsaveFile)
      globFiles = glob.glob(os.path.join(inFolderName, relPsaveFile))
      #print('globresult', glob.glob(relPsaveFile))
      for globFile in globFiles:
        shutil.copy(globFile, outFolderName)
    else:
      shutil.copy(relPsaveFile, outFolderName)
    

def updateParameter(fileIn, varIn, paraIndex, newVal, regexIn = '(\s*)([a-zA-Z0-9]+)(\s+)([a-zA-Z0-9\s,;\[\]\-]+)(\s*)'):
#  print('updateParameter inputs: \n --- \n {0} \n {1} \n {2} \n {3} \n {4} \n ---'.format(fileIn, varIn, paraIndex, newVal, regexIn))
  paraFile = open(fileIn)
  lines = paraFile.readlines()
  paraFile.close()
  for ixLine, line in enumerate(lines):
    (a, var, b, oldValMat, c) = re.match(regexIn, line.rstrip()).groups()
#    print('var=', var)
    if var == varIn:
      oldValMat = str2mat(oldValMat)
      if oldValMat.shape == (1,):
        if re.search('\.', str(newVal)):
          newValMat = float(newVal)
          newValMat = '%.3f' % (newValMat)
        elif not re.search('.*[0-9].*', str(newVal)): # string only
          newValMat = newVal
        else:
          newValMat = int(newVal)
          newValMat = '%d' % (newValMat)
      else:
        newValMat = oldValMat
        newValMat[paraIndex] = newVal
        newValMat = mat2str(newValMat)
      lines[ixLine] = a + var + b + newValMat + c + '\n'
  if not 'newValMat' in locals():
    print('variable %s not in parameter file %s' % (varIn, fileIn))
  paraFile = open(fileIn, 'w')
  paraFile.writelines(lines)
  paraFile.close()
  return newValMat

def str2vals(strIn):
  '''
  strIn: string containing different vals
  out:   np.array of different vals
  Function can be used to store val vectors in one overall vector and then unpack the string. 
  '''
  #if re.search('linspace.*', strIn):
    #print(strIn)
    #args = re.match('linspace\(([(0-9\.\s]+),([0-9\.\s]+),([0-9\.\s]+)\)', strIn).groups()
    #args = [ float(arg) for arg in args]
    ##print(np.linspace(args[0], args[1], args[2]))
    #return np.linspace(args[0], args[1], args[2])
  #el
  
  if re.search('.*linspace.*', strIn):
    out = np.array([])
    while strIn:
      if re.match('\s*linspace\(.*\)', strIn):
        (linSpacePart, strIn) = re.match('(\s*linspace\(.*?\)\s*)[,\s*]*(.*)', strIn).groups()
        args = re.match('linspace\(([(0-9\.\s-]+),([0-9\.\s-]+),([0-9\.\s-]+)\)', linSpacePart).groups()
        args = [ float(arg) for arg in args] # assume we always want float for linspace
        linSpaceVec = np.linspace(args[0], args[1], args[2])
        out = np.append(out, linSpaceVec)
      elif re.match('\s*[0-9\.]*\s*,', strIn):
        (valPart, strIn)      = re.match('(\s*[0-9\.]*\s*)[,\s*]*(.*)', strIn).groups()
        valPart = valPart.strip()
        if re.search('\.', valPart):
          valPart = float(valPart)
        else:
          valPart = int(valPart)
        out = np.append(out, valPart)
    return out

    #elements = re.match('(.*)(linspace\(.*\))(.*)', strIn).groups()
    #for subElement in elements:
      #if re.search('linspace', subElement):
        #args = re.match('linspace\(([(0-9\.\s]+),([0-9\.\s]+),([0-9\.\s]+)\)', subElement).groups()
        #args = [ float(arg) for arg in args]
        #out = np.append(out, np.linspace(args[0], args[1], args[2]))
      #else:
        #subElements = subElement.split(',')
        #element = [ element for element in subElements if element.strip() ] 
        #if element: out = np.append(out, element)
    #return out
  else:
    return str2mat(strIn)
  #else: 
    #print('string not recognized. Exiting...')
    #sys.exit()
  
  

def str2mat(strIn):
  '''
    strIn: String containing matrix
    out:   np.array with translated string elements. 
  '''
#  print('str2mat, inputs: \n --- \n {0} \n --- '.format(strIn))
  if not re.search(',', strIn) and not re.search(';', strIn): 
    return eval('np.array(' + '["' + strIn.strip() + '"]' + ')')
  elif not re.search(';', strIn):
    if re.match('\[', strIn):
      bareStr = re.match('\[(.*)\]',strIn).group(1)
    else: bareStr = strIn
    vec = re.split(',', bareStr)
    vec = [ element.strip() for element in vec ]
    return np.array(vec)
  else:
    mat = []
    bareStr = re.match('\[(.*)\]',strIn).group(1)
    rows = re.split(';', bareStr)
    for row in rows:
      elements = re.split(',', row)
      for ix in range(0, len(elements)):
        elements[ix] = elements[ix].strip()
      mat.append(elements)
    return np.array(mat)

def mat2str(matIn, fmt = '%.2f '):
  print('hello')
  strOut = '[ '
  nRows = len(matIn)
  for ixRow, row in enumerate(matIn):
    nEles = len(row)
    for ixEle, ele in enumerate(row):
      strOut += fmt % (ele)
      if ixEle == nEles - 1: break
      strOut += ', '
    if ixRow == nRows - 1: break
    strOut += '; '
  strOut += ' ]'
  print('\nstrOut' + strOut)
  return strOut



#def restoreFiles():
  #import os
  #print('restoring files')
  #os.system('cp /mnt/shareOffice/pymods/parameters.inOrig  /home/benjamin/workspace/forwardpremium/input/parameters.in')
  #os.system('cp /mnt/shareOffice/pymods/markovA.inOrig /home/benjamin/workspace/forwardpremium/input/markovA.in')
  #print('done')

if __name__ == '__main__':
  print('start') 
  #x = str2vals('0.9,1.1')
  #print(x[1])
  #saveOutputFiles('/mnt/shareOffice/ForwardPremium/Results/', ['/home/benjamin/workspace/forwardpremium/output/*.out'])
  args = [ '-f', '.']
  loop(args)
  print('\ndone') 
