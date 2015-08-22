#!/usr/bin/env python

from __future__ import print_function
import numpy as np
import numpy.ma as ma
import sys, copy, re, os

if __name__ == "__main__" and __package__ is None:
  path2Pymods = os.path.join(os.path.dirname(__file__), '../../')
  if not sys.path.count(path2Pymods):
    sys.path.append(path2Pymods)
  import pymods

from pymods.support.support import is_float, is_int, transposed 

# Keep the type of the mask value depending on scipy version used. 
#try: # scipy >0.8
  #maskConstant = np.ma.core.MaskedConstant
#except AttributeError: # scipy 0.72
  #maskConstant = np.ma.core.MaskedArray  


numpyVersion = int(''.join(np.__version__.split('.')))
if numpyVersion >= 141:
#np.__version__ == '1.5.1' or np.__version__ == '1.4.1':
  maskConstant = np.ma.core.MaskedArray
#elif np.__version__ == '1.4.1':
#  maskConstant = np.ma.core.MaskedConstant

  
# Discussion about initializers
# http://stackoverflow.com/questions/682504/what-is-a-clean-pythonic-way-to-have-multiple-constructors-in-python
# http://coding.derkeiler.com/Archive/Python/comp.lang.python/2005-02/1294.html
# http://docs.scipy.org/doc/numpy/reference/maskedarray.baseclass.html#maskedarray-baseclass
class tableDict:
  '''
    Spread sheet like dictionary wrapper. Each dictionary contains a list of entries. 
    Data set is square. 
  '''
  
  def __init__(self, cols = [], dataList = [], width = 10, prec = 2):
    '''
    Inputs: 
      keys: List of keys
      dataList: List of table cols. 
    '''
    self.width = width
    self.prec  = prec  
    if not cols and not dataList:
      self.cols = 0
      self.nRows = 0
      self.data = []
      return
    self.cols = np.array(cols) #list(cols) # self.cols needs to be list since I use append in various methods. (not np.array(cols))
    self.data = dict()
    if not dataList: 
      self.nRows = 0
      self.colFormat = {}
      for ixCol, col in enumerate(cols):
        self.data[col] = np.ma.empty([])
        self.colFormat[col] = '>' + str(self.width) + 's'
      return
    self.nRows = len(dataList[0])
    for ixCol, col in enumerate(cols):
      firstEle = dataList[ixCol][0]
      # Check if string or unicode (py 2.x)
      if isinstance(firstEle, str) or isinstance(firstEle, np.unicode_) or isinstance(firstEle, unicode):
        if re.match('^[0-9]+$', firstEle):
          dt = np.dtype(int)
        elif re.match('^[-0-9]+\.[0-9e\-\+]+$',firstEle):
          dt = np.dtype(float)
        else:
          dt = None
      else:
        dt = None
        
      mask = np.array([ ele == '--' for ele in dataList[ixCol] ])        
      maskedArray = np.array(dataList[ixCol])        
      maskedArray[mask] = 0
      self.data[col] = np.ma.array(maskedArray, dtype = dt, fill_value = 0, mask = mask)
      
    self.colFormat = self.__formatCols()
    
    
  def insertEmptyCol(self, colName, dt):
    '''
    Inserts an empty column into the table. 
    '''
    self.cols = np.append(self.cols, colName)
    self.colFormat[colName] = str(self.width) + '.' + str(self.prec) + 'f'
    self.data[colName] = ma.empty((self.nRows,), dtype = dt)
    self.data[colName][:] = ma.masked
    
  @classmethod
  def fromDict(cls, dictIn, width = 10, prec = 2):
    if 'keys' in dictIn: 
      columns = dictIn['keys']
      del dictIn['keys']
    else:
      columns = list(dictIn.keys())
    dataList = []
    [ dataList.append(dictIn[col]) for col in columns ]
    return cls(columns, dataList, width, prec)
  
  @classmethod
  def fromRowList(cls, columns, rowList, width = 10, prec = 2):
    if isinstance(rowList, list):
      columnList = list(transposed(rowList))
    elif isinstance(rowList, np.ndarray):
      columnList = rowList.T
    else:
      print('unknown type, rowList')
      sys.exit()
    return cls(columns, columnList, width, prec)
  
  @classmethod
  def fromTextFile(cls, fileIn, delim = None, width = 10, prec = 2):
    rowList = []
    fh = open(fileIn, 'r')
    line = fh.readline()
    while line == '\n':
      line = fh.readline()
    cols = line.split(delim)
    cols = [ ele.strip() for ele in cols ]
#    cols = re.split('\s\s\s+', line.rstrip())
    line = fh.readline()
    while not line == '\n' and not line == '':
      dataRow = line.split(delim)
      dataRow = [ ele.strip() for ele in dataRow ]
      rowList.append(dataRow)
      line = fh.readline()
    return tableDict.fromRowList(cols, rowList, width, prec)
  
  
  
  
  def __setitem__(self, column, value):
    '''
      Set key to masked array of value. 
      1) column: New column in table
      2) value: List of values. 
    '''
    self.data[column] = np.ma.array(value)
    self.colFormat[column] = self.__formatCol(column)
    if column not in self.cols: 
      self.cols = np.append(self.cols, column)
      
    
    
  def __getitem__(self, key):
    '''
      Possible uses: 
      1) Get column -> key = string
      2) Subset -> key = a) slice b) int
    '''
    if isinstance(key, np.ndarray):
      return self.subset(key)
    if isinstance(key, str):
      return self.data[key]
    if isinstance(key, slice):
      new = copy.deepcopy(self)
      for col in new.cols:
        new.data[col] = new.data[col][key]
        new.nRows = len(new[col])
      return new
    if isinstance(key, int):
      new = copy.deepcopy(self)
      for col in new.cols:
        # important here: array([]), the [] inside the array
        new.data[col] = np.ma.array([new.data[col][key]])
        new.nRows = 1
      return new
      
    
  
  def __len__(self):
    '''
    Returns number of entries. Bc data set square take length
    of arbitrary (first) variable. 
    '''
    return self.nRows
  
  def hzcatTableDict(self, na, append = True):
    '''
      Add columns to data sheet "self". 
      append: True if the new columns are to be added at the end of the data sheet. False if they should come first. 
    '''
    if append:
      for col in na.cols:
        self.cols = np.append(self.cols, col)
  #      self.cols = np.append(self.cols, col)
        self.colFormat[col] = na.colFormat[col]
      for col in na.data:
        self.data[col] = na.data[col]
    elif not append:
      self.cols = self.cols[::-1]
      for col in na.cols:
        self.cols = np.append(self.cols, col)
        self.colFormat[col] = na.colFormat[col]
      for col in na.data:
        self.data[col] = na.data[col]
      self.cols = self.cols[::-1]
  
    
  def subset(self, boolIn):      
    for column in self.data.keys():
      self.data[column] = self.data[column][boolIn]
    self.nRows = len(self.data[column])
    ## set nRows to length of last key
    #try: 
      #self.nRows = len(self.data[column])
    #except TypeError: # scalars have no length attr
      #self.nRows = 1
    
  def getSubset(self, boolIn):
    new = copy.deepcopy(self)
    new.subset(boolIn)
    return new

      
  def append(self, na):
    '''
    Append tableDict na to self. 
    '''
    #for key in na.cols:
      #if key == 'keys': continue
      #if key in self.cols:
        #newDataVec = self.__getitem__(key)
        #for ele in na.data[key]:
          ##newDataVec.append(ele)          
          #np.append(newDataVec, ele)
        #self.__setitem__(key, newDataVec)
    for col in self.cols:
      self.data[col] = ma.concatenate((self.data[col][:], na.data[col][:]))
    self.nRows = self.nRows + na.nRows
        
  def getAppended(self, na):
    new = copy.deepcopy(self)
    new.append(na)
    return new
  
  def rename(self, oldName, newName):
    # Necessary bc column will be deleted otherwise
    if oldName == newName:
      return
    # convert self.cols to np.array to use booleans... could use self.cols as np.array the whole time
    columns = np.array(self.cols)
    col = columns == oldName
    #self.cols = ma.array(self.cols)
    self.cols = np.array(self.cols)
    largestColString = np.max([len(c) for c in self.cols])
    if len(newName) > largestColString:
      dt = np.dtype( ('S', len(newName) ) )
      self.cols = np.array(self.cols, dtype = dt)
    self.cols[col] = newName

    self.data[newName] = self.data[oldName]
    del self.data[oldName]
    self.colFormat[newName] = self.colFormat[oldName]
    del self.colFormat[oldName]
    #self[newName] = self[oldName]
    #self.drop(oldName)
        
  def __str__(self):
    '''
    First draft for a print method
    '''
    outStr = '\n\n'
    for col in self.cols:
      prStr = col.rjust(self.width, ' ')
      outStr += prStr
    outStr += '\n'
    maxEle = self.__len__()
    for ixEntry in range(0, maxEle):
      for col in self.cols:
        try: 
          ele = self.data[col][ixEntry]
        except IndexError:
          ele = None           
        outStr += self.__formatString(ele, col)
      outStr += '\n'
    return outStr
  

  def determineType(self, ele):
    if isinstance(ele, maskConstant):
      return maskConstant
    elif isinstance(ele, float):
      return float
    elif isinstance(ele, int):
      return int
    elif isinstance(ele, str) or isinstance(ele, np.unicode_) or ele == None:
      return str
    elif isinstance(ele, list):
      return list
    else:
      return 'unknown format'
    
  def __formatEle(self, ele):
    # Formatting see: http://docs.python.org/library/string.html#format-specification-mini-language
    eleType = self.determineType(ele)
    if eleType == float:
      fmt = str(self.width) + '.' + str(self.prec) + 'f'
    elif eleType == int:
      fmt = str(self.width) + 'd'
    elif eleType == str or eleType == list:
      fmt = '>' + str(self.width) + 's'
    elif eleType == maskConstant:
      fmt = '>' + str(self.width) + 's'
    else:
      print('eleType', eleType, 'unknown. Implement in tableDict')
    return fmt   

  def __formatCol(self, col):
    fmt = self.__formatEle(self.data[col][0])
    return fmt
    

  def __formatCols(self):
    colFormats = {}
    for col in self.cols:
      colFormats[col] = self.__formatCol(col)
    return colFormats    
    
      
  def __formatString(self, ele, col):
      
    fmt = '{0:' + self.colFormat[col] + '}'
    try:
      prStr = fmt.format(ele)      
    except ValueError:
      fmt = '{0:' + self.__formatEle(ele) + '}'
      try: prStr = fmt.format('--')
      except:
        print('cannot format ', ele, 'as ', fmt)
        print('continuing')
    return prStr

  def setWidth(self, width):
    self.width = width
    for col in self.cols:
      fmt = re.match(r'([\<\>\=\+\-\^\s]*)([0-9]+)([\,\.a-zA-Z0-9]+)', self.colFormat[col]).groups()
      pre = fmt[0]
      oldWidth = fmt[1]
      post = fmt[2]
      self.colFormat[col] = pre + str(self.width) + post
  

  def setPrec(self, prec):
    self.prec = prec
    for col in self.cols:
      if self.colFormat[col].find('f') != -1:
        width = self.colFormat[col].split('.')[0]
        self.colFormat[col] = width  + '.' + str(self.prec) + 'f'
    

  def insertRow(self, rowIndex, msk = True):
    '''
      At the moment, insert empty row before index rowIndex
      rowindex starts counting at zeros (as usual)
      New row is masked by default
    '''

    for col in self.cols:
      dt = self.data[col].dtype
      newEle = ma.empty((1,), dtype = dt)
      #print(self.data[col])
      #print(self.data[col][:rowIndex].shape)
      #print(newEle.shape)
      #print(self.data[col][rowIndex:].shape)
      self.data[col] = ma.concatenate((self.data[col][:rowIndex], newEle, self.data[col][rowIndex:]))
      #if rowIndex <= self.nRows:
        #self.data[col] = ma.concatenate((self.data[col][:rowIndex], newEle, self.data[col][rowIndex:]))
      #else:
        #self.data[col] = ma.concatenate((self.data[col], newEle))
      if msk: self.data[col][rowIndex] = ma.masked
    self.nRows += 1
      
    
  def merge(self, na, column):
    '''
      merge method for unique keys
    '''
#    if self.nRows == 0 and na.nRows == 0:
#      return
    if na.nRows == 0:
      return
    elif self.nRows == 0 and na.nRows > 0:
      self.cols = copy.deepcopy(na.cols)
      self.data = copy.deepcopy(na.data)
      self.colFormat = copy.deepcopy(na.colFormat)
      self.nRows = na.nRows
      print(self.cols)
      return
    self.sort(column)
    na.sort(column)
    colsToAppend = list(na.cols)
    colsToAppend.remove(column)
    keyVals = self.data[column]
    for ixRow, key in enumerate(na.data[column]):
      if key not in keyVals:
        keyVals = np.append(keyVals, np.ma.array([key]))
    keyVals.sort()
    selfKeyVals = self.data[column]
    naKeyVals   = na.data[column]
    newNrows = len(keyVals)
    # Set up new columns
    self.cols = np.append(self.cols, '_merge')
    self.__setitem__('_merge', np.ma.zeros((self.nRows,), dtype = np.int))
    for col in colsToAppend:
      if col in self.cols:
        print('Column already exists in original table. Aborting... ')
        sys.exit()
      self.cols = np.append(self.cols, col)
      self.colFormat[col] = na.colFormat[col]
      dt = na.data[col].dtype
      self.data[col] = np.ma.empty((self.nRows,), dtype = dt)
    # loop over rows
    for ixRow, key in enumerate(keyVals):
      naIndex = naKeyVals == key
      if not key in selfKeyVals:
        self.insertRow(ixRow)
        self.data['_merge'][ixRow] = 2
      elif not key in naKeyVals:
        self.data['_merge'][ixRow] = 1
      else:
        self.data['_merge'][ixRow] = 3
      for col in colsToAppend:
        if not key in naKeyVals:
          self.data[col][ixRow] = ma.masked
        else:
          self.data[col][ixRow] = na.data[col][naIndex][0]
    self.data[column] = keyVals
    self.data[column].mask = np.zeros(len(keyVals), dtype = np.int)
    
  def merged(self, na, column):
    new = self
    new.merge(na, column)
    return new
   
  def keep(self, columns):
    '''
      Select columns from the table
    '''
    columnList = []
    for column in columns:
      columnList.append(self.data[column])
    self.data.clear()
    for ixCol, col in enumerate(columnList):
      self.data[columns[ixCol]] = columnList[ixCol]
    self.cols = columns
    
  def drop(self, columns):
    '''
      Drop columns from the table
    '''
    if not isinstance(columns, list):
      columnList = []
      columnList.append(columns)
    else: 
      columnList = columns
    for column in columnList:
      del self.data[column]
      #self.cols.remove(column)
      self.cols = self.cols[ ~(self.cols == column) ]
        
  
  def sort(self, columns):
    '''
      Sort tableDict by columns specified in column list.
      Column list contains a list of keys where the first is the primary, or
      column is a string of the column to sort for. 
    '''
    if isinstance(columns, list):
      dataCols = []
      for column in columns:
        dataCols.append(self.data[column])
      dataCols.reverse()
      dataCols = tuple(dataCols)
    else:
      dataCols = (self.data[columns], )
    ind = np.lexsort(dataCols)
    self.subset(ind)
    
  def deleteAllRows(self):
    '''
      Method to slim table down to zero observations. 
    '''
    for col in self.cols: 
      dt = type(self.data[col][0])
      self.data[col] = np.ma.array([], dtype = dt)
    self.nRows = 0
    
  def nonMissing(self):
    '''
      Doesn't work at the moment. Not really needed too...Maybe delete soon...
      Eliminate all obs with missing values. 
      Does not work with default mask false. Every value needs to be set to false to retain. 
    '''
    for col in self.cols:
      self.data[col] = self.data[col][~self.data[col].mask]
#      self.data[col] = self.data[col].compressed()

  def order(self, columns):
    for col in columns:
#      self.cols.remove(col)
      self.cols = self.cols[ ~(self.cols == col) ]
    reverseColumns = columns
    reverseColumns.reverse()
    for col in reverseColumns:
#      self.cols.insert(0, col)
      self.cols = np.insert(self.cols, 0, col)
      
  def makeSQL(self):
    import sqlite3
    conn = sqlite3.connect('table')
    curs = conn.cursor()
    tablecmd = 'create table tableDict (name char(30), job char(10), pay int(4) )'
    curs.execute(tablecmd)
    
    
  def print2latex(self, fileIn = sys.stdout, alignment = None):
    from pymods.support.log import logger
    
    if not alignment == None:
      alignment = 'l' + alignment + 'l'
    
    if fileIn != sys.stdout:
      loggerObj = logger(fileIn)
    else:
      loggerObj = sys.stdout
      
      
    if alignment == None:
      alignment = 'c' * ( len(self.cols) + 2 )
      
#    print('\\begin{}', file = loggerObj)
    print('\\begin{tabular}{', end = '', file = loggerObj)
    #for ixCol, col in enumerate(self.cols):
      #alignment = 'c'
      #print('{0}|'.format(alignment), end = '', file = loggerObj)
    print(alignment, end = '', file = loggerObj)
    loggerObj.flush()
    print('}\\toprule\\addlinespace\n', end = '', file = loggerObj)
    print(' &', end = '', file = loggerObj)
    for ixCol, col in enumerate(self.cols):
      prStr = col.rjust(self.width, ' ')
      print('', end = '')
      print(prStr, end = '', file = loggerObj)
      loggerObj.flush()
      if not ixCol == len(self.cols) - 1: print(' &', end = '', file = loggerObj)
    print(' &', end = '', file = loggerObj)
    print(' \\\\ \\midrule\\addlinespace\n', end = '', file = loggerObj)
    for ixRow in range(self.nRows):
      print(' &', end = '', file = loggerObj)
      for ixCol, col in enumerate(self.cols):
        prStr = self.__formatString(self.data[col][ixRow], col)
        print(prStr, end = '', file = loggerObj)
        if not ixCol == len(self.cols) - 1: print(' &', end = '', file = loggerObj)
      print(' &', end = '', file = loggerObj)
      print(' \\\\  \n'.format(self.data[col][self.nRows - 1]), end = '', file = loggerObj)
    print('\\bottomrule \\addlinespace\n', end = '', file = loggerObj)
    print('\\end{tabular}\n', end = '', file = loggerObj)   
 #   print('\\end{}\n', end = '', file = loggerObj)
    loggerObj.flush()
    print('done')
  
  

if __name__ == '__main__':
  print('start') 
  
  myTable = tableDict.fromTextFile('/home/benjamin/workspace/fpProj/empirical/outputInput/momentTable/Gdp/gdpMoments.csv', ',')
  myTable.setWidth(7)
#  myTable = tableDict.fromRowList(['para', 'val' ], [ [1, 2 ], [4, 4], [1,3]])
  print(myTable)
  #myTable2 = tableDict.fromRowList(['para', 'val2' ], [ [1, 'dk' ], [2,'dkd'], [3,'dice']])
  #print(myTable2)

  #myTable.merge(myTable2, 'para')
  #print(myTable)


#  myTable.makeSQL()
 # myTable.print2latex('/tmp/blub')
  
  print('\ndone')   
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  

