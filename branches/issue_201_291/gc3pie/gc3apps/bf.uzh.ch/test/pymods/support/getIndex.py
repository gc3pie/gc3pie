#!/usr/bin/env python3

import numpy as np
from numpy import array, zeros, int16
import sys

class getIndex:
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
    self.base = array(base)
    self.loopIndex = zeros(len(self.base), dtype=int16)
    self.iteration = 1
    if isinstance(restr, np.ndarray):
      self.restr = restr[0].lower()
    elif restr:      
      self.restr = restr.lower()
    else:
      self.restr = restr

  def __iter__(self):
#    print('iter')
    return getIndex(self.base, self.restr)

  def increment(self):
    for ix in range(len(self.base) - 1, -1, -1):
      if self.loopIndex[ix] == self.base[ix] - 1:
        self.loopIndex[ix] = 0
      else:
        self.loopIndex[ix] += 1
        return

  def lowerTr(self):
    for ix in range(0, len(self.base) - 1) :
      if self.loopIndex[ix] < self.loopIndex[ix + 1]:
    #    print(self.loopIndex[ix], '>', self.loopIndex[ix + 1])
        return False
    return True

  def diagnol(self):
 #   print(self.loopIndex)
    for ix in range(0, len(self.base) - 1) :
      if self.loopIndex[ix] != self.loopIndex[ix + 1]:
     #   print(self.loopIndex[ix], '!=', self.loopIndex[ix + 1])
        return True
    return False    

  def __next__(self):
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
      print('Unknown restriction')
      sys.exit()

    if skip == True:
     # print('skipping', self.loopIndex)
      return self.__next__()
    else:
     # print ('returning loopindex', self.loopIndex)
      return self.loopIndex.tolist()

class Squares:
  def __init__(self, start, stop):
    self.value = start - 1
    self.stop = stop
  def __iter__(self):
    return self
  def __next__(self):
    if self.value == self.stop:
      raise StopIteration
    self.value += 1
    return self.value ** 2


if __name__ == '__main__':
  print('start')
  # x=Squares(1,5)
  # print(list(x))

  # indices = getIndex(base = [5,2,5], restr = 'dianol')
  # I = iter(indices)
  # print('squeezed', numpy.squeeze(list(I)))
  # print('nonsqueezed', list(I))

  # # x = iter(indices)
  # # next(x)
  # # print(next(indices))
  # # print(next(indices))


  indices = getIndex(base = [3,4], restr = 'lowertr')
  list(indices)
  for index in indices:
    print('index', index)


  print('done')
