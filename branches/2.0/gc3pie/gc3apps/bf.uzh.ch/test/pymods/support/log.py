#!/usr/bin/env python3

import sys, io

import shutil
import tempfile 
import os
import os.path

class logger:
  '''
    Personal logger class. Very basic and can be extended when needed. 
  '''
  
  def __init__(self, fileName = '/tmp/log.txt'):
    if fileName == None:
      self.filenName = tempfile.NamedTemporaryFile(mode='w', suffix='', prefix='tmp', dir='/tmp/', delete=False).name
    else:
      self.filenName = fileName
    try: 
      self.logPath = os.path.dirname(self.filenName)
    except AttributeError: 
      self.logPath = os.getcwd()
    if not os.path.exists(self.logPath): os.makedirs(self.logPath)
    if isinstance(fileName, str):
      self.logFile = open(self.filenName, 'w')
    else:
      self.logFile = fileName

  def write(self, string):
    sys.stdout.write(str(string))
    self.logFile.write(str(string))
    
  def writeline(self, line):
    sys.stdout.writelines(line)
    self.logFile.writelines(line)   

  def writelines(self, lines):
    sys.stdout.writelines(lines)
    self.logFile.writelines(lines)
  
  def saveLogFile(self, desFolder):
    self.logFile.flush()
    shutil.copy(self.filenName, os.path.join(desFolder, os.path.basename(self.filenName)))
    
  def flush(self):
    self.logFile.flush()

  def fileno(self):
    self.logFile.fileno()
    
    
class Tee(object):
  def __init__(self, name, mode):
    self.file = open(name, mode)
    self.stdout = sys.stdout
    sys.stdout = self
  #def __del__(self):
    #sys.stdout = self.stdout
    #self.file.close()
  def write(self, data):
    self.file.write(data)
    self.stdout.write(data)
    
    
def initialize_logging(loglevel = 'debug', quiet= False, logdir = '.', clean = True):
    """ Log information based upon users options"""
    import logging, os
    logger = logging.getLogger('project')
    formatter = logging.Formatter('%(asctime)s %(levelname)s\t%(message)s')
#    level = logging.__dict__.get(loglevel,logging.DEBUG)
    #logger.setLevel(logger.debug)
    logger.setLevel(logging.DEBUG)

    # Output logging information to screen
    if not quiet:
        hdlr = logging.StreamHandler(sys.stdout)
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)

    # Output logging information to file
  #  logfile = os.path.join(logdir, "project.log")
    logfile = "proj.log"
    if clean and os.path.isfile(logfile):
        os.remove(logfile)
    hdlr2 = logging.FileHandler(logfile)
    hdlr2.setFormatter(formatter)
    logger.addHandler(hdlr2)

    return logger

# implement getattribute that points to file


if __name__ == '__main__':
  print('start')
  #log = logger()
  #log = Tee('log.txt', 'w')
  logger = initialize_logging()
  logger.error("This is an error message.")
  logger.info("This is an info message.")
  logger.debug("This is a debug message.")
  #print('hello i am here', file = log)
  #print('hello i am here2', file = log)
  print('end')
