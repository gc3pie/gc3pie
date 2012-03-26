import logbook, sys, os

class wrapLogger():
    def __init__(self, loggerName = 'myLogger', streamVerb = 'DEBUG', logFile = 'logFile', fileVerb = 'DEBUG', 
                       streamFormat = '{record.message}', fileFormat = '{record.message}'):
        self.loggerName = loggerName
        self.streamVerb = streamVerb
        self.logFile    = logFile
        self.streamFormat = streamFormat
        self.fileFormat = fileFormat
        logger = getLogger(loggerName = self.loggerName, streamVerb = self.streamVerb, logFile = self.logFile, 
                           streamFormat = streamFormat, fileFormat = fileFormat)
        self.wrappedLog = logger

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['wrappedLog']
        return state
    def __setstate__(self, state):
        self.__dict__ = state
        logger = getLogger(loggerName = self.loggerName, streamVerb = self.streamVerb, logFile = self.logFile, fileVerb = 'DEBUG')
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
    
    def setStreamVerb(self, verb):
        newHandlers = []
        # take out previous stream handlers
        for handler in self.wrappedLog.handlers:
            if type(handler) != logbook.StreamHandler: newHandlers.append(handler)
        # create new handler with adjusted verbosity
        self.streamVerb = verb
        mySH = logbook.StreamHandler(stream = sys.stdout, level = self.streamVerb.upper(), format_string = self.streamFormat, bubble = True)
        mySH.format_string = self.streamFormat
        # append new handler to newhandler list
        newHandlers.append(mySH)
        # set handlers in logbook object
        self.wrappedLog.handlers = newHandlers
        return
    
    def setFileVerb(self, verb):
        newHandlers = []
        # take out previous stream handlers
        for handler in self.wrappedLog.handlers:
            if type(handler) != logbook.FileHandler: newHandlers.append(handler)
        # create new handler with adjusted verbosity
        self.fileVerb = verb
        myFH = logbook.FileHandler(filename = self.logFile, level = self.fileVerb, bubble = True, mode = 'a')
        myFH.format_string = self.fileFormat 
        # append new handler to newhandler list
        newHandlers.append(myFH)
        # set handlers in logbook object
        self.wrappedLog.handlers = newHandlers
        return
        
    


def getLogger(loggerName = 'mylogger.log', streamVerb = 'DEBUG', logFile = 'log', fileVerb = 'DEBUG', 
            streamFormat = '{record.message}', fileFormat = '{record.message}'):

    # Get a logger instance.
    logger = logbook.Logger(name = loggerName)
    
    # set up logger
    mySH = logbook.StreamHandler(stream = sys.stdout, level = streamVerb.upper(), format_string = streamFormat, bubble = True)
    mySH.format_string = streamFormat
    logger.handlers.append(mySH)
    if logFile:
        if len(os.path.split(logFile)) > 1:
            dirName = os.path.dirname(logFile)
            if dirName and not os.path.isdir(dirName):
                os.makedirs(os.path.dirname(logFile))
        myFH = logbook.FileHandler(filename = logFile, level = fileVerb, bubble = True, mode = 'a')
        myFH.format_string = fileFormat
        logger.handlers.append(myFH)   
    
    try:
        stdErr = list(logbook.handlers.Handler.stack_manager.iter_context_objects())[0]
        stdErr.pop_application()
    except: 
        pass
    return logger
