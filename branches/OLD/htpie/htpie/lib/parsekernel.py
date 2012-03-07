import re

class ParseKernel(object):
    '''
    Searchs a text file for blocks of text that matchs the given criteria.
    A function is then called and pased the block of text found.
    The starting and ending lines are included in the block of text found.
    '''
    def __init__(self):
        '''
        Constructor
        '''        
        # Tags that start and end a text block.
        # If start=end, the block of text will be a single line.
        self.start = list()
        self.end = list()
       # Function to be called when a block of text has been generated.
       # funToCall(blockOfText)
        self.fun = list() 
    
    def addRule(self, start, end, funToCall):
        """ Add the search rules and function to be called to the list of search terms. """
        self.start.append(start)
        self.end.append(end)
        self.fun.append(funToCall)
    
    def getFun(self, result):
        """ Get the function to be called on the block of text """
        removed = list()
        for i in self.start: removed.append(i.replace('\\', ''))
        #See is we can find what we matched in one of the list's search strings.
        #Remember that one search string might match many different things!
        index =self.find_matched_index(result[0])
        return self.fun[index]
    
    def getStartRule(self):
        """ Get the rule that specifies a starting block of text ."""
        return '|'.join(self.start)
        
    def getEndRule(self, result):
        """ Get the rule that specifies the ending of a block of text. """
        removed = list()
        for i in self.start: removed.append(i.replace('\\', ''))
        index = self.find_matched_index(result[0])
        return self.end[index]
        
    def parse(self, fileIn):
        """ Extracts blocks of text from a file based on starting and ending rules.
            
            !!!!Does not support nested text blocks!!!!!
            
            File is read in line by line. Each line is tested against a start rule. 
            When matched, the end rule is looked for. When found a block of 
            text is passed to a parsing function that has been specified to 
            be used with those starting and ending rules.
        """
        # Starting rules are a list of regexs that have been '|' together
        regStart=re.compile(self.getStartRule())        
        foundOne = False
        fileIn.seek(0)
        line = fileIn.readline()
        while line:
            if not foundOne:
                result=regStart.findall(line)
                if result:
                    foundOne = True
                    firstPos = fileIn.tell()
                    blockText = line
                    funToCall = self.getFun(result)
                    regEnd=re.compile(self.getEndRule(result))
            # After I find the starting rule, see if I match the ending rule for the same line
            # If I do, my block of text will be one line, otherwise it will grow until I find the ending rule.
            if foundOne: 
                result=regEnd.findall(line)
                if result:
                    secondPos = fileIn.tell()
                    fileIn.seek(firstPos)                
                    blockText += fileIn.read(secondPos-firstPos)
                    funToCall(blockText)
                    foundOne=False
                    #print 'FOUND!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
            line = fileIn.readline()
        fileIn.seek(0)
    
    def find_matched_index(self, matchstr):
        for i in range(0, len(self.start)): 
            if re.search(self.start[i], matchstr):
                return i
        return None
