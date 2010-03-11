"""This module defines an ASE interface to GAMESS

http://www.msg.ameslab.gov/GAMESS/
By: Mark Monroe
Email: monroe@oci.uzh.ch
"""
#Installed by default
import sys
import struct
import re
import string
import random
import time as time

#Must install yourself
import numpy as np
from pyparsing import *

from ase.atoms import Atoms 

class GamessAtoms(Atoms):
    # Shoenflies space group
    symmetry = None
    # User comment in the INP file
    comment = None 

    def copy(self):
        import copy
        new_atoms = GamessAtoms(Atoms.copy(self))
        new_atoms.comment(copy.deepcopy(self.comment))
        new_atoms.symmetry(copy.deepcopy(self.symmetry))
        return new_atoms

class GamessParams:
    '''Holds the GAMESS run parameters'''
    groups = dict()
   
    def get_group(self, group_key):
        return self.groups[group_key]

    def set_group_param(self, group_key, param_key, value):
        if not group_key in self.groups:
            self.groups[group_key] = dict()
        group_dict=self.groups[group_key]
        group_dict[param_key]=value
    
    def get_group_param(self, group_key, param_key):
        group_dict=self.groups[group_key]
        return group_dict[param_key]

class Gamess(object):
    """Create the file that specifies the GAMESS job
    
    Class is used to write GAMESS job files.
    """

    def __init__(self, label='gamess', gamess_params=GamessParams(), 
                 inp=None, stderr=None,  stdout=None,  dat=None):
        
        # These are assumed to be file like objects
        self.f_input = inp
        self.f_stderr = stderr
        self.f_stdout = stdout
        self.f_dat = dat
        
        self.label=label
        self.atoms = None
        self.parsed_dat = ParseGamessDat()
        self.parsed_out = ParseGamessOut()
        self.gamess_params = gamess_params
        self.is_initialized = False
        self.parsed_a_file = False
        
        """We need to keep track of whether or not the atoms/params object 
            was changed on us. if it has, we need to do a new calculation, 
            if it has not, we can use 
            the one that exists."""
        self.monitor = Monitor()
        
    def is_changed(self):
        check_if_changed = [self.atoms, self.gamess_params, self.f_input, self.f_stderr, self.f_stdout, self.f_dat]
        return self.monitor.is_changed(check_if_changed)

    def set_atoms(self, atoms):
        self.atoms = atoms.copy()

    def get_atoms(self):
        atoms = self.atoms.copy()
        return atoms

    def get_potential_energy(self, atoms, force_consistent=False):
        #There are many different energies that we can return here.
        self.update(atoms)
        return float(self.parsed_dat.get_energy())
#        if force_consistent:
#            return self.energy_free
#        else:
#            return self.energy_zero

    def get_forces(self, atoms):
        """This returns the gradients."""
        self.update(atoms)
        grad = self.parsed_dat.get_force()
        mat = np.array(np.zeros((len(grad), 3)), dtype=float)
        for i in range(0, len(grad)):
            mat[i] = grad[i][1]
        return mat
    
    def get_coords_result(self, atoms):
        self.update(atoms)
        raw_coords=self.parsed_dat.get_coords()
        coords = np.array(raw_coords[1::2], dtype=float)
        return coords
        
    def get_hessian(self, atoms, raw_format=False):
        self.update(atoms)
        if raw_format:
            return self.parsed_dat.get_hess()
        return np.array(self.parsed_dat.get_hess(), dtype=float)
    
    def get_orbitals(self, atoms, raw_format=False):
        """In GAMESS the $VEC group are the orbitals."""
        self.update(atoms)
        if raw_format:
            return self.parsed_dat.get_vec()
        return np.array(self.parsed_dat.get_vec(), dtype=float)

    #def get_stress(self, atoms):
    #    self.update(atoms)
    #    return self.stress
    
    def initialize(self, atoms):
        if not self.is_initialized:
            #We must start the change tracking
            self.is_changed()
            self.is_initialized = True
        return atoms
        
    def read(self, atoms):
        """Read the results form the GAMESS output files."""
        self.parsed_dat.parse_file(self.f_dat)
        self.parsed_out.parse_file(self.f_stdout)       
        return
    
    def update(self, atoms):
        self.initialize(atoms)
        """If we do not have a filename, we can not parse a file.
            That means we need to first make the output file by
            running GAMESS."""
        if self.is_changed() or self.f_stdout==None:
            self.calculate(atoms)
            self.parsed_a_file = False
        if not self.parsed_a_file:
            self.read(atoms)
            self.parsed_a_file = True
        return
    # TODO: Figure out how to handle the calculations
    def calculate(self, atoms):
        pass
        """Run the GAMESS calculation in here"""
        from ase.io.gamess import WriteGamessInp
        gamess_writer = WriteGamessInp()
        self.set_filename()
        gamess_writer.write(self.f_input, atoms)
        #Run the job some how
        jobid = submitSGEJob(self.f_input)
        while not finishedSGEJob(jobid):
            print "waiting for jobid %s to finish"%(jobid)
            time.sleep(60)
            
    def set_filename(self, filename=None):
        if filename:
            self.filename=filename
        else:
            self.filename = '%s_%s'%(self.label, self.generate_random_string())
        
    @staticmethod
    def generate_random_string():
        """Generate a unique file name for the GAMESS job."""
        from uuid import uuid4 as uuidf
        import md5 as md5
        """
        Generates a universally unique ID.
        Any arguments only create more randomness.
        """
        t = long( time.time() * 1000 )
        r = long( random.random()*100000000000000000L )

        # if we can't get a network address, just imagine one
        a = random.random()*100000000000000000L
        data = str(t)+' '+str(r)+' '+str(a)
        data = md5.md5(data).hexdigest()
        return data
        
class ParseGamessDat(object):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        #Group titles used to reference values in the group dictionary
        self.VEC = 'VEC'
        self.HESS = 'HESS'
        self.COORD = 'COORD'
        self.ENERGY = 'ENERGY'
        self.GRAD = 'GRAD'
        self.NORM_MODE = 'NORM_MODE'
#        keys = [self.VEC, self.HESS, self.COORD, self.ENERGY, self.GRAD, self.NORM_MODE]
#        values = [[], [], [], [], [], []]
#        
#        self.group = dict(zip(keys, values))
        self.group = dict()
        
        self.parse_kernal = ParseKernal()
        start,  end = self.read_vec(returnRule=True)
        self.parse_kernal.addRule(start, end, self.read_vec)
        start,  end = self.read_hess(returnRule=True)
        self.parse_kernal.addRule(start, end, self.read_hess)
        start,  end = self.read_coord(returnRule=True)
        self.parse_kernal.addRule(start, end, self.read_coord)
        start,  end = self.read_grad(returnRule=True)
        self.parse_kernal.addRule(start, end, self.read_grad)
        start,  end = self.read_energy(returnRule=True)
        self.parse_kernal.addRule(start, end, self.read_energy)
        start,  end = self.read_normal_mode_molplt(returnRule=True)
        self.parse_kernal.addRule(start, end, self.read_normal_mode_molplt)
    
    def get_result_group(self):
        return self.group

    def get_vec(self, index=-1):
        if self.VEC in self.group:
            return self.group[self.VEC][index]
        return None
    
    def get_hess(self, index=-1):
        if self.HESS in self.group:
            return self.group[self.HESS][index]
        return None
    
    def get_coords(self, index=-1):
        if self.COORD in self.group:
            return self.group[self.COORD][index]
        return None
    
    def get_energy(self, index=-1):
        #Which energy is the questions!!!!
        if self.ENERGY in self.group:
            return self.group[self.ENERGY][index]
        return None

    def get_force(self, index=-1):
        if self.GRAD in self.group:
            return self.group[self.GRAD][index]
        return None

    def get_normal_mode(self, index=-1):
        if self.NORM_MODE in self.group:
            return self.group[self.NORM_MODE][index]
        return None
    
    def parse_file(self, f_dat):
        self.parse_kernal.parse(f_dat)
    
    @staticmethod
    def fix_gamess_matrix(mat):
        """Rearranges matrix result when fist element of martix is key
        result[0][0][0] is a key
        result[0][0][1] is another key
        result[0] -> Defines a result set
        result[0][0] -> Defines a key set
        result[0][1] -> Defines a value line        
        """        
        last = [0, 0]
        data = list()
        for key, value in mat:            
            if last == (int(key[0]), int(key[1]) - 1):
                data[-1].extend(value)
            else:
                data.append(value)
            last = (int(key[0]), int(key[1]))
        return tuple(map(tuple, data))
    
    def read_vec(self, text_block=None, returnRule=False):
        if returnRule:
            #Define parse rule
            heading=r"""\$VEC"""
            trailing=r"""\$END"""
            return (heading, trailing)
        else:
            groupTitle = self.VEC
            resultset = list()
            last_key = ('')
            fun_append = resultset.append
            fun_struct_unpack = struct.unpack
            for line in text_block.splitlines(True)[1:-1]: #Do not parse heading and ending
                    cnt = (len(line) - 5) / 15 #Count how many values we have minus the key
                    fmt = "2s3s" + cnt*"15s" + "1x"
                    vals = fun_struct_unpack(fmt, line)
                    vals = map(string.strip, vals)
                    #resultset.append([vals[0:2], vals[2:]]) # Keys
                    key=vals[0:2]
                    value=vals[2:]
                    if last_key == (int(key[0]), int(key[1]) - 1):
                        resultset[-1].extend(value)
                    else:
                        fun_append(value)
                    last_key = (int(key[0]), int(key[1]))
            #resultset = self.fix_gamess_matrix(resultset)
            map(tuple, resultset)
            if groupTitle in self.group:
                self.group[groupTitle].append(resultset)
            else:
                self.group[groupTitle]=[resultset]

    def read_hess(self, text_block=None, returnRule=False):
        if returnRule:
            #Define parse rule
            heading=r"""\$HESS"""
            trailing=r"""\$END"""
            return (heading, trailing)
        else:
            groupTitle = self.HESS
            resultset = list()        
            last_key = ('')
            fun_append = resultset.append
            fun_struct_unpack = struct.unpack
            for line in text_block.splitlines(True)[2:-1]: #Skip the first two lines
                    cnt = (len(line) - 5) / 15 #Count how many values we have minus the key
                    fmt = "2s3s" + cnt*"15s" + "1x"
                    vals = fun_struct_unpack(fmt, line)
                    vals = map(string.strip, vals)
                    #resultset.append([vals[0:2], vals[2:]]) # Keys
                    key=vals[0:2]
                    value=vals[2:]
                    if last_key == (int(key[0]), int(key[1]) - 1):
                        resultset[-1].extend(value)
                    else:
                        fun_append(value)
                    last_key = (int(key[0]), int(key[1]))
#            resultset = self.fix_gamess_matrix(resultset)
            map(tuple, resultset)
            if groupTitle in self.group:
                self.group[groupTitle].append(resultset)
            else:
                self.group[groupTitle]=[resultset]
        
    def read_grad(self, text_block=None, returnRule=False):
        if returnRule:
            #Define parse rule
            heading=r"""\$GRAD"""
            trailing=r"""\$END"""
            return (heading, trailing)
        else:        
            groupTitle = self.GRAD
            resultset = list()
            for line in text_block.splitlines(True)[2:-1]: #Skip the first two header lines and last end line tag
                    fmt = "2s9x7s17s3x17s3x17s1x"
                    vals = list(struct.unpack(fmt, line))
                    vals = MyUtilities.striplist(vals)
                    resultset.append([vals[0:2], vals[2:]]) # Keys                            
            if groupTitle in self.group:
                self.group[groupTitle].append(resultset)
            else:
                self.group[groupTitle]=[resultset]

    def read_coord(self, text_block=None, returnRule=False):
        if returnRule:
            #Define parse rule
            heading=r"""COORDINATES OF SYMMETRY UNIQUE ATOMS \(ANGS\)"""
            trailing=r"""--- """
            return (heading, trailing)
        else:
            groupTitle= self.COORD
            rule = OneOrMore(Group(Word(alphas) + Word(nums+'.')) + Group(OneOrMore(Word(nums+'-.', min=12))))
            result = rule.searchString(text_block).asList()[0]
            if groupTitle in self.group:
                self.group[groupTitle].append(result)
            else:
                self.group[groupTitle]=[result]
        
    def read_energy(self, text_block=None, returnRule=False):
        if returnRule:
            #Define parse rule
            heading=r"""^E[\(]{1}"""
            trailing=r"""ITERS"""
            return (heading, trailing)
        else:        
            groupTitle=self.ENERGY
            '''The first energy is the one returned, not the NUC energy, but both are parsed for 
            E(RHF)=     -308.7504263559, E(NUC)=  299.7127028401,    9 ITERS'''
            rule=Group(Literal('E(') + Word(nums+'-'+alphas) + Literal(')=')).suppress() + Word(nums+'-.')
            result = rule.searchString(text_block).asList()[0]
            if groupTitle in self.group:
                self.group[groupTitle].append(result[0])
            else:
                self.group[groupTitle]=result
        
    #MJM TODO: FIX ME!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    def read_normal_mode_molplt(self, text_block=None, returnRule=False):
        """"MOLPT Normal Mode output is formated like the gradient output:
        C FREQ(X) FREQ(Y) FREQ(Z)
        H FREQ(X) FREQ(Y) FREQ(Z)
        H FREQ(X) FREQ(Y) FREQ(Z) """
        if returnRule:
            #Define parse rule
            heading=r"""----- START OF NORMAL MODES FOR -MOLPLT- PROGRAM -----"""
            trailing=r"""----- END OF NORMAL MODES FOR -MOLPLT- PROGRAM -----"""
            return (heading, trailing)
        else:
            #Define parse rule
            groupTitle=self.NORM_MODE
            if text_block:
                section = section[0][0] #We have only one section in the dat file from MOLPLT data
                rule=Literal('ATOMIC MASSES').suppress()+OneOrMore(Word(nums+'.'))        
                resultMass = rule.searchString(section).asList() #Atomic Masses
                rule=Literal('FREQUENCY=').suppress()+Word(nums+'-.')        
                resultFreq = rule.searchString(section).asList() #Frequencies
                rule=Literal('(CM**-1)').suppress()+OneOrMore(Word(nums+'-.E'))        
                resultMode = rule.searchString(section).asList() #Modes
                #file.seek(0)        
                resultset=dict()
                resultset['ATOMIC_MASS']=resultMass[0]
                resultset['FREQUENCY']=list()
                for i in resultFreq: resultset['FREQUENCY'].append(i[0])
                resultset['MODE']=resultMode
                self.group[groupTitle]=resultset #There can only be one MOLPLT group

class ParseGamessOut(object):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        #For statuses True is always good, Faluse is always bad (error,did not complete,etc)
        self.STATUS_EXIT = 'STATUS_EXIT_SUCCESSFUL'
        self.STATUS_GEOM_LOCATED='STATUS_GEOM_LOCATED'
        self.STATUS_NO_CPU_TIMEOUT = 'STATUS_NO_CPU_TIMEOUT'
        
        self.group = dict()
        
        self.parse_kernal = ParseKernal()
        start,  end = self.read_status_cpu_timeout(returnRule=True)
        self.parse_kernal.addRule(start, end, self.read_status_cpu_timeout)        
        start,  end = self.read_status_exit(returnRule=True)
        self.parse_kernal.addRule(start, end, self.read_status_exit)
        start,  end = self.read_status_geom_located(returnRule=True)
        self.parse_kernal.addRule(start, end, self.read_status_geom_located)

    def parse_file(self, f_dat):        
        self.parse_kernal.parse(f_dat)
    
    def status_exit_successful(self):
        return self.group[self.STATUS_EXIT]
    
    def status_geom_located(self):
        return self.group[self.STATUS_GEOM_LOCATED]
    
    def status_no_cpu_timeout(self):
        return self.group[self.STATUS_NO_CPU_TIMEOUT]
    
    def read_status_exit(self, text_block=None, returnRule=False):
        groupTitle = self.STATUS_EXIT
        self.group[groupTitle]=False
        if returnRule:
            #Define parse rule
            heading=r"""ddikick\.x: exited gracefully."""
            trailing=r"""ddikick\.x: exited gracefully."""
            return (heading, trailing)
        else:        
            if text_block:
                self.group[groupTitle]=True
        return            
     
    def read_status_geom_located(self, text_block=None, returnRule=False):
        groupTitle = self.STATUS_GEOM_LOCATED
        self.group[groupTitle]=False
        if returnRule:
            #Define parse rule
            heading=r"""EQUILIBRIUM GEOMETRY LOCATED"""
            trailing=r"""EQUILIBRIUM GEOMETRY LOCATED"""
            return (heading, trailing)
        else:        
            if text_block:
                self.group[groupTitle]=True
        return

    def read_status_cpu_timeout(self, text_block=None, returnRule=False):
        groupTitle = self.STATUS_NO_CPU_TIMEOUT
        self.group[groupTitle]=True
        if returnRule:
            #Define parse rule
            heading=r"""THIS JOB HAS EXHAUSTED ITS CPU TIME"""
            trailing=r"""THIS JOB HAS EXHAUSTED ITS CPU TIME"""
            return (heading, trailing)
        else:        
            if text_block:
                self.group[groupTitle]=False
        return

class ParseKernal(object):
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

class Monitor(object):
    '''Monitors the object to see if has been changed.
    
    Each time is_changed(object) it compares it to the last object
    that is_changed was passed. If different, returns false,
    otherwise returns true.        
    '''
    from cPickle import dumps
    _cm_last_dump = None
    def is_changed(self, obj):
        prev_dump = self._cm_last_dump
        self._cm_last_dump = None
        cur_dump = self.dumps(obj, -1)
        self._cm_last_dump = cur_dump
        return ( ( prev_dump is not None ) and ( prev_dump != cur_dump ) )
            
class MyUtilities(object):
    import itertools

    @staticmethod
    def striplist(l):
        '''String the white space from a list that contains strings
        '''
        return([x.strip() for x in l])
    
    @staticmethod
    def strip_tuple(l):
        '''String the white space from a tuple that contains strings
        '''
        return(tuple([x.strip() for x in l]))
    
    @staticmethod
    def search_list_for_substring(l, substring):
        """Search a list to see if any of the list's strings contain the substring."""
        for i in range(0, len(l)): 
            if -1 != l[i].find(substring) :
                found = i
                break
        return found

    @staticmethod
    def split_seq(iterable, size):
        """ Split a interable into chunks of the given size
            tuple(split_seq([1,2,3,4], size=2)
                            returns ([1,2],[3,4])
            
        """
        import itertools
        it = iter(iterable)
        item = list(itertools.islice(it, size))
        while item:
            yield item
            item = list(itertools.islice(it, size))
    
    @staticmethod
    def flatten(l, ltypes=(list, tuple)):
        '''Remove any nesting from a tuple or list
        [[1,2],[2,3]], becomes [1,2,2,3]
        '''
        ltype = type(l)
        l = list(l)
        i = 0
        while i < len(l):
            while isinstance(l[i], ltypes):
                if not l[i]:
                    l.pop(i)
                    i -= 1
                    break
                else:
                    l[i:i + 1] = l[i]
            i += 1
        return ltype(l)

    @staticmethod
    def sortNumericalStr(alist):
        '''Sort string containing numbers and chars in numerical order
        Some times you want to sort a string that is a mix of numbers and
        characters. ['hess1','hess10','hess8'] becomes ['hess1','hess8','hess10']
        '''    
        # inspired by Alex Martelli
        # http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/52234
        indices = map(MyUtilities._generate_index, alist)
        decorated = zip(indices, alist)
        decorated.sort()
        return [ item for index, item in decorated ]
    @staticmethod
    def _generate_index(str):
        """
        Splits a string into alpha and numeric elements, which
        is used as an index for sorting"
        """
        #
        # the index is built progressively
        # using the _append function
        #
        index = []
        def _append(fragment, alist=index):
            if fragment.isdigit(): fragment = int(fragment)
            alist.append(fragment)
        # initialize loop
        prev_isdigit = str[0].isdigit()
        current_fragment = ''
        # group a string into digit and non-digit parts
        for char in str:
            curr_isdigit = char.isdigit()
            if curr_isdigit == prev_isdigit:
                current_fragment += char
            else:
                _append(current_fragment)
                current_fragment = char
                prev_isdigit = curr_isdigit
        _append(current_fragment)    
        return tuple(index)

import subprocess
from subprocess import Popen

def finishedSGEJob(sgeID):    
    cmd = 'qstat'
    args = ' -j %s'%(sgeID)
    stdOut,  stdErr = Popen([cmd + args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True).communicate()
    if stdOut:
        print "Job %s still running."%(sgeID)
        return False
    elif stdErr: #qstat writes to the error when the job is not in the queue anymore
        #return re.findall(RE_JOBS, output)
        print "Job %s is not in the qeueu."%(sgeID)
        return True

def submitSGEJob(filename):
    RE_ID =  r"""(Your job ){1}([0-9]+)"""
    cmd = '~/qgms'
    arg = ' -n 16 %s'%(filename)
    output = Popen([cmd + arg], stdout=subprocess.PIPE, shell=True).communicate()[0]
    return re.search(RE_ID, output).group(2)
