import markstools
from markstools.lib.parsekernel import ParseKernel
import string
import struct

from pyparsing import *

class GamessDat(dict):
    #Group titles used to reference values in the group dictionary
    VEC = 'VEC'
    HESS = 'HESS'
    COORD = 'COORD'
    ENERGY = 'ENERGY'
    GRAD = 'GRAD'
    NORM_MODE = 'NORM_MODE'
    
    def get_vec(self, index=-1):
        if self.VEC in self:
            return self[self.VEC][index]
        return None
    
    def get_hess(self, index=-1):
        if self.HESS in self:
            return self[self.HESS][index]
        return None
    
    def get_coords(self, index=-1):
        if self.COORD in self:
            return self[self.COORD][index]
        return None
    
    def get_energy(self, index=-1):
        #Which energy is the questions!!!!
        if self.ENERGY in self:
            return self[self.ENERGY][index]
        return None

    def get_forces(self, index=-1):
        if self.GRAD in self:
            return self[self.GRAD][index]
        return None

    def get_normal_mode(self, index=-1):
        if self.NORM_MODE in self:
            return self[self.NORM_MODE][index]
        return None


class ParseGamessDat(object):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''

#        keys = [self.VEC, self.HESS, self.COORD, self.ENERGY, self.GRAD, self.NORM_MODE]
#        values = [[], [], [], [], [], []]
#        
#        self.group = dict(zip(keys, values))
        self.group = GamessDat()
        
        self.parse_kernel = ParseKernel()
        start,  end = self.read_vec(returnRule=True)
        self.parse_kernel.addRule(start, end, self.read_vec)
        start,  end = self.read_hess(returnRule=True)
        self.parse_kernel.addRule(start, end, self.read_hess)
        start,  end = self.read_coord(returnRule=True)
        self.parse_kernel.addRule(start, end, self.read_coord)
        start,  end = self.read_grad(returnRule=True)
        self.parse_kernel.addRule(start, end, self.read_grad)
        start,  end = self.read_energy(returnRule=True)
        self.parse_kernel.addRule(start, end, self.read_energy)
        start,  end = self.read_normal_mode_molplt(returnRule=True)
        self.parse_kernel.addRule(start, end, self.read_normal_mode_molplt)
    
    def parse_file(self, f_dat):
        self.parse_kernel.parse(f_dat)
        return self.group
    
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
            groupTitle = self.group.VEC
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
            groupTitle = self.group.HESS
            resultset = list()        
            last_key = ('')
            fun_append = resultset.append
            fun_struct_unpack = struct.unpack
            for line in text_block.splitlines(True)[2:-1]: #Skip the first two lines
                    cnt = (len(line) - 5) / 15 #Count how many values we have minus the key
                    fmt = "2s3s" + cnt*"15s" + "1x"
                    vals = fun_struct_unpack(fmt, line)
                    vals = map(string.strip, vals)
                    key=vals[0:2]
                    value=vals[2:]
                    if last_key == (int(key[0]), int(key[1]) - 1):
                        resultset[-1].extend(value)
                    else:
                        fun_append(value)
                    last_key = (int(key[0]), int(key[1]))
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
            groupTitle = self.group.GRAD
            resultset = list()
            for line in text_block.splitlines(True)[2:-1]: #Skip the first two header lines and last end line tag
                    fmt = "2s9x7s17s3x17s3x17s1x"
                    vals = list(struct.unpack(fmt, line))
                    #Strip the list
                    vals = [x.strip() for x in vals]
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
            groupTitle= self.group.COORD
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
            groupTitle=self.group.ENERGY
            '''The first energy is the one returned, not the NUC energy, but both are parsed for 
            E(RHF)=     -308.7504263559, E(NUC)=  299.7127028401,    9 ITERS'''
            rule=Group(Literal('E(') + Word(nums+'-'+alphas) + Literal(')=')).suppress() + Word(nums+'-.')
            result = rule.searchString(text_block).asList()[0]
            if groupTitle in self.group:
                self.group[groupTitle].append(result[0])
            else:
                self.group[groupTitle]=result
        
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
            groupTitle=self.group.NORM_MODE
            #Get ride of the first and last lines, it makes parsing easier
            text_block = '\n'.join(text_block.split('\n')[1:-2])
            rule=Literal('ATOMIC MASSES').suppress()+OneOrMore(Word(nums+'.'))        
            resultMass = rule.searchString(text_block).asList() #Atomic Masses
            rule=Literal('FREQUENCY=').suppress()+Word(nums+'-.')        
            resultFreq = rule.searchString(text_block).asList() #Frequencies
            rule=Literal('(CM**-1)').suppress()+OneOrMore(Word(nums+'-.E'))        
            resultMode = rule.searchString(text_block).asList() #Modes
            resultset=dict()
            resultset['ATOMIC_MASS']=resultMass[0]
            resultset['FREQUENCY']=list()
            for i in resultFreq: resultset['FREQUENCY'].append(i[0])
            resultset['MODE']=resultMode
            self.group[groupTitle]=resultset #There can only be one MOLPLT group

class GamessOut(dict):
    #For statuses True is always good, Faluse is always bad (error,did not complete,etc)
    STATUS_EXIT = 'STATUS_EXIT_SUCCESSFUL'
    STATUS_GEOM_LOCATED='STATUS_GEOM_LOCATED'
    STATUS_NO_CPU_TIMEOUT = 'STATUS_NO_CPU_TIMEOUT'
    
    def is_exit_successful(self):
        return self[self.STATUS_EXIT]
    
    def is_geom_located(self):
        return self[self.STATUS_GEOM_LOCATED]
    
    def status_no_cpu_timeout(self):
        return self[self.STATUS_NO_CPU_TIMEOUT]
    
    
class ParseGamessOut(object):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.group = GamessOut()
        
        self.parse_kernel = ParseKernel()
        start,  end = self.read_status_cpu_timeout(returnRule=True)
        self.parse_kernel.addRule(start, end, self.read_status_cpu_timeout)        
        start,  end = self.read_status_exit(returnRule=True)
        self.parse_kernel.addRule(start, end, self.read_status_exit)
        start,  end = self.read_status_geom_located(returnRule=True)
        self.parse_kernel.addRule(start, end, self.read_status_geom_located)

    def parse_file(self, f_dat):        
        self.parse_kernel.parse(f_dat)
        return self.group
    
    def read_status_exit(self, text_block=None, returnRule=False):
        groupTitle = self.group.STATUS_EXIT
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
        groupTitle = self.group.STATUS_GEOM_LOCATED
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
        groupTitle = self.group.STATUS_NO_CPU_TIMEOUT
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
