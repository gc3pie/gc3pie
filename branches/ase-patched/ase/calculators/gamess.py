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
import cStringIO as StringIO

#Must install yourself
import numpy as np
from pyparsing import *

sys.path.append('/home/mmonroe/apps/gorg')
from gorg_site.gorg_site.model.gridjob import GridjobModel
from gorg_site.gorg_site.model.gridtask import GridtaskModel
from gorg_site.gorg_site.lib.mydb import Mydb

from ase.atoms import Atoms 


class GamessAtoms(Atoms):
    # Shoenflies space group
    symmetry = None

    def copy(self):
        import copy
        new_atoms = GamessAtoms(Atoms.copy(self))
        new_atoms.symmetry= copy.deepcopy(self.symmetry)
        return new_atoms

class Result(object):
    def __init__(self, atoms, params, calculator):
        import copy
        self.atoms = atoms.copy()
        self.params = copy.deepcopy(params)
        self.calculator = copy.deepcopy(calculator)
    

class GamessResult(Result):
    
    def __init__(self, atoms, params, calculator):
        super(GamessResult, self).__init__(atoms, params, calculator)
        self.parsed_dat = ParseGamessDat()
        self.parsed_out = ParseGamessOut()
        self.j_job = None
        self.j_task = None

    def wait(self, status='DONE', timeout=60):
        result, self.j_job  = self.calculator.wait(self.j_job, status, timeout)
        return result
    
    def get_coords(self):
        raw_coords=self.parsed_dat.get_coords()
        coords = np.array(raw_coords[1::2], dtype=float)
        return coords

    def get_orbitals(self, raw=False):
        """In GAMESS the $VEC group contains the orbitals."""
        if raw:
            return self.parsed_dat.get_vec()
        return np.array(self.parsed_dat.get_vec(), dtype=float)
        
    def get_hessian(self, raw=False):
        """In GAMESS the $HES group contains the Hessian."""
        if raw:
            return self.parsed_dat.get_hess()
        return np.array(self.parsed_dat.get_hess(), dtype=float)

    def get_forces(self):
        """This returns the gradients."""
        grad = self.parsed_dat.get_forces()
        mat = np.array(np.zeros((len(grad), 3)), dtype=float)
        for i in range(0, len(grad)):
            mat[i] = grad[i][1]
        return mat

    def get_potential_energy(self):
        return float(self.parsed_dat.get_energy())
    
    def is_exit_successful(self):
        return self.parsed_out.is_exit_successful()
    
    def is_geom_located(self):
        return self.parsed_out.is_geom_located()
        
    def read(self):
        """Read the results from the GAMESS output files."""
        # Get the attachments from the database so we can parse them
        f_output = self.calculator.get_files(self.j_job)
        self.parsed_dat.parse_file(f_output['dat'])
        self.parsed_out.parse_file(f_output['stdout'])
        map(file.close,f_output.values())
    
class GamessParams:
    '''Holds the GAMESS run parameters'''
    groups = dict()
    j_user_params = dict()
    j_title = 'A title goes here'
    r_orbitals = None
    r_hessian = None
    
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

class MyCalculator(object):
    """Base class for calculators. 
    
    We might want a calculator that uses the grid and another one that
    runs the applicationon the local computer. Both calculators need to implement 
    the same functions, only how the application is run, and where the result files
   are located would be different.
   """
    def calculate(self, atoms, params):
        assert False, 'Must implement a calcuate method'
    
    def preexisting_result(self, location):
        assert False, 'Must implement a preexisting_result method'
    
    def get_files(self):
        assert False,  'Must implement a get_files method'

class GamessGridCalc(MyCalculator):
    """Calculator class that interfaces with GAMESS-US using
    a database. The jobs in the database are executed on a grid.
    """
    def __init__(self, author='mark', j_db_name='gorg_site', j_db_url='http://127.0.0.1:5984'):
        self.j_author = author
        self.j_db_name = j_db_name
        self.j_db_url = j_db_url
        self.j_input_file = 'inp'
    
    def calculate(self, atoms, params, j_task=None, j_parent=tuple()):
        """We run GAMESS in here."""
        from ase.io.gamess import WriteGamessInp
        result = GamessResult(atoms, params, self)
        db=Mydb(self.j_db_name,self.j_db_url).cdb()
        # Map the GamessParams used for running the job to the job in the db
        a_job = GridjobModel()
        a_job.author = self.j_author
        a_job.defined_type = 'Gamess'
        a_job.input_file = self.j_input_file
        a_job.title = params.j_title
        # Convert the python dictionary to one that uses the couchdb schema
        for key in params.j_user_params:
            a_job.user_params[key] = params.j_user_params[key]
        
        f_inp = StringIO.StringIO()
        writer = WriteGamessInp()
        writer.write(f_inp, atoms, params)
        f_inp.seek(0)
        a_job=a_job.put_attachment(db, f_inp, a_job.input_file)
        del f_inp
        # Store the job in the task
        if j_task:
            j_task.add_job(a_job, j_parent)
            j_task.store(db)
        result.j_job = a_job
        result.j_task = j_task
        return result

    def preexisting_result(self, j_id ):
        """Rather than running GAMESS, we just read
        and parse the results already in the database."""
        db=Mydb(self.j_db_name,self.j_db_url).cdb()
        a_job = GridjobModel().load(db, j_id)
        f_attachments= a_job.attachments_to_files(db)
        new_reader = ReadGamessInp(f_attachments['inp'])
        result = GamessResult(new_reader.atoms, new_reader.params, self)        
        result.j_job = a_job
        return result

    def get_files(self, j_job):
        db=Mydb(self.j_db_name, self.j_db_url).cdb()
        f_attachments= j_job.attachments_to_files(db)
        return f_attachments

    def wait(self, j_job, status='DONE', timeout=60, check_freq=10):
        import time
        if timeout == 'INFINITE':
            timeout = sys.maxint
        if check_freq > timeout:
            check_freq = timeout
        db=Mydb(self.j_db_name, self.j_db_url).cdb()
        starting_time = time.time()
        j_job=j_job.load(db, j_job.id)
        while starting_time + timeout > time.time() and j_job.status != status:
            time.sleep(check_freq)        
            j_job=j_job.load(db, j_job.id)
        if j_job.status == status or j_job.status == 'ERROR':
            # We did not timeout 
            return True, j_job
        else:
            # We timed out
            return False, j_job

class GamessLocalCalc(MyCalculator):
    EXT_INPUT_FILE = 'inp'
    EXT_STDOUT_FILE = 'stdout'
    EXT_STDERR_FILE = 'stderr'
    EXT_DAT_FILE = 'dat'
    CMD_GAMESS = 'rungms'
    
    def __init__(self, f_dir='/tmp'):
        self.f_dir = f_dir
    
    def preexisting_result(self, f_name):
        """Rather than running GAMESS, we just read
        and parse the results already on the file system."""
        from ase.io.gamess import ReadGamessInp
        f_dict = self.get_files(f_name)
        new_reader = ReadGamessInp(f_dict['inp'])
        result = GamessResult(new_reader.atoms, new_reader.params, self)        
        result.j_job = f_name
        return result
        
    def get_files(self, j_id):
        f_dict = dict()
        f_dict[self.EXT_INPUT_FILE] = open('%s/%s.%s'%(self.f_dir, j_id, self.EXT_INPUT_FILE))
        f_dict[self.EXT_STDOUT_FILE] = open('%s/%s.%s'%(self.f_dir, j_id, self.EXT_STDOUT_FILE))
        f_dict[self.EXT_STDERR_FILE] = open('%s/%s.%s'%(self.f_dir, j_id, self.EXT_STDERR_FILE))
        f_dict[self.EXT_DAT_FILE] = open('%s/%s.%s'%(self.f_dir, j_id, self.EXT_DAT_FILE))
        return f_dict
    
    def calculate(self, atoms, params):
        import os
        from ase.io.gamess import WriteGamessInp
        result = GamessResult(atoms, params, self)
        writer = WriteGamessInp()
        f_name = GamessLocalCalc.generate_new_docid()
        f_inp = open('%s/%s.%s'%(self.f_dir, f_name, self.EXT_INPUT_FILE), 'w')
        writer.write(f_inp, atoms, params)
        f_inp.close()
        cmd = 'cd %s; %s %s.%s 1> %s.%s 2> %s.%s'%(self.f_dir, self.CMD_GAMESS, 
                                          f_name, self.EXT_INPUT_FILE, f_name, self.EXT_STDOUT_FILE, 
                                          f_name, self.EXT_STDERR_FILE)
        return_code = os.system(cmd)
        assert return_code == 0, 'Error runing GAMESS. Check %s/%s.%s'%(self.f_dir, f_name, self.EXT_STDOUT_FILE)
        result.j_job = f_name
        return result
     
    def wait(self, j_job, status='DONE', timeout=60, check_freq=10):
        if j_job.status == status or j_job.status == 'ERROR':
            # We did not timeout 
            return True, j_job
        else:
            # We timed out
            return False, j_job
        
    @staticmethod
    def generate_new_docid():
        from uuid import uuid4
        return uuid4().hex

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

    def get_forces(self, index=-1):
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
    
    def is_exit_successful(self):
        return self.group[self.STATUS_EXIT]
    
    def is_geom_located(self):
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
