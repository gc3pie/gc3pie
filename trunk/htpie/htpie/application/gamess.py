from htpie import application
from htpie import model
from htpie.lib import parsekernel
from htpie.lib import utils

#Used in the parser
import string
import struct
from pyparsing import *
import numpy as np
import ase
import os

class GamessResult(model.MongoBase):
    collection_name = 'GamessResult'
    
    structure = {'exit_successful':bool,
                         'geom_located':bool, 
                         'no_cpu_timeout':bool,  
                         'hessian':[model.MongoMatrix],
                         'gradient':[model.MongoMatrix], 
                         'vec':[model.MongoMatrix],
                         'num_basis_functions':int,
                         'num_mos_variation':int,
                         'coord':[model.MongoMatrix], 
                         'energy':[float],
                         'normal_mode':{'atomic_mass':[float], 
                                                   'frequency':[float], 
                                                   'mode':model.MongoMatrix, 
                                                   }
                        }
    
    default_values = {
        '_type':u'GamessResult', 
    }

model.con.register([GamessResult])

def select_norb(result):    
    if result['num_mos_variation']:
        return result['num_mos_variation']
    else:
        return result['num_basis_functions']

class GamessParams(object):
    '''Holds the GAMESS run parameters'''
    def __init__(self):
        self.groups = dict()
        self.title = None
        self.r_orbitals = None
        self.r_hessian = None
        self.r_orbitals_norb = None
    
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
    
    def __cmp__(self, other):
        if isinstance(other, GamessParams):
            return cmp(other.groups, self.groups)
    
class GamessAtoms(ase.Atoms):
    # Shoenflies space group
    symmetry = None

    def copy(self):
        import copy
        new_atoms = GamessAtoms(Atoms.copy(self))
        new_atoms.symmetry= copy.deepcopy(self.symmetry)
        return new_atoms

class GamessParseDat(object):
    '''
    classdocs
    '''
    
    VEC = 'vec'
    HESS = 'hessian'
    COORD = 'coord'
    ENERGY = 'energy'
    GRAD = 'gradient'
    NORM_MODE = 'normal_mode'
    
    def __init__(self, group):
        '''
        Constructor
        '''
        
#        keys = [self.VEC, self.HESS, self.COORD, self.ENERGY, self.GRAD, self.NORM_MODE]
#        values = [[], [], [], [], [], []]
#        
#        self.group = dict(zip(keys, values))
        
        self.group = group
        
        self.parse_kernel = parsekernel.ParseKernel()
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
    
    def parse(self, f_dat):
        self.parse_kernel.parse(f_dat)
    
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
            
            to_save = model.MongoMatrix.create()
            to_save.matrix  = np.array(resultset, dtype=float)
            
            self.group[groupTitle].append(to_save)
    
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
                    key=vals[0:2]
                    value=vals[2:]
                    if last_key == (int(key[0]), int(key[1]) - 1):
                        resultset[-1].extend(value)
                    else:
                        fun_append(value)
                    last_key = (int(key[0]), int(key[1]))
            
            map(tuple, resultset)
            
            to_save = model.MongoMatrix.create()
            to_save.matrix  = np.array(resultset, dtype=float)
            
            self.group[groupTitle].append(to_save)
        
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
                    #Strip the list
                    vals = [x.strip() for x in vals]
                    resultset.append([vals[0:2], vals[2:]]) # Keys
            
            mat = np.array(np.zeros((len(resultset), 3)), dtype=float)
            for i in range(0, len(resultset)):
                mat[i] = resultset[i][1]
            
            to_save = model.MongoMatrix.create()
            to_save.matrix  = mat
            
            self.group[groupTitle].append(to_save)
    
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
            
            to_save = model.MongoMatrix.create()
            to_save.matrix  = np.array(result[1::2], dtype=float)
            
            self.group[groupTitle].append(to_save)
        
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
            
            self.group[groupTitle].append(float(result[0]))
    
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
            #Get ride of the first and last lines, it makes parsing easier
            text_block = '\n'.join(text_block.split('\n')[1:-2])
            rule=Literal('ATOMIC MASSES').suppress()+OneOrMore(Word(nums+'.'))        
            resultMass = rule.searchString(text_block).asList() #Atomic Masses
            rule=Literal('FREQUENCY=').suppress()+Word(nums+'-.')        
            resultFreq = rule.searchString(text_block).asList() #Frequencies
            rule=Literal('(CM**-1)').suppress()+OneOrMore(Word(nums+'-.E'))        
            resultMode = rule.searchString(text_block).asList() #Modes
#            resultset=dict()
#            resultset['ATOMIC_MASS']=resultMass[0]
#            resultset['FREQUENCY']=list()
#            for i in resultFreq: resultset['FREQUENCY'].append(i[0])
#            resultset['MODE']=resultMode
            self.group[groupTitle]['atomic_mass']=map(float, resultMass[0])
            self.group[groupTitle]['frequency'] = [float(v[0]) for v in resultFreq]
            self.group[groupTitle]['mode'] = model.MongoMatrix.create()
            self.group[groupTitle]['mode'].matrix = np.array(resultMode,dtype=float)

class GamessParseOut(object):
    '''
    classdocs
    '''
    STATUS_EXIT = 'exit_successful'
    STATUS_GEOM_LOCATED='geom_located'
    STATUS_NO_CPU_TIMEOUT = 'no_cpu_timeout'
    NUM_BASIS_FUNCTIONS = 'num_basis_functions'
    NUM_MOS_VARIATION = 'num_mos_variation'
    
    def __init__(self, group):
        '''
        Constructor
        '''
        self.group = group
        
        self.parse_kernel = parsekernel.ParseKernel()
        start,  end = self.read_status_cpu_timeout(returnRule=True)
        self.parse_kernel.addRule(start, end, self.read_status_cpu_timeout)        
        start,  end = self.read_status_exit(returnRule=True)
        self.parse_kernel.addRule(start, end, self.read_status_exit)
        start,  end = self.read_status_geom_located(returnRule=True)
        self.parse_kernel.addRule(start, end, self.read_status_geom_located)
        start,  end = self.read_num_basis_functions(returnRule=True)
        self.parse_kernel.addRule(start, end, self.read_num_basis_functions)
        start,  end = self.read_num_mos_variation(returnRule=True)
        self.parse_kernel.addRule(start, end, self.read_num_mos_variation)
    
    def parse(self, f_out):        
        self.parse_kernel.parse(f_out)
    
    def read_num_basis_functions(self, text_block=None, returnRule=False):
        if returnRule:
            #Define parse rule
            heading=r"""NUMBER OF CARTESIAN GAUSSIAN BASIS FUNCTIONS"""
            trailing=r"""NUMBER OF CARTESIAN GAUSSIAN BASIS FUNCTIONS"""
            return (heading, trailing)
        else:
            groupTitle = self.NUM_BASIS_FUNCTIONS
            num_ao = text_block.split('=')[-1]
            num_ao = num_ao.strip()
            num_ao = int(num_ao)
            self.group[groupTitle]=num_ao
        return         
    
    def read_num_mos_variation(self, text_block=None, returnRule=False):
        if returnRule:
            #Define parse rule
            heading=r"""TOTAL NUMBER OF MOS IN VARIATION SPACE"""
            trailing=r"""TOTAL NUMBER OF MOS IN VARIATION SPACE"""
            return (heading, trailing)
        else:
            groupTitle = self.NUM_MOS_VARIATION
            num_ao = text_block.split('=')[-1]
            num_ao = num_ao.strip()
            num_ao = int(num_ao)
            self.group[groupTitle]=num_ao
        return     
        
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
            else:
                self.group[groupTitle]=False
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
            else:
                self.group[groupTitle]=False
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
            else:
                self.group[groupTitle]=True
        return

class GamessWriteInp(object):
    """Create the file that specifies the GAMESS job
    
    Class is used to write GAMESS job files.
    """
    def build_coords(self, atoms, params):
        #We are only handling C1, no symmetry
        heading = ' $DATA\n'
        trailing = ' $END\n'        
        names = atoms.get_chemical_symbols()
        positions = atoms.get_positions()
        masses = atoms.get_masses()        
        output = heading + params.j_title + '\n' + atoms.symmetry +'\n'
        #GAMESS-US format requires a space after none C1 space groups!
        if atoms.symmetry != 'C1':
            output += '\n'
        for i in range(0, len(names)):
            output += '%s %f %.10f %.10f %.10f\n'\
                %(names[i],  masses[i], positions[i, 0], positions[i, 1], positions[i, 2])
        output +=trailing
        return output
        
    def build_params(self, params):
        """Returns a string of parameters in the way GAMESS 
        expects to see them in the inp file.
        
        $GROUP1 PARAM1=VALUE1 PARAM2=VALUE2 $END
        $GROUP2 PARAM3=VALUE1 PARAM4=VALUE2 $END
        """
        groups = params.groups
        output = ''
        for group_key, params_dict in groups.iteritems():
            output += ' %s '%(group_key)
            for param_key, param in params_dict.iteritems():
                section = '%s=%s '%(param_key, param)
                if len(output.rsplit('\n')[-1]) + len(section) >= 70:
                    output += '\n'
                output += section
            output += '%s\n'%('$END')
        return output
    
    def write(self, f_like, atoms, params):
        output = self.build_params(params)
        f_like.write(output)
        output = self.build_coords(atoms, params)
        f_like.write(output)
        self.write_vec(f_like, params)
        self.write_hess(f_like, params)
        f_like.flush()
    
    def write_vec(self, f_like, params):
        vec = params.r_orbitals
        if vec is None:
            return ''
        heading = ' $VEC'
        trailing = '\n $END\n'        
        f_like.write(heading)
        self.build_gamess_matrix(f_like, vec, params.r_orbitals_norb)
        f_like.write(trailing)
        
    def write_hess(self, f_like, params):
        hess =params.r_hessian
        if hess is None:
            return ''
        heading = ' $HESS\n'
        trailing = '\n $END\n'
        f_like.write(heading)
        self.build_gamess_matrix(f_like, hess)
        f_like.write(trailing)
    
    @staticmethod
    def build_gamess_matrix(f_like, mat, norb=None):
        #We can have up to 99 rows before starting over again with the numbering
        #We need to Change this number based on NORB for the VEC group only!!
        numCol = 5
        maxCol = 1000
        maxRow= 100
        outRow = 1
        outCol = 1
        if mat.__class__.__name__ == 'ndarray':
            format = '%15.8E'
        else:
            format = '%15s'
        for row in mat:
            split_rows=tuple(utils.split_seq(row, numCol))
            for row_to_print in split_rows:
                output = '\n%2s%3s'% (outRow, outCol % maxCol)
                f_like.write(output)
                output=''.join(format%i for i in row_to_print)
                f_like.write(output)
                outCol += 1;
            if norb:
                if outRow == norb:
                    outRow = 0
            outRow = (outRow + 1) % maxRow
            outCol=1
        return output

class GamessParseInp(object):
    '''
    classdocs
    '''   
    
    def __init__(self):
        '''
        Constructor
        '''
        # The DATA group in the INP file specifies atom positions.
        self.coords = None
        # The comment group in the INP file where users can write what they want
        self.title = None
        # The shoenflies space group
        self.symmetry = None
        # This holds all of the flags GAMESS uses to execute a job.
        self.params = None
        # This holds the GAMESS molecule
        self.atoms = None        

        self.parse_kernal = parsekernel.ParseKernel()
        start,  end = self.read_coords(returnRule=True)
        self.parse_kernal.addRule(start, end, self.read_coords)
        start,  end = self.read_params(returnRule=True)
        self.parse_kernal.addRule(start, end, self.read_params)
    
    def parse(self, f_inp):
        # Parse the file like object
        self.parse_kernal.parse(f_inp)
        self.params.j_title = self.title
    
    def get_atoms(self):        
        data = self.coords
        atom_list = list()
        for i in range(0,len(data)):            
            a_position = data[i][1]
            a_mass = data[i][0][1]
            a_name = data[i][0][0]
            atom_list.append(ase.Atom(symbol=a_name, mass=a_mass, position=a_position))
        atoms = GamessAtoms(atom_list)
        atoms.symmetry = self.symmetry
        return atoms
    
    def read_params(self, text_block=None, returnRule=False):
        """Read the params we are going to use to run this molecule.
        
        Example inp header:
        $CONTRL RUNTYP=OPTIMIZE SCFTYP=RHF DFTTYP=B97-D MAXIT=200 $END
        ! $GUESS GUESS=MOREAD NORB=649 $END        
         $DATA
            AATT ds stack, DZV(2d,2p)
        $END
        The heading matches lines that start with '$' followed by 
        'key=value' pairs. That way it will not match the '$DATA' group, nor
        comments, which are lines that start with '!'.
        """
        if not self.params:
            self.params = GamessParams()
        gamess_params=self.params
        if returnRule:
            #Define parse rule            
            heading=r"""^\s*\$\w+\s+[\w\.]+[=]{1}[\.\w]+"""
            trailing=r"""\$END"""
            return (heading, trailing)
        else:
            rule = (Word(alphas+'$')+OneOrMore(Group((Word(alphas+nums+'-')+Suppress(Literal('='))+Word(alphas+nums+'-.')))))
            result = rule.searchString(text_block).asList()[0]
            group_key = result[0]
            for i in result[1:]:
                param_key = i[0]
                param = i[1]
                gamess_params.set_group_param(group_key, param_key, param)                
            self.params=gamess_params
    
    def read_coords(self, text_block=None, returnRule=False):
        """Parsed the $DATA group in a GAMESS-US input file

            $DATA
            Methylene...1-A-1 state...RHF/STO-2G
            Cnv  2

            C           6.0      0.0000000000      0.0000000000     -0.0899124183
            H           1.0      0.8928757283      0.0000000000      0.5352858974
             $END
        """
        if returnRule:
            #Define parse rule
            heading=r"""\$DATA"""
            trailing=r"""\$END"""
            return (heading, trailing)
        else:
            lines=text_block.splitlines(True)
            # In GAMESS this is the comment line.
            # We are going to use it as the title.
            self.title = lines[1].strip()
            self.symmetry = lines[2].strip()
            new_text_block = ''.join(lines[3:])
            rule = OneOrMore(Group(Group(Word(alphas) + Word(nums+'.')) + Group(OneOrMore(Word(nums+'-.')))))
            result = rule.searchString(new_text_block).asList()[0]
            self.coords=result

class GamessParseErr(object):
    def __init__(self, result):
        pass
    
    def parse(self, f_out):        
        pass
    
class GamessApplication(application.Application):
    _result_file_mapping = {'dat':GamessParseDat, 'out':GamessParseOut, 'err':GamessParseErr}
    _input_file_mapping = {'inp': GamessParseInp}

    @classmethod
    def parse_result(cls, f_container):
        try:
            f_list = utils.verify_file_container(f_container)
            result = GamessResult.new()
            for a_file in f_list:
                parser = cls._result_file_mapping[(a_file.name).split('.')[-1]](result)
                parser.parse(a_file)
        finally:
            [f.close() for f in f_list]
        return result
    
    @classmethod
    def parse_input(cls, f_container):
        try:
            a_file = utils.verify_file_container(f_container)
            parser = cls._input_file_mapping[(a_file.name).split('.')[-1]]()
            parser.parse(a_file)
        finally:
            a_file.close()
        return (parser.get_atoms(), parser.params)
    
    @staticmethod
    def write_input(f_container, atoms, params):
        try:
            a_file = utils.verify_file_container(f_container, 'w')
            writer = GamessWriteInp()
            writer.write(a_file, atoms, params)
        finally:
            a_file.close()
    
    @staticmethod
    def build_gamess_matrix(f_container, mat):
        try:
            a_file = utils.verify_file_container(f_container, 'w')
            writer = GamessWriteInp()
            writer.build_gamess_matrix(a_file, mat)
        finally:
            a_file.close()

    @staticmethod
    def gc3_application(f_inp_path, requested_cores, requested_memory,  requested_walltime):
        from gc3utils import Application
        return Application.GamessApplication(input_file_path=f_inp_path, 
                                                                       requested_memory = requested_memory, 
                                                                       requested_cores = requested_cores, 
                                                                       requested_walltime = requested_walltime, 
                                                                       job_local_dir = os.path.expanduser('~/gc3_jobs')
                                                                    )
    
    @staticmethod
    def gc3_pass_through(*args, **kwargs):
        from gc3utils import Application
        return Application.Application(*args, **kwargs)
    
    @staticmethod
    def temp_application(requested_cores, requested_memory,  requested_walltime):
        return dict(requested_memory = requested_memory, 
                           requested_cores = requested_cores, 
                           requested_walltime = requested_walltime, 
                        )
