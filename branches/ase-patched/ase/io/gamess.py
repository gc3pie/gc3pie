"""Classes to read and write GAMESS files
By: Mark Monroe
Email: monroe@oci.uzh.ch
"""
from pyparsing import *
import re

from ase import Atom
from ase.calculators.gamess import ParseKernal, GamessParams, GamessAtoms, MyUtilities

class WriteGamessInp:
    """Create the file that specifies the GAMESS job
    
    Class is used to write GAMESS job files.
    """
    def __init__(self):
        '''
        Constructor
        '''
        
    def build_coords(self, atoms):
        #We are only handling C1, no symmetry
        heading = ' $DATA\n'
        trailing = ' $END\n'        
        names = atoms.get_chemical_symbols()
        positions = atoms.get_positions()
        masses = atoms.get_masses()
        space_group=atoms.get_shoenflies_space_group()
        comment=atoms.get_comment()
        output = heading + comment + '\n' + space_group +'\n'
        #GAMESS-US format requires a space after none C1 space groups!
        if space_group != 'C1':
            output += '\n'
        for i in range(0, len(names)):
            output += '%s %f %.10f %.10f %.10f\n'\
                %(names[i],  masses[i], positions[i, 0], positions[i, 1], positions[i, 2])
        output +=trailing
        return output
        
    def build_params(self, atoms):
        """Returns a string of parameters in the way GAMESS 
        expects to see them in the inp file.
        
        $GROUP1 PARAM1=VALUE1 PARAM2=VALUE2 $END
        $GROUP2 PARAM3=VALUE1 PARAM4=VALUE2 $END
        """
        groups = atoms.get_calculator().gamess_params.get_groups()
        output = ''
        for group_key, params_dict in groups.iteritems():
            output += ' %s '%(group_key)
            for param_key, param in params_dict.iteritems():
                output += '%s=%s '%(param_key, param)
            output += '%s\n'%('$END')
        return output
    
    def write(self, filename, atoms):
        file = open(filename, 'w')
        inp_file = self.build_params(atoms)
        file.write(inp_file)
        inp_file = self.build_coords(atoms)
        file.write(inp_file)
        inp_file = self.write_vec(atoms, file)
        inp_file = self.write_hess(atoms, file)
        file.close()       
    
    def write_vec(self, atoms, file):
        vec = atoms.get_calculator().gamess_params.get_orbitals()
        if not vec:
            return ''
        heading = ' $VEC'
        trailing = '\n $END\n'        
        file.write(heading)
        self.build_gamess_matrix(vec, file)
        file.write(trailing)
        
    def write_hess(self, atoms, file):
        hess = atoms.get_calculator().gamess_params.get_hessian()
        if not hess:
            return ''
        heading = ' $HESS\n'
        trailing = '\n $END\n'
        file.write(heading)
        self.build_gamess_matrix(hess, file)
        file.write(trailing)
        
    @staticmethod
    def build_gamess_matrix(mat, file):
        numCol = 5
        maxRow = 100 #We can have up to 99 rows before starting over again with the numbering
        maxCol = 1000
        outRow = 1
        outCol = 1
        for row in mat:
            split_rows=tuple(MyUtilities.split_seq(row, numCol))
            for row_to_print in split_rows:
                output = '\n%2s%3s'% (outRow, outCol % maxCol)
                file.write(output)
                output=''.join('%15s'%i for i in row_to_print)
                file.write(output)
                outCol += 1;                 
            outRow = (outRow + 1) % maxRow
            outCol=1
        return output

class ReadGamessInp:
    '''
    classdocs
    '''
    #Group titles used to reference values in the group dictionary
    DATA = 'DATA'
    COMMENT = 'COMMENT'
    SYMMETRY = 'SYMMETRY'
    PARAMS = 'PARAMS'
    
    def __init__(self):
        '''
        Constructor
        '''
        self.group = dict()
        self.parse_kernal = ParseKernal()
        start,  end = self.read_coords(returnRule=True)
        self.parse_kernal.addRule(start, end, self.read_coords)
        start,  end = self.read_params(returnRule=True)
        self.parse_kernal.addRule(start, end, self.read_params)
        
    def read_file(self, inp):
        self.parse_kernal.parse(inp)
    
    def get_result_group(self):
        return self.group
    
    def get_coords(self):
        """The data group in the INP file specifies atom positions.""" 
        return self.group[self.DATA]
    
    def get_params(self):
        """This will hold all of the flags GAMESS uses to execute a job.""" 
        return self.group[self.PARAMS]
    
    def get_molecule(self):        
        data = self.get_coords()
        atom_list = list()
        for i in range(0,len(data)):            
            a_position = data[i][1]
            a_mass = data[i][0][1]
            a_name = data[i][0][0]
            atom_list.append(Atom(symbol=a_name, mass=a_mass, position=a_position))
        atoms = GamessAtoms(atom_list)
        atoms.set_comment(self.group[self.COMMENT])
        atoms.set_shoenflies_space_group(self.group[self.SYMMETRY])
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
        if not self.PARAMS in self.group:
            self.group[self.PARAMS] = GamessParams()
        gamess_params=self.group[self.PARAMS]        
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
            self.group[self.PARAMS]=gamess_params

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
            self.group[self.COMMENT] = lines[1].strip()
            self.group[self.SYMMETRY] = lines[2].strip()
            new_text_block = ''.join(lines[3:])
            rule = OneOrMore(Group(Group(Word(alphas) + Word(nums+'.')) + Group(OneOrMore(Word(nums+'-.')))))
            result = rule.searchString(new_text_block).asList()[0]
            self.group[self.DATA]=result
            

