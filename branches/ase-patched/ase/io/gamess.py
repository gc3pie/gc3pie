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
    def build_coords(self, atoms):
        #We are only handling C1, no symmetry
        heading = ' $DATA\n'
        trailing = ' $END\n'        
        names = atoms.get_chemical_symbols()
        positions = atoms.get_positions()
        masses = atoms.get_masses()        
        output = heading + atoms.comment + '\n' + atoms.symmetry +'\n'
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
    
    def write(self, file_like, atoms):
        output = self.build_params(atoms)
        file_like.write(output)
        output = self.build_coords(atoms)
        file_like.write(output)
        self.write_vec(atoms, file_like)
        self.write_hess(atoms, file_like)
        file_like.flush()
    
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
    # The DATA group in the INP file specifies atom positions.
    g_coords = None
    # The comment group in the INP file where users can write what they want
    g_comment = None
    # The shoenflies space group
    g_symmetry = None
    # This will hold all of the flags GAMESS uses to execute a job.
    g_params = None
    
    def __init__(self, f_inp):
        '''
        Constructor
        '''        
        self.parse_kernal = ParseKernal()
        start,  end = self.read_coords(returnRule=True)
        self.parse_kernal.addRule(start, end, self.read_coords)
        start,  end = self.read_params(returnRule=True)
        self.parse_kernal.addRule(start, end, self.read_params)
        # Parse the file like object
        self.parse_kernal.parse(f_inp)
    
    def get_molecule(self):        
        data = self.g_coords
        atom_list = list()
        for i in range(0,len(data)):            
            a_position = data[i][1]
            a_mass = data[i][0][1]
            a_name = data[i][0][0]
            atom_list.append(Atom(symbol=a_name, mass=a_mass, position=a_position))
        atoms = GamessAtoms(atom_list)
        atoms.set_comment(self.comment)
        atoms.set_shoenflies_space_group(self.g_symmetry)
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
        if not self.g_params:
            self.g_params = GamessParams()
        gamess_params=self.g_params
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
            self.g_params=gamess_params

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
            self.g_comment = lines[1].strip()
            self.g_symmetry = lines[2].strip()
            new_text_block = ''.join(lines[3:])
            rule = OneOrMore(Group(Group(Word(alphas) + Word(nums+'.')) + Group(OneOrMore(Word(nums+'-.')))))
            result = rule.searchString(new_text_block).asList()[0]
            self.g_coords=result
