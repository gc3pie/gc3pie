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
                output += '%s=%s '%(param_key, param)
            output += '%s\n'%('$END')
        return output
    
    def write(self, file_like, atoms, params):
        output = self.build_params(params)
        file_like.write(output)
        output = self.build_coords(atoms, params)
        file_like.write(output)
        self.write_vec(params, file_like)
        self.write_hess(params, file_like)
        file_like.flush()
    
    def write_vec(self, params, file_like):
        vec = params.r_orbitals
        if not vec:
            return ''
        heading = ' $VEC'
        trailing = '\n $END\n'        
        file_like.write(heading)
        self.build_gamess_matrix(vec, file_like)
        file_like.write(trailing)
        
    def write_hess(self, params, file_like):
        hess =params.r_hessian
        if not hess:
            return ''
        heading = ' $HESS\n'
        trailing = '\n $END\n'
        file_like.write(heading)
        self.build_gamess_matrix(hess, file_like)
        file_like.write(trailing)

    @staticmethod
    def build_gamess_matrix(mat, file_like):
        numCol = 5
        maxRow = 100 #We can have up to 99 rows before starting over again with the numbering
        maxCol = 1000
        outRow = 1
        outCol = 1
        if mat.__class__.__name__ == 'ndarray':
            format = '%15.8E'
        else:
            format = '%15s'
        for row in mat:
            split_rows=tuple(MyUtilities.split_seq(row, numCol))
            for row_to_print in split_rows:
                output = '\n%2s%3s'% (outRow, outCol % maxCol)
                file_like.write(output)
                output=''.join(format%i for i in row_to_print)
                file_like.write(output)
                outCol += 1;                 
            outRow = (outRow + 1) % maxRow
            outCol=1
        return output

class ReadGamessInp:
    '''
    classdocs
    '''   
    # The DATA group in the INP file specifies atom positions.
    coords = None
    # The comment group in the INP file where users can write what they want
    title = None
    # The shoenflies space group
    symmetry = None
    # This holds all of the flags GAMESS uses to execute a job.
    params = None
    # This holds the GAMESS molecule
    atoms = None
    
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
        self.atoms = self.get_atoms()
        self.params.j_title = self.title

    def get_atoms(self):        
        data = self.coords
        atom_list = list()
        for i in range(0,len(data)):            
            a_position = data[i][1]
            a_mass = data[i][0][1]
            a_name = data[i][0][0]
            atom_list.append(Atom(symbol=a_name, mass=a_mass, position=a_position))
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
