"""Classes to read and write GAMESS files
By: Mark Monroe
Email: monroe@oci.uzh.ch
"""
from pyparsing import *
import re

from ase import Atom, Atoms
from ase.calculators.gamess import ParseKernal, GamessParams

class WriteGamessInp:
    """Create the file that specifies the GAMESS job
    
    Class is used to write GAMESS job files.
    """
    def __init__(self):
        '''
        Constructor
        '''
    
    def build_vec(self, atoms):
        vec = atoms.get_calculator().get_orbitals(atoms, raw_format=True)
        if not vec:
            return ''
        heading = ' $VEC'
        trailing = '\n $END\n'        
        gamess_mat = self.build_gamess_matrix(vec)
        gamess_mat = heading + gamess_mat + trailing
        return gamess_mat
        
    def build_hess(self, atoms):
        hess = atoms.get_calculator().get_hessian(atoms, raw_format=True)
        if not hess:
            return ''
        heading = ' $HESS\n'
        trailing = '\n $END\n'        
        gamessMat = self.build_gamess_matrix(hess)
        gamessMat = heading + gamess_mat + trailing
        return gamess_mat
        
    def build_coords(self, atoms):
        #We are only handling C1, no symmetry
        heading = ' $DATA\nMade By ase\nC1\n'
        trailing = ' $END\n'        
        names = atoms.get_chemical_symbols()
        positions = atoms.get_positions()
        masses = atoms.get_masses()
        output = heading
        for i in range(0, len(names)):
            output += '%s %f %.10f %.10f %.10f\n'\
                %(names[i],  masses[i], positions[i, 0], positions[i, 1], positions[i, 2])
        output +=trailing
        return output

    def build_params(self, atoms):
        """The params used to run calcs on this molecule"""
        return atoms.get_calculator().gamess_params.build_params()
    
    def write(self, filename, atoms):
        inp_file = self.build_params(atoms) + self.build_coords(atoms) + self.build_vec(atoms) + self.build_hess(atoms)
        file = open(filename, 'w')
        file.write(inp_file)
        file.close()       
    
    @staticmethod
    def build_gamess_matrix(mat):
        numCol = 5
        maxRow = 100 #We can have up to 99 rows before starting over again with the numbering
        maxCol = 1000
        outRow = 1
        outCol = 1
        output = ''
        countCol = 0
        for row in mat:        
            for col in row:
                if countCol % numCol == 0:
                    output +='\n%2s%3s'% (outRow, outCol % maxCol) #First two cols are row index (max 99), next three are col index (max 999)
                    outCol += 1;                 
                output += '%15s' % col #the numbers are only 14 chars + a minus sign '% 9.8E'
                countCol +=1
            outRow = (outRow + 1) % maxRow
            countCol = 0
            outCol=1
        return output

class ReadGamessInp:
    '''
    classdocs
    '''
    #Group titles used to reference values in the group dictionary
    DATA = 'DATA'
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
        
    def read_file(self, filename):
        self.parse_kernal.parse(filename)
    
    def get_result_group(self):
        return self.group
    
    def get_coords(self):
        """The data group in the INP file specifies atom positions.""" 
        return self.group[self.DATA]
    
    def get_params(self):
        """This will hold all of the flags GAMESS uses to execute a job.""" 
        if not self.PARAMS in self.group:
            self.group[self.PARAMS]=GamessParams()
        return self.group[self.PARAMS]
    
    def get_molecule(self):        
        data = self.get_coords()
        atom_list = list()
        for i in range(0,len(data)):            
            a_position = data[i][1]
            a_mass = data[i][0][1]
            a_name = data[i][0][0]
            atom_list.append(Atom(symbol=a_name, mass=a_mass, position=a_position))
        atoms = Atoms(atom_list)
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
        gamess_params=self.get_params()        
        if returnRule:
            #Define parse rule            
            heading=r"""^\s*\$\w+\s[\w\.]+[=]{1}[\.\w]+"""
            trailing=r"""\$END"""
            return (heading, trailing)
        else:
            groupTitle = self.PARAMS
            rule = (Word(alphas+'$')+OneOrMore(Group((Word(alphas+nums+'-')+Suppress(Literal('='))+Word(alphas+nums+'-.')))))
            result = rule.searchString(text_block).asList()[0]
            group_key = result[0]
            for i in result[1:]:
                param_key = i[0]
                param = i[1]
                gamess_params.set_group_param(group_key, param_key, param)                
            self.group[groupTitle]=gamess_params

    def read_coords(self, text_block=None, returnRule=False):
        if returnRule:
            #Define parse rule
            heading=r"""\$DATA"""
            trailing=r"""\$END"""
            return (heading, trailing)
        else:
            groupTitle = self.DATA
            rule = OneOrMore(Group(Group(Word(alphas) + Word(nums+'.')) + Group(OneOrMore(Word(nums+'-.')))))
            result = rule.searchString(text_block).asList()[0]
            self.group[groupTitle]=result
        
        
#        file = open(self.fileTemplate)
#        strTemplate = file.read()
#        file.close()
#        header = self.getInpHeader(strTemplate)    
#    
#    def getInpHeader(self, strTemplate):
#        #First element is the group name, second is the comment
#        heading = Combine(Literal('$DATA')+(restOfLine+LineEnd()))
#        comment = restOfLine + LineEnd()
#        """We need to handle the case when we have a space between the symmetry and the atoms
#        and when we do not. That is why we match two line endings OR a single line ending"""
#        sym = (restOfLine + ((LineEnd() + LineEnd())|(LineEnd()))) 
#        atoms = Group(OneOrMore(Group(Word(alphas) + Word(nums+'.')) + Group(OneOrMore(Word(nums+'-.', max=13))))).setParseAction(self.replaceCoord)
#        trailing = Literal('$END')
#        rule = heading + comment + sym + atoms + trailing        
#        return rule.transformString(strTemplate)
#    
#    def replaceCoord(self, s, l, t):
#        """Used to replace the coords in the restart.inp file
#        """
#        return self.getCoord()
#    
#    def getCoord(self):        
#        output = ''
#        for section in self.coords:
#            output+='%s \n'%' '.join(MyUtilities.flatten(section))                           
#        return output
