from ase.atoms import Atoms
from ase.units import Bohr


def read_turbomole(filename='coord'):
    """Method to read turbomole coord file
    
    coords in bohr, atom types in lowercase, format:
    $coord
    x y z atomtype 
    x y z atomtype f
    $end
    Above 'f' means a fixed atom.
    """
    from ase import Atoms, Atom
    from ase.constraints import FixAtoms

    if isinstance(filename, str):
        f = open(filename)

    lines = f.readlines()
    atoms_pos = []
    atom_symbols = []
    dollar_count=0
    myconstraints=[]
    for line in lines:
        if ('$' in line):
            dollar_count = dollar_count + 1
            if (dollar_count >= 2):
                break
        else:
            x, y, z, symbolraw = line.split()[:4]
            symbolshort=symbolraw.strip()
            symbol=symbolshort[0].upper()+symbolshort[1:].lower()
            #print symbol
            atom_symbols.append(symbol)
            atoms_pos.append([float(x)*Bohr, float(y)*Bohr, float(z)*Bohr])
            cols = line.split()
            if (len(cols) == 5):
                fixedstr = line.split()[4].strip()
                if (fixedstr == "f"):
                    myconstraints.append(True)
                else:
                    myconstraints.append(False)
            else:
                myconstraints.append(False)
            
    if type(filename) == str:
        f.close()

    atoms = Atoms(positions = atoms_pos, symbols = atom_symbols, pbc = False)
    c = FixAtoms(myconstraints)
    atoms.set_constraint(c)
    #print c
    

    return atoms

def write_turbomole(filename, atoms):
    """Method to write turbomole coord file
    """

    import numpy as np
    from ase.constraints import FixAtoms

    if isinstance(filename, str):
        f = open(filename, 'w')
    else: # Assume it's a 'file-like object'
        f = filename

    coord = atoms.get_positions()
    symbols = atoms.get_chemical_symbols()
    printfixed = False

    if atoms.constraints:
        for constr in atoms.constraints:
            if isinstance(constr, FixAtoms):
                fix_index=constr.index
                printfixed=True
    #print sflags
        
    if (printfixed):
        fix_str=[]
        for i in fix_index:
            if i == 1:
                fix_str.append("f")
            else:
                fix_str.append(" ")


    f.write("$coord\n")
    if (printfixed):
        for (x, y, z), s, fix in zip(coord,symbols,fix_str):
            f.write('%20.14f  %20.14f  %20.14f      %2s  %2s \n' 
                    % (x/Bohr, y/Bohr, z/Bohr, s.lower(), fix))

    else:
        for (x, y, z), s in zip(coord,symbols):
            f.write('%20.14f  %20.14f  %20.14f      %2s \n' 
                    % (x/Bohr, y/Bohr, z/Bohr, s.lower()))
    f.write("$end\n")
    
