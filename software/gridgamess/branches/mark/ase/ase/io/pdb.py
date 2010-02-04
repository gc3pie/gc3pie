import numpy as np 

from ase.atoms import Atom, Atoms
from ase.parallel import paropen

"""Module to read and write atoms in PDB file format"""


def read_pdb(fileobj, index=-1):
    """Read PDB files.

    The format is assumed to follow the description given in
    http://www.wwpdb.org/documentation/format32/sect9.html."""
    if isinstance(fileobj, str):
        fileobj = open(fileobj)

    atoms = Atoms()
    for line in fileobj.readlines():
        if line.startswith('ATOM') or line.startswith('HETATM'):
            try:
                symbol = line[12:16].strip()
                # we assume that the second character is a label 
                # in case that it is upper case
                if len(symbol) > 1 and symbol[1].isupper():
                    symbol = symbol[0]
                words = line[30:55].split()
                position = np.array([float(words[0]), 
                                     float(words[1]),
                                     float(words[2])])
                atoms.append(Atom(symbol, position))
            except:
                pass

    return atoms

def write_pdb(fileobj, images):
    """Write images to PDB-file."""
    if isinstance(fileobj, str):
        fileobj = paropen(fileobj, 'w')

    if not isinstance(images, (list, tuple)):
        images = [images]

    format = 'ATOM  %5d %-4s              %8.3f%8.3f%8.3f  0.00  0.00\n'

    # RasMol complains if the atom index exceeds 100000. There might
    # be a limit of 5 digit numbers in this field.
    MAXNUM = 100000

    symbols = images[0].get_chemical_symbols()
    natoms = len(symbols)
    
    for atoms in images:
        fileobj.write('MODEL         1\n')
        p = atoms.get_positions()
        for a in range(natoms):
            x, y, z = p[a]
            fileobj.write(format % (a % MAXNUM, symbols[a], x, y, z))
        fileobj.write('ENDMDL\n')
