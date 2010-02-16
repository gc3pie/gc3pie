"""
This module contains an interface to I/O using the python binding of
the OpenBabel library ( http://openbabel.org/ ), supporting over 90
different chemical file formats. In order to avoid a hard dependency
on yet another library, the idea is to use this module as a fallback
in case none of the builtin methods work. 

The OpenBabel Python interface is described in

O'Boyle et al., Chem. Cent. J., 2, 5 (2008), doi:10.1186/1752-153X-2-5

This module has been contributed by Janne Blomqvist

"""

from ase.atoms import Atoms, Atom

def guess_format(filename, read=True):
    """Babel specific file format guesser.

    filename: str
        Name of file to guess format of.
    read: bool
        Are we trying to read; if False we are writing
    
    """
    if filename.endswith('.gz'):
        filename = filename[:-3]
    elif filename.endswith('.bz2'):
        filename = filename[:-4]
    lastdot = filename.rfind('.')
    if lastdot != -1:
        import pybel
        ext = filename[(lastdot+1):]
        if read:
            if ext in pybel.informats:
                return ext
        else:
            if ext in pybel.outformats:
                return ext
    return None

def read_babel(filename, format=None, index=-1):
    """Read a file containing one or more images using OpenBabel

    Returns the image given by the index argument.

    Doesn't try to get unit cell, pbc, constraint or such, only the
    atomic symbols and coordinates. Also, chemists and molecular
    biologists have the weird idea to overload chemical symbol names
    with charge states and whatever. However, ASE doesn't understand
    this, e.g. that the chemical symbol 'Me1' might mean the CH3 group
    in a methanol molecule, and hence symbol names might be messed up.

    """
    import pybel

    if format == None or format.lower() == 'babel':
        format = guess_format(filename, True)

    images = []
    for mol in pybel.readfile(format, filename):
        atoms = []
        for atom in mol.atoms:
            atoms.append(Atom(atom.atomicnum, atom.coords))
        images.append(Atoms(atoms))
    return images[index]

def write_babel(filename, images, format=None):
    """Write a set of images with OpenBabel
    
    Similar to the read_babel function, only cares about atomic
    symbols and coordinates.
    
    """
    import pybel
    import openbabel as ob

    if not isinstance(images, (list, tuple)):
        images = [images]

    if format == None:
        format = guess_format(filename, False)

    outfile = pybel.Outputfile(format, filename, overwrite=True)
    for image in images: # image is an ase.Atoms object
        mol = ob.OBMol()
        for atom in image:
            a = mol.NewAtom()
            a.SetAtomicNum(atom.number)
            c = atom.position
            a.SetVector(c[0], c[1], c[2])
        pmol = pybel.Molecule(mol)
        outfile.write(pmol)
    outfile.close()

if __name__ == '__main__':
    import sys
    a = read_babel(sys.argv[1])
    write_babel(sys.argv[2], a)
