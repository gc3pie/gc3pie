from ase.atoms import Atoms


def write_py(fileobj, images):
    if isinstance(fileobj, str):
        fileobj = open(fileobj, 'w')

    fileobj.write('from ase import *\n')
    
    if not isinstance(images, (list, tuple)):
        images = [images]
        fileobj.write('atoms = ')
    else:
        fileobj.write('images = [')

    symbols = images[0].get_chemical_symbols()
    natoms = len(symbols)
    for atoms in images:
        fileobj.write('%d\n\n' % natoms)
        for s, (x, y, z) in zip(symbols, atoms.get_positions()):
            fileobj.write('%-2s %22.15f %22.15f %22.15f\n' % (s, x, y, z))
