
def read_aims(filename):
    """Import FHI-aims geometry type files.

    Reads unitcell, atom positions and constraints from
    a geometry.in file.
    """

    from ase import Atoms, FixAtoms, FixCartesian
    import numpy as np

    atoms = Atoms()
    fd = open(filename, 'r')
    lines = fd.readlines()
    fd.close()
    positions = []
    cell = []
    symbols = []
    fix = []
    fix_cart = []
    xyz = np.array([0, 0, 0])
    i = -1
    n_periodic = -1
    periodic = np.array([False, False, False])
    for n, line in enumerate(lines):
        inp = line.split()
        if inp == []:
            continue
        if inp[0] == 'atom':
            if xyz.all():
                fix.append(i)
            elif xyz.any():
                print 1
                fix_cart.append(FixCartesian(i, xyz))
            floatvect = float(inp[1]), float(inp[2]), float(inp[3])
            positions.append(floatvect)
            symbols.append(inp[-1])
            i += 1
            xyz = np.array([0, 0, 0])
        elif inp[0] == 'lattice_vector':
            floatvect = float(inp[1]), float(inp[2]), float(inp[3])
            cell.append(floatvect)
            n_periodic = n_periodic + 1
            periodic[n_periodic] = True
        if inp[0] == 'constrain_relaxation':
            if inp[1] == '.true.':
                fix.append(i)
            elif inp[1] == 'x':
                xyz[0] = 1
            elif inp[1] == 'y':
                xyz[1] = 1
            elif inp[1] == 'z':
                xyz[2] = 1
    if xyz.all():
        fix.append(i)
    elif xyz.any():
        fix_cart.append(FixCartesian(i, xyz))
    atoms = Atoms(symbols, positions)
    if periodic.all():
        atoms.set_cell(cell)
        atoms.set_pbc(periodic)
    if len(fix):
        atoms.set_constraint([FixAtoms(indices=fix)]+fix_cart)
    else:
        atoms.set_constraint(fix_cart)
    return atoms

def write_aims(filename, atoms):
    """Method to write FHI-aims geometry files.

    Writes the atoms positions and constraints (only FixAtoms is
    supported at the moment). 
    """

    from ase.constraints import FixAtoms, FixCartesian
    import numpy as np

    fd = open(filename, 'w')
    i = 0
    if atoms.get_pbc().any():
        for n, vector in enumerate(atoms.get_cell()):
            fd.write('lattice_vector ')
            for i in range(3):
                fd.write('%16.16f ' % vector[i])
            fd.write('\n')
    fix_cart = np.zeros([len(atoms),3]) 

    if atoms.constraints:
        for constr in atoms.constraints:
            if isinstance(constr, FixAtoms):
                fix_cart[constr.index] = [1,1,1]
            elif isinstance(constr, FixCartesian):
                fix_cart[constr.a] = -constr.mask+1

    for i, atom in enumerate(atoms):
        fd.write('atom ')
        for pos in atom.get_position():
            fd.write('%16.16f ' % pos)
        fd.write(atom.symbol)
        fd.write('\n')
# (1) all coords are constrained:
        if fix_cart[i].all():
            fd.write('constrain_relaxation .true.\n')
# (2) some coords are constrained:
        elif fix_cart[i].any():
            xyz = fix_cart[i]
            for n in range(3):
                if xyz[n]:
                    fd.write('constraint_relaxation %s\n' % 'xyz'[n])
        if atom.charge:
            fd.write('initial_charge %16.6f\n' % atom.charge)
        if atom.magmom:
            fd.write('initial_moment %16.6f\n' % atom.magmom)
# except KeyError:
#     continue

def read_energy(filename):
    for line in open(filename, 'r'):
        if line.startswith('  | Total energy corrected'):
            E = float(line.split()[-2])
    return E

def read_aims_output(filename):
    """  Import FHI-aims output files with all data available, i.e. relaxations, 
    MD information, force information etc etc etc. """
    from ase import Atoms, Atom 
    from ase.calculators import SinglePointCalculator
    fd = open(filename, 'r')
    cell = []
    images = []
    n_periodic = -1
    f = None
    pbc = False
    while True:
        line = fd.readline()
        if not line:
            break
        if "Number of atoms" in line:
            inp = line.split()
            n_atoms = int(inp[5])
        if "| Unit cell:" in line:
            if not pbc:
                pbc = True
                for i in range(3):
                    inp = fd.readline().split()
                    cell.append([inp[1],inp[2],inp[3]])
        if "Atomic structure:" in line:
            fd.readline()
            atoms = Atoms()
            for i in range(n_atoms):
                inp = fd.readline().split()
                atoms.append(Atom(inp[3],(inp[4],inp[5],inp[6]))) 
        if "Updated atomic structure:" in line:
            fd.readline()
            atoms = Atoms()
            for i in range(n_atoms):
                inp = fd.readline().split()
                atoms.append(Atom(inp[4],(inp[1],inp[2],inp[3])))                 
        if "Total atomic forces" in line:
            f = []
            for i in range(n_atoms):
                inp = fd.readline().split()
                f.append([float(inp[2]),float(inp[3]),float(inp[4])])
            e = images[-1].get_potential_energy()
            images[-1].set_calculator(SinglePointCalculator(e,f,None,None,atoms))
            e = None
            f = None
        if "Total energy corrected" in line:
            e = float(line.split()[5])
            if pbc:
                atoms.set_cell(cell)
                atoms.pbc = True
            atoms.set_calculator(SinglePointCalculator(e,None,None,None,atoms))
            images.append(atoms)
            e = None
    fd.close()
    return images

