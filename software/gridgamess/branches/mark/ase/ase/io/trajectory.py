import os
import pickle

from ase.calculators import SinglePointCalculator
from ase.atoms import Atoms
from ase.parallel import rank
from ase.utils import devnull
from ase.neb import NEB


class PickleTrajectory:
    "Reads/writes Atoms objects into a .traj file."
    # Per default, write these quantities
    write_energy = True
    write_forces = True
    write_stress = True
    write_magmoms = True
    write_momenta = True
    
    def __init__(self, filename, mode='r', atoms=None, master=None,
                 backup=True):
        """A PickleTrajectory can be created in read, write or append mode.

        Parameters:

        filename:
            The name of the parameter file.  Should end in .traj.

        mode='r':
            The mode.

            'r' is read mode, the file should already exist, and
            no atoms argument should be specified.

            'w' is write mode.  If the file already exists, is it
            renamed by appending .bak to the file name.  The atoms
            argument specifies the Atoms object to be written to the
            file, if not given it must instead be given as an argument
            to the write() method.

            'a' is append mode.  It acts a write mode, except that
            data is appended to a preexisting file.

        atoms=None:
            The Atoms object to be written in write or append mode.

        master=None:
            Controls which process does the actual writing. The
            default is that process number 0 does this.  If this
            argument is given, processes where it is True will write.

        backup=True:
            Use backup=False to disable renaming of an existing file.
        """
        self.offsets = []
        if master is None:
            master = (rank == 0)
        self.master = master
        self.backup = backup
        self.set_atoms(atoms)
        self.open(filename, mode)

    def open(self, filename, mode):
        """Opens the file.

        For internal use only.
        """
        self.fd = filename
        if mode == 'r':
            if isinstance(filename, str):
                self.fd = open(filename, mode + 'b')
            self.read_header()
        elif mode == 'a':
            exists = True
            if isinstance(filename, str):
                exists = os.path.isfile(filename)
                self.fd = open(filename, mode + 'b+')
            if exists:
                self.read_header()
        elif mode == 'w':
            if self.master:
                if isinstance(filename, str):
                    if self.backup and os.path.isfile(filename):
                        os.rename(filename, filename + '.bak')
                    self.fd = open(filename, 'wb')
            else:
                self.fd = devnull
        else:
            raise ValueError('mode must be "r", "w" or "a".')

    def set_atoms(self, atoms=None):
        """Associate an Atoms object with the trajectory.

        Mostly for internal use.
        """
        if atoms is not None and not hasattr(atoms, 'get_positions'):
            raise TypeError('"atoms" argument is not an Atoms object.')
        self.atoms = atoms

    def read_header(self):
        try:
            if self.fd.read(len('PickleTrajectory')) != 'PickleTrajectory':
                raise IOError('This is not a trajectory file!')
            d = pickle.load(self.fd)
        except EOFError:
            raise EOFError('Bad trajectory file.')
        self.pbc = d['pbc']
        self.numbers = d['numbers']
        self.tags = d.get('tags')
        self.masses = d.get('masses')
        self.constraints = d['constraints']
        self.offsets.append(self.fd.tell())

    def write(self, atoms=None):
        """Write the atoms to the file.

        If the atoms argument is not given, the atoms object specified
        when creating the trajectory object is used.
        """
        if atoms is None:
            atoms = self.atoms

        if hasattr(atoms, 'interpolate'):
            # seems to be a NEB
            neb = atoms
            try:
                neb.get_energies_and_forces(all=True)
            except AttributeError:
                pass
            for image in neb.images:
                self.write(image)
            return

        if len(self.offsets) == 0:
            self.write_header(atoms)

        if atoms.has('momenta'):
            momenta = atoms.get_momenta()
        else:
            momenta = None

        d = {'positions': atoms.get_positions(),
             'cell': atoms.get_cell(),
             'momenta': momenta}

        if atoms.get_calculator() is not None:
            if self.write_energy:
                d['energy'] = atoms.get_potential_energy()
            if self.write_forces:
                assert self.write_energy
                try:
                    d['forces'] = atoms.get_forces(apply_constraint=False)
                except NotImplementedError:
                    pass
            if self.write_stress:
                assert self.write_energy
                try:
                    d['stress'] = atoms.get_stress()
                except NotImplementedError:
                    pass

            if self.write_magmoms:
                try:
                    if atoms.calc.get_spin_polarized():
                        d['magmoms'] = atoms.get_magnetic_moments()
                except (NotImplementedError, AttributeError):
                    pass

        if 'magmoms' not in d and atoms.has('magmoms'):
            d['magmoms'] = atoms.get_initial_magnetic_moments()
            
        if self.master:
            pickle.dump(d, self.fd, protocol=-1)
        self.fd.flush()
        self.offsets.append(self.fd.tell())

    def write_header(self, atoms):
        self.fd.write('PickleTrajectory')
        if atoms.has('tags'):
            tags = atoms.get_tags()
        else:
            tags = None
        if atoms.has('masses'):
            masses = atoms.get_masses()
        else:
            masses = None
        d = {'pbc': atoms.get_pbc(),
             'numbers': atoms.get_atomic_numbers(),
             'tags': tags,
             'masses': masses,
             'constraints': atoms.constraints}
        pickle.dump(d, self.fd, protocol=-1)
        self.header_written = True
        self.offsets.append(self.fd.tell())
        
    def close(self):
        """Close the trajectory file."""
        self.fd.close()

    def __getitem__(self, i=-1):
        N = len(self.offsets)
        if 0 <= i < N:
            self.fd.seek(self.offsets[i])
            try:
                d = pickle.load(self.fd)
            except EOFError:
                raise IndexError
            if i == N - 1:
                self.offsets.append(self.fd.tell())
            try:
                magmoms = d['magmoms']
            except KeyError:
                magmoms = None    
            atoms = Atoms(positions=d['positions'],
                          numbers=self.numbers,
                          cell=d['cell'],
                          momenta=d['momenta'],
                          magmoms=magmoms,
                          tags=self.tags,
                          masses=self.masses,
                          pbc=self.pbc,
                          constraint=[c.copy() for c in self.constraints])
            if 'energy' in d:
                calc = SinglePointCalculator(
                    d.get('energy', None), d.get('forces', None),
                    d.get('stress', None), magmoms, atoms)
                atoms.set_calculator(calc)
            return atoms

        if i >= N:
            for j in range(N - 1, i + 1):
                atoms = self[j]
            return atoms

        i = len(self) + i
        if i < 0:
            raise IndexError('Trajectory index out of range.')
        return self[i]

    def __len__(self):
        N = len(self.offsets) - 1
        while True:
            self.fd.seek(self.offsets[N])
            try:
                pickle.load(self.fd)
            except EOFError:
                return N
            self.offsets.append(self.fd.tell())
            N += 1

    def __iter__(self):
        del self.offsets[1:]
        return self

    def next(self):
        try:
            return self[len(self.offsets) - 1]
        except IndexError:
            raise StopIteration


def read_trajectory(filename, index=-1):
    traj = PickleTrajectory(filename, mode='r')

    if isinstance(index, int):
        return traj[index]
    else:
        # Here, we try to read only the configurations we need to read
        # and len(traj) should only be called if we need to as it will
        # read all configurations!

        # XXX there must be a simpler way?
        step = index.step or 1
        if step > 0:
            start = index.start or 0
            if start < 0:
                start += len(traj)
            stop = index.stop or len(traj)
            if stop < 0:
                stop += len(traj)
        else:
            if index.start is None:
                start = len(traj) - 1
            else:
                start = index.start
                if start < 0:
                    start += len(traj)
            if index.stop is None:
                stop = -1
            else:
                stop = index.stop
                if stop < 0:
                    stop += len(traj)
                    
        return [traj[i] for i in range(start, stop, step)]

def write_trajectory(filename, images):
    """Write image(s) to trajectory.

    Write also energy, forces, and stress if they are already
    calculated."""

    traj = PickleTrajectory(filename, mode='w')

    if not isinstance(images, (list, tuple)):
        images = [images]
        
    for atoms in images:
        # Avoid potentially expensive calculations:
        calc = atoms.get_calculator()
        if calc is not None:
            if  hasattr(calc, 'calculation_required'):
                if calc.calculation_required(atoms, ['energy']):
                    traj.write_energy = False
                if calc.calculation_required(atoms, ['forces']):
                    traj.write_forces = False
                if calc.calculation_required(atoms, ['stress']):
                    traj.write_stress = False
                if calc.calculation_required(atoms, ['magmoms']):
                    traj.write_magmoms = False
        else:
            traj.write_energy = False
            traj.write_forces = False
            traj.write_stress = False
            traj.write_magmoms = False
            
        traj.write(atoms)
    traj.close()
