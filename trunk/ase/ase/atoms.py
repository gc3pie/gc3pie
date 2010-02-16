# Copyright 2008, 2009 CAMd
# (see accompanying license files for details).

"""Definition of the Atoms class.

This module defines the central object in the ASE package: the Atoms
object.
"""

from math import cos, sin

import numpy as np

from ase.atom import Atom
from ase.data import atomic_numbers, chemical_symbols, atomic_masses

class Atoms(object):
    """Atoms object.
    
    The Atoms object can represent an isolated molecule, or a
    periodically repeated structure.  It has a unit cell and
    there may be periodic boundary conditions along any of the three
    unit cell axes.

    Information about the atoms (atomic numbers and position) is
    stored in ndarrays.  Optionally, there can be information about
    tags, momenta, masses, magnetic moments and charges.

    In order to calculate energies, forces and stresses, a calculator
    object has to attached to the atoms object.

    Parameters:

    symbols: str (formula) or list of str
        Can be a string formula, a list of symbols or a list of
        Atom objects.  Examples: 'H2O', 'COPt12', ['H', 'H', 'O'],
        [Atom('Ne', (x, y, z)), ...].
    positions: list of xyz-positions
        Atomic positions.  Anything that can be converted to an
        ndarray of shape (n, 3) will do: [(x1,y1,z1), (x2,y2,z2),
        ...].
    scaled_positions: list of scaled-positions
        Like positions, but given in units of the unit cell.
        Can not be set at the same time as positions.
    numbers: list of int
        Atomic numbers (use only one of symbols/numbers).
    tags: list of int
        Special purpose tags.
    momenta: list of xyz-momenta
        Momenta for all atoms.
    masses: list of float
        Atomic masses in atomic units.
    magmoms: list of float
        Magnetic moments.
    charges: list of float
        Atomic charges.
    cell: 3x3 matrix
        Unit cell vectors.  Can also be given as just three
        numbers for orthorhombic cells.  Default value: [1, 1, 1].
    pbc: one or three bool
        Periodic boundary conditions flags.  Examples: True,
        False, 0, 1, (1, 1, 0), (True, False, False).  Default
        value: False.
    constraint: constraint object(s)
        Used for applying one or more constraints during structure
        optimization.
    calculator: calculator object
        Used to attach a calculator for calulating energies and atomic
        forces.

    Examples:

    These three are equivalent:

    >>> d = 1.104  # N2 bondlength
    >>> a = Atoms('N2', [(0, 0, 0), (0, 0, d)])
    >>> a = Atoms(numbers=[7, 7], positions=[(0, 0, 0), (0, 0, d)])
    >>> a = Atoms([Atom('N', (0, 0, 0)), Atom('N', (0, 0, d)])

    FCC gold:

    >>> a = 4.05  # Gold lattice constant
    >>> b = a / 2
    >>> fcc = Atoms('Au',
    ...             cell=[(0, b, b), (b, 0, b), (b, b, 0)],
    ...             pbc=True)

    Hydrogen wire:
    
    >>> d = 0.9  # H-H distance
    >>> L = 7.0
    >>> h = Atoms('H', positions=[(0, L / 2, L / 2)],
    ...           cell=(d, L, L),
    ...           pbc=(1, 0, 0))
    """

    def __init__(self, symbols=None,
                 positions=None, numbers=None,
                 tags=None, momenta=None, masses=None,
                 magmoms=None, charges=None,
                 scaled_positions=None,
                 cell=None, pbc=None,
                 constraint=None,
                 calculator=None):

        atoms = None

        if hasattr(symbols, 'GetUnitCell'):
            from ase.old import OldASEListOfAtomsWrapper
            atoms = OldASEListOfAtomsWrapper(symbols)
            symbols = None
        elif hasattr(symbols, "get_positions"):
            atoms = symbols
            symbols = None    
        elif (isinstance(symbols, (list, tuple)) and
              len(symbols) > 0 and isinstance(symbols[0], Atom)):
            # Get data from a list or tuple of Atom objects:
            data = zip(*[atom.get_data() for atom in symbols])
            atoms = Atoms(None, *data)
            symbols = None    
                
        if atoms is not None:
            # Get data from another Atoms object:
            if scaled_positions is not None:
                raise NotImplementedError
            if symbols is None and numbers is None:
                numbers = atoms.get_atomic_numbers()
            if positions is None:
                positions = atoms.get_positions()
            if tags is None and atoms.has('tags'):
                tags = atoms.get_tags()
            if momenta is None and atoms.has('momenta'):
                momenta = atoms.get_momenta()
            if magmoms is None and atoms.has('magmoms'):
                magmoms = atoms.get_initial_magnetic_moments()
            if masses is None and atoms.has('masses'):
                masses = atoms.get_masses()
            if charges is None and atoms.has('charges'):
                charges = atoms.get_charges()
            if cell is None:
                cell = atoms.get_cell()
            if pbc is None:
                pbc = atoms.get_pbc()
            if constraint is None:
                constraint = [c.copy() for c in atoms.constraints]
            if calculator is None:
                calculator = atoms.get_calculator()

        self.arrays = {}
        
        if symbols is None:
            if numbers is None:
                if positions is not None:
                    natoms = len(positions)
                elif scaled_positions is not None:
                    natoms = len(scaled_positions)
                else:
                    natoms = 0
                numbers = np.zeros(natoms, int)
            self.new_array('numbers', numbers, int)
        else:
            if numbers is not None:
                raise ValueError(
                    'Use only one of "symbols" and "numbers".')
            else:
                self.new_array('numbers', symbols2numbers(symbols), int)

        if cell is None:
            cell = np.eye(3)
        self.set_cell(cell)

        if positions is None:
            if scaled_positions is None:
                positions = np.zeros((len(self.arrays['numbers']), 3))
            else:
                positions = np.dot(scaled_positions, self._cell)
        else:
            if scaled_positions is not None:
                raise RuntimeError, 'Both scaled and cartesian positions set!'
        self.new_array('positions', positions, float, (3,))

        self.set_constraint(constraint)
        self.set_tags(default(tags, 0))
        self.set_momenta(default(momenta, (0.0, 0.0, 0.0)))
        self.set_masses(default(masses, None))
        self.set_initial_magnetic_moments(default(magmoms, 0.0))
        self.set_charges(default(charges, 0.0))
        if pbc is None:
            pbc = False
        self.set_pbc(pbc)

        self.adsorbate_info = {}

        self.set_calculator(calculator)

    def set_calculator(self, calc=None):
        """Attach calculator object."""
        if hasattr(calc, '_SetListOfAtoms'):
            from ase.old import OldASECalculatorWrapper
            calc = OldASECalculatorWrapper(calc, self)
        if hasattr(calc, 'set_atoms'):
            calc.set_atoms(self)
        self.calc = calc

    def get_calculator(self):
        """Get currently attached calculator object."""
        return self.calc

    def set_constraint(self, constraint=None):
        """Apply one or more constrains.

        The *constraint* argument must be one constraint object or a
        list of constraint objects."""
        if constraint is None:
            self._constraints = []
        else:
            if isinstance(constraint, (list, tuple)):
                self._constraints = constraint
            else:
                self._constraints = [constraint]

    def _get_constraints(self):
        return self._constraints

    def _del_constraints(self):
        self._constraints = []

    constraints = property(_get_constraints, set_constraint, _del_constraints,
                           "Constraints of the atoms.")
    
    def set_cell(self, cell, scale_atoms=False, fix=None):
        """Set unit cell vectors.

        Parameters:

        cell : 
            Unit cell.  A 3x3 matrix (the three unit cell vectors) or
            just three numbers for an orthorhombic cell.
        scale_atoms : bool
            Fix atomic positions or move atoms with the unit cell?
            Default behavior is to *not* move the atoms (scale_atoms=False).

        Examples:

        Two equivalent ways to define an orthorhombic cell:
        
        >>> a.set_cell([a, b, c])
        >>> a.set_cell([(a, 0, 0), (0, b, 0), (0, 0, c)])

        FCC unit cell:

        >>> a.set_cell([(0, b, b), (b, 0, b), (b, b, 0)])
        """

        if fix is not None:
            raise TypeError('Please use scale_atoms=%s' % (not fix))

        cell = np.array(cell, float)
        if cell.shape == (3,):
            cell = np.diag(cell)
        elif cell.shape != (3, 3):
            raise ValueError('Cell must be length 3 sequence or '
                             '3x3 matrix!')
        if scale_atoms:
            M = np.linalg.solve(self._cell, cell)
            self.arrays['positions'][:] = np.dot(self.arrays['positions'], M)
        self._cell = cell

    def get_cell(self):
        """Get the three unit cell vectors as a 3x3 ndarray."""
        return self._cell.copy()

    def set_pbc(self, pbc):
        """Set periodic boundary condition flags."""
        if isinstance(pbc, int):
            pbc = (pbc,) * 3
        self._pbc = np.array(pbc, bool)
        
    def get_pbc(self):
        """Get periodic boundary condition flags."""
        return self._pbc.copy()

    def new_array(self, name, a, dtype=None, shape=None):
        """Add new array.

        If *shape* is not *None*, the shape of *a* will be checked."""
        
        if dtype is not None:
            a = np.array(a, dtype)
        else:
            a = a.copy()
            
        if name in self.arrays:
            raise RuntimeError

        for b in self.arrays.values():
            if len(a) != len(b):
                raise ValueError('Array has wrong length: %d != %d.' %
                                 (len(a), len(b)))
            break

        if shape is not None and a.shape[1:] != shape:
            raise ValueError('Array has wrong shape %s != %s.' %
                             (a.shape, (a.shape[0:1] + shape)))
        
        self.arrays[name] = a

    def get_array(self, name, copy=True):
        """Get an array.

        Returns a copy unless the optional argument copy is false.
        """
        if copy:
            return self.arrays[name].copy()
        else:
            return self.arrays[name]
    
    def set_array(self, name, a, dtype=None, shape=None):
        """Update array.

        If *shape* is not *None*, the shape of *a* will be checked.
        If *a* is *None*, then the array is deleted."""
        
        b = self.arrays.get(name)
        if b is None:
            if a is not None:
                self.new_array(name, a, dtype, shape)
        else:
            if a is None:
                del self.arrays[name]
            else:
                a = np.asarray(a)
                if a.shape != b.shape:
                    raise ValueError('Array has wrong shape %s != %s.' %
                                     (a.shape, b.shape))
                b[:] = a

    def has(self, name):
        """Check for existance of array.

        name must be one of: 'tags', 'momenta', 'masses', 'magmoms',
        'charges'."""
        return name in self.arrays
    
    def set_atomic_numbers(self, numbers):
        """Set atomic numbers."""
        self.set_array('numbers', numbers, int, ())

    def get_atomic_numbers(self):
        """Get integer array of atomic numbers."""
        return self.arrays['numbers']

    def set_chemical_symbols(self, symbols):
        """Set chemical symbols."""
        self.set_array('numbers', symbols2numbers(symbols), int, ())

    def get_chemical_symbols(self, reduce=False):
        """Get list of chemical symbol strings.

        If reduce is True, a single string is returned, where repeated
        elements have been contracted to a single symbol and a number.
        E.g. instead of ['C', 'O', 'O', 'H'], the string 'CO2H' is returned.
        """
        if not reduce:
            return [chemical_symbols[Z] for Z in self.arrays['numbers']]
        else:
            num = self.get_atomic_numbers()
            N = len(num)
            dis = np.concatenate(([0], np.arange(1, N)[num[1:] != num[:-1]]))
            repeat = np.append(dis[1:], N) - dis
            symbols = ''.join([chemical_symbols[num[d]] + str(r) * (r != 1)
                               for r, d in zip(repeat, dis)])
            return symbols

    def set_tags(self, tags):
        """Set tags for all atoms."""
        self.set_array('tags', tags, int, ())
        
    def get_tags(self):
        """Get integer array of tags."""
        if 'tags' in self.arrays:
            return self.arrays['tags'].copy()
        else:
            return np.zeros(len(self), int)

    def set_momenta(self, momenta):
        """Set momenta."""
        if len(self.constraints) > 0 and momenta is not None:
            momenta = np.array(momenta)  # modify a copy
            for constraint in self.constraints:
                constraint.adjust_forces(self.arrays['positions'], momenta)
        self.set_array('momenta', momenta, float, (3,))

    def set_velocities(self, velocities):
        """Set the momenta by specifying the velocities."""
        self.set_momenta(self.get_masses()[:,np.newaxis] * velocities)
        
    def get_momenta(self):
        """Get array of momenta."""
        if 'momenta' in self.arrays:
            return self.arrays['momenta'].copy()
        else:
            return np.zeros((len(self), 3))
        
    def set_masses(self, masses='defaults'):
        """Set atomic masses.

        The array masses should contain the a list masses.  In case
        the masses argument is not given or for those elements of the
        masses list that are None, standard values are set."""
        
        if masses == 'defaults':
            masses = atomic_masses[self.arrays['numbers']]
        elif isinstance(masses, (list, tuple)):
            newmasses = []
            for m, Z in zip(masses, self.arrays['numbers']):
                if m is None:
                    newmasses.append(atomic_masses[Z])
                else:
                    newmasses.append(m)
            masses = newmasses
        self.set_array('masses', masses, float, ())

    def get_masses(self):
        """Get array of masses."""
        if 'masses' in self.arrays:
            return self.arrays['masses'].copy()
        else:
            return atomic_masses[self.arrays['numbers']]
        
    def set_initial_magnetic_moments(self, magmoms):
        """Set the initial magnetic moments."""
        self.set_array('magmoms', magmoms, float, ())

    def set_magnetic_moments(self, magmoms):
        print 'Please use set_initial_magnetic_moments() instead!'
        self.set_initial_magnetic_moments(magmoms)

    def get_initial_magnetic_moments(self):
        """Get array of initial magnetic moments."""
        if 'magmoms' in self.arrays:
            return self.arrays['magmoms'].copy()
        else:
            return np.zeros(len(self))

    def get_magnetic_moments(self):
        """Get calculated local magnetic moments."""
        if self.calc is None:
            raise RuntimeError('Atoms object has no calculator.')
        if self.calc.get_spin_polarized():
            return self.calc.get_magnetic_moments(self)
        else:
            return np.zeros(len(self))
        
    def get_magnetic_moment(self):
        """Get calculated total magnetic moment."""
        if self.calc is None:
            raise RuntimeError('Atoms object has no calculator.')
        if self.calc.get_spin_polarized():
            return self.calc.get_magnetic_moment(self)
        else:
            return 0.0

    def set_charges(self, charges):
        """Set charges."""
        self.set_array('charges', charges, float, ())

    def get_charges(self):
        """Get array of charges."""
        if 'charges' in self.arrays:
            return self.arrays['charges'].copy()
        else:
            return np.zeros(len(self))

    def set_positions(self, newpositions):
        """Set positions."""
        positions = self.arrays['positions']
        if self.constraints:
            newpositions = np.asarray(newpositions, float)
            for constraint in self.constraints:
                constraint.adjust_positions(positions, newpositions)
                
        self.set_array('positions', newpositions, shape=(3,))

    def get_positions(self):
        """Get array of positions."""
        return self.arrays['positions'].copy()

    def get_potential_energy(self):
        """Calculate potential energy."""
        if self.calc is None:
            raise RuntimeError('Atoms object has no calculator.')
        return self.calc.get_potential_energy(self)

    def get_potential_energies(self):
        """Calculate the potential energies of all the atoms.

        Only available with calculators supporting per-atom energies
        (e.g. classical potentials).
        """
        if self.calc is None:
            raise RuntimeError('Atoms object has no calculator.')
        return self.calc.get_potential_energies(self)

    def get_kinetic_energy(self):
        """Get the kinetic energy."""
        momenta = self.arrays.get('momenta')
        if momenta is None:
            return 0.0
        return 0.5 * np.vdot(momenta, self.get_velocities())

    def get_velocities(self):
        """Get array of velocities."""
        momenta = self.arrays.get('momenta')
        if momenta is None:
            return None
        m = self.arrays.get('masses')
        if m is None:
            m = atomic_masses[self.arrays['numbers']]
        return momenta / m.reshape(-1, 1)

    def get_total_energy(self):
        """Get the total energy - potential plus kinetic energy."""
        return self.get_potential_energy() + self.get_kinetic_energy()

    def get_forces(self, apply_constraint=True):
        """Calculate atomic forces.

        Ask the attached calculator to calculate the forces and apply
        constraints.  Use *apply_constraint=False* to get the raw
        forces."""

        if self.calc is None:
            raise RuntimeError('Atoms object has no calculator.')
        forces = self.calc.get_forces(self)
        if apply_constraint:
            for constraint in self.constraints:
                constraint.adjust_forces(self.arrays['positions'], forces)
        return forces

    def get_stress(self):
        """Calculate stress tensor.

        Returns an array of the six independent components of the
        symmetric stress tensor, in the traditional order
        (s_xx, s_yy, s_zz, s_yz, s_xz, s_xy).
        """
        if self.calc is None:
            raise RuntimeError('Atoms object has no calculator.')
        stress = self.calc.get_stress(self)
        shape = getattr(stress, "shape", None)
        if shape == (3,3):
            return np.array([stress[0,0], stress[1,1], stress[2,2],
                             stress[1,2], stress[0,2], stress[0,1]])
        else:
            # Hopefully a 6-vector, but don't check in case some weird
            # calculator does something else.
            return stress
    
    def get_stresses(self):
        """Calculate the stress-tensor of all the atoms.

        Only available with calculators supporting per-atom energies and
        stresses (e.g. classical potentials).  Even for such calculators
        there is a certain arbitrariness in defining per-atom stresses.
        """
        if self.calc is None:
            raise RuntimeError('Atoms object has no calculator.')
        return self.calc.get_stresses(self)

    def get_dipole_moment(self):
        """Calculate the electric dipole moment for the atoms object.

        Only available for calculators which has a get_dipole_moment() method."""
        if self.calc is None:
            raise RuntimeError('Atoms object has no calculator.')
        try:
            dipole = self.calc.get_dipole_moment(self)
        except AttributeError:
            raise AttributeError('Calculator object has no get_dipole_moment method.')
        return dipole
    
    def copy(self):
        """Return a copy."""
        import copy
        atoms = Atoms(cell=self._cell, pbc=self._pbc)

        atoms.arrays = {}
        for name, a in self.arrays.items():
            atoms.arrays[name] = a.copy()
        atoms.constraints = copy.deepcopy(self.constraints)
        atoms.adsorbate_info = copy.deepcopy(self.adsorbate_info)
        return atoms

    def __len__(self):
        return len(self.arrays['positions'])

    def get_number_of_atoms(self):
        """Returns the number of atoms.

        Equivalent to len(atoms) in the standard ASE Atoms class.
        """
        return len(self)
    
    def __repr__(self):
        num = self.get_atomic_numbers()
        N = len(num)
        if N == 0:
            symbols = ''
        elif N <= 60:
            symbols = self.get_chemical_symbols(reduce=True)
        else:
            symbols = ''.join([chemical_symbols[Z] for Z in num[:15]]) + '...'
        s = "Atoms(symbols='%s', " % symbols
        for name in self.arrays:
            if name == 'numbers':
                continue
            s += '%s=..., ' % name
        if (self._cell - np.diag(self._cell.diagonal())).any():
            s += 'cell=%s, ' % self._cell.tolist()            
        else:
            s += 'cell=%s, ' % self._cell.diagonal().tolist()
        s += 'pbc=%s, ' % self._pbc.tolist()
        if len(self.constraints) == 1:
            s += 'constraint=%s, ' % repr(self.constraints[0])
        if len(self.constraints) > 1:
            s += 'constraint=%s, ' % repr(self.constraints)
        if self.calc is not None:
            s += 'calculator=%s(...), ' % self.calc.__class__.__name__
        return s[:-2] + ')'

    def __add__(self, other):
        atoms = self.copy()
        atoms += other
        return atoms

    def extend(self, other):
        """Extend atoms object by appending atoms from *other*."""
        if isinstance(other, Atom):
            other = Atoms([other])
            
        n1 = len(self)
        n2 = len(other)
        
        for name, a1 in self.arrays.items():
            a = np.zeros((n1 + n2,) + a1.shape[1:], a1.dtype)
            a[:n1] = a1
            a2 = other.arrays.get(name)
            if a2 is not None:
                a[n1:] = a2
            self.arrays[name] = a

        for name, a2 in other.arrays.items():
            if name in self.arrays:
                continue
            a = np.zeros((n1 + n2,) + a2.shape[1:], a2.dtype)
            a[n1:] = a2
            self.set_array(name, a)

        return self

    __iadd__ = extend

    def append(self, atom):
        """Append atom to end."""
        self.extend(Atoms([atom]))

    def __getitem__(self, i):
        """Return a subset of the atoms.

        i -- scalar integer, list of integers, or slice object
        describing which atoms to return.

        If i is a scalar, return an Atom object. If i is a list or a
        slice, return an Atoms object with the same cell, pbc, and
        other associated info as the original Atoms object. The
        indices of the constraints will be shuffled so that they match
        the indexing in the subset returned.

        """
        if isinstance(i, int):
            natoms = len(self)
            if i < -natoms or i >= natoms:
                raise IndexError('Index out of range.')

            return Atom(atoms=self, index=i)
        
        import copy
        from ase.constraints import FixConstraint
        
        atoms = Atoms(cell=self._cell, pbc=self._pbc)
        # TODO: Do we need to shuffle indices in adsorbate_info too?
        atoms.adsorbate_info = self.adsorbate_info
        
        atoms.arrays = {}
        for name, a in self.arrays.items():
            atoms.arrays[name] = a[i].copy()
        
        # Constraints need to be deepcopied, since we need to shuffle
        # the indices
        atoms.constraints = copy.deepcopy(self.constraints)
        condel = []
        for con in atoms.constraints:
            if isinstance(con, FixConstraint):
                try:
                    con.index_shuffle(i)
                except IndexError:
                    condel.append(con)
        for con in condel:
            atoms.constraints.remove(con)
        return atoms

    def __delitem__(self, i):
        if len(self._constraints) > 0:
            raise RuntimeError('Remove constraint using set_constraint() ' +
                               'before deleting atoms.')
        mask = np.ones(len(self), bool)
        mask[i] = False
        for name, a in self.arrays.items():
            self.arrays[name] = a[mask]

    def pop(self, i=-1):
        """Remove and return atom at index *i* (default last)."""
        atom = self[i]
        atom.cut_reference_to_atoms()
        del self[i]
        return atom
    
    def __imul__(self, m):
        if len(self._constraints) > 0:
            raise RuntimeError('Remove constraint before modifying atoms.')
        if isinstance(m, int):
            m = (m, m, m)
        M = np.product(m)
        n = len(self)
        
        for name, a in self.arrays.items():
            self.arrays[name] = np.tile(a, (M,) + (1,) * (len(a.shape) - 1))

        positions = self.arrays['positions']
        i0 = 0
        for m2 in range(m[2]):
            for m1 in range(m[1]):
                for m0 in range(m[0]):
                    i1 = i0 + n
                    positions[i0:i1] += np.dot((m0, m1, m2), self._cell)
                    i0 = i1
        self._cell = np.array([m[c] * self._cell[c] for c in range(3)])
        return self

    def repeat(self, rep):
        """Create new repeated atoms object.

        The *rep* argument should be a sequence of three positive
        integers like *(2,3,1)* or a single integer (*r*) equivalent
        to *(r,r,r)*."""

        atoms = self.copy()
        atoms *= rep
        return atoms

    __mul__ = repeat
    
    def translate(self, displacement):
        """Translate atomic positions.

        The displacement argument can be a float an xyz vector or an
        nx3 array (where n is the number of atoms)."""

        self.arrays['positions'] += np.array(displacement)

    def center(self, vacuum=None, axis=None):
        """Center atoms in unit cell.

        Centers the atoms in the unit cell, so there is the same
        amount of vacuum on all sides.

        Parameters:

        vacuum (default: None): If specified adjust the amount of
        vacuum when centering.  If vacuum=10.0 there will thus be 10
        Angstrom of vacuum on each side.

        axis (default: None): If specified, only act on the specified
        axis.  Default: Act on all axes.
        """
        # Find the orientations of the faces of the unit cell
        c = self.get_cell()
        dirs = np.zeros_like(c)
        for i in range(3):
            dirs[i] = np.cross(c[i-1], c[i-2])
            dirs[i] /= np.sqrt(np.dot(dirs[i], dirs[i])) #Normalize
            if np.dot(dirs[i], c[i]) < 0.0:      
                dirs[i] *= -1

        # Now, decide how much each basis vector should be made longer
        if axis is None:
            axes = (0,1,2)
        else:
            axes = (axis,)
        p = self.arrays['positions']
        longer = np.zeros(3)
        shift = np.zeros(3)
        for i in axes:
            p0 = np.dot(p, dirs[i]).min()
            p1 = np.dot(p, dirs[i]).max()
            height = np.dot(c[i], dirs[i])
            if vacuum is not None:
                lng = (p1 - p0 + 2*vacuum) - height
            else:
                lng = 0.0  # Do not change unit cell size!
            top = lng + height - p1
            shf = 0.5 * (top - p0)
            cosphi = np.dot(c[i], dirs[i]) / np.sqrt(np.dot(c[i], c[i]))
            longer[i] = lng / cosphi
            shift[i] = shf / cosphi

        # Now, do it!
        translation = np.zeros(3)
        for i in axes:
            nowlen = np.sqrt(np.dot(c[i], c[i]))
            self._cell[i] *= 1 + longer[i] / nowlen
            translation += shift[i] * c[i] / nowlen
        self.arrays['positions'] += translation

    def get_center_of_mass(self, scaled = False):
        """Get the center of mass.

        If scaled=True the center of mass in scaled coordinates
        is returned."""
        m = self.arrays.get('masses')
        if m is None:
            m = atomic_masses[self.arrays['numbers']]
        com = np.dot(m, self.arrays['positions']) / m.sum()
        if scaled:
            return np.dot(np.linalg.inv(self._cell), com)
        else:
            return com

    def get_moments_of_inertia(self):
        '''Get the moments of inertia

        The three principal moments of inertia are computed from the
        eigenvalues of the inertial tensor. periodic boundary
        conditions are ignored. Units of the moments of inertia are
        amu*angstrom**2.
        '''
        
        com = self.get_center_of_mass()
        positions = self.get_positions()
        positions -= com #translate center of mass to origin
        masses = self.get_masses()

        #initialize elements of the inertial tensor
        I11 = I22 = I33 = I12 = I13 = I23 = 0.0
        for i in range(len(self)):
            x,y,z = positions[i]
            m = masses[i]

            I11 += m*(y**2 + z**2)
            I22 += m*(x**2 + z**2)
            I33 += m*(x**2 + y**2)
            I12 += -m*x*y
            I13 += -m*x*z
            I23 += -m*y*z

        I = np.array([[I11, I12, I13],
                      [I12, I22, I23],
                      [I13, I23, I33]])

        evals, evecs = np.linalg.eig(I)
        return evals

    def rotate(self, v, a=None, center=(0, 0, 0), rotate_cell=False):
        """Rotate atoms.

        Rotate the angle *a* around the vector *v*.  If *a* is not
        given, the length of *v* is used as the angle.  If *a* is a
        vector, then *v* is rotated into *a*.  The point at *center*
        is fixed.  Use *center='COM'* to fix the center of mass.
        Vectors can also be strings: 'x', '-x', 'y', ... .
        If both *v* and *a* are vectors: rotate *v* into *a*.

        Examples:

        Rotate 90 degrees around the z-axis, so that the x-axis is
        rotated into the y-axis:

        >>> a = pi / 2
        >>> atoms.rotate('z', a)
        >>> atoms.rotate((0, 0, 1), a)
        >>> atoms.rotate('-z', -a)
        >>> atoms.rotate((0, 0, a))
        >>> atoms.rotate('x', 'y')
        """

        norm = np.linalg.norm
        v = string2vector(v)
        if a is None:
            a = norm(v)
        if isinstance(a, (float, int)):
            v /= norm(v)
            c = cos(a)
            s = sin(a)
        else:
            v2 = string2vector(a)
            v /= norm(v)
            v2 /= norm(v2)
            c = np.dot(v, v2)
            v = np.cross(v, v2)
            s = norm(v)
            v /= s
        
        if isinstance(center, str) and center.lower() == 'com':
            center = self.get_center_of_mass()

        p = self.arrays['positions'] - center
        self.arrays['positions'][:] = (c * p - 
                                       np.cross(p, s * v) + 
                                       np.outer(np.dot(p, v), (1.0 - c) * v)+
                                       center)
        if rotate_cell:
            rotcell = self.get_cell()
            rotcell[:] = (c * rotcell - 
                          np.cross(rotcell, s * v) + 
                          np.outer(np.dot(rotcell, v), (1.0 - c) * v))
            self.set_cell(rotcell)
                
    def rotate_euler(self, center=(0, 0, 0), phi=0.0, theta=0.0, psi=0.0):
        """Rotate atoms via Euler angles.
        
        See e.g http://mathworld.wolfram.com/EulerAngles.html for explanation.
        
        Parameters:
        
        center :
            The point to rotate about. A sequence of length 3 with the
            coordinates, or 'COM' to select the center of mass.
        phi :
            The 1st rotation angle around the z axis.
        theta :
            Rotation around the x axis.
        psi :
            2nd rotation around the z axis.
        
        """
        if isinstance(center, str) and center.lower() == 'com':
            center = self.get_center_of_mass()
        else:
            center = np.array(center)
        # First move the molecule to the origin In contrast to MATLAB,
        # numpy broadcasts the smaller array to the larger row-wise,
        # so there is no need to play with the Kronecker product.
        rcoords = self.positions - center
        # First Euler rotation about z in matrix form
        D = np.array(((cos(phi), sin(phi), 0.),
                      (-sin(phi), cos(phi), 0.),
                      (0., 0., 1.)))
        # Second Euler rotation about x:
        C = np.array(((1., 0., 0.),
                      (0., cos(theta), sin(theta)),
                      (0., -sin(theta), cos(theta))))
        # Third Euler rotation, 2nd rotation about z:
        B = np.array(((cos(psi), sin(psi), 0.),
                      (-sin(psi), cos(psi), 0.),
                      (0., 0., 1.)))
        # Total Euler rotation
        A = np.dot(B, np.dot(C, D))
        # Do the rotation
        rcoords = np.dot(A, np.transpose(rcoords))
        # Move back to the rotation point
        self.positions = np.transpose(rcoords) + center

    def get_dihedral(self,list):
        """
        calculate dihedral angle between the vectors list[0]->list[1] and list[2]->list[3], 
        where list contains the atomic indexes in question. 
        """
        # vector 0->1, 1->2, 2->3 and their normalized cross products:
        a    = self.positions[list[1]]-self.positions[list[0]]
        b    = self.positions[list[2]]-self.positions[list[1]]
        c    = self.positions[list[3]]-self.positions[list[2]]
        bxa  = np.cross(b,a)
        bxa /= np.sqrt(np.vdot(bxa,bxa))
        cxb  = np.cross(c,b)
        cxb /= np.sqrt(np.vdot(cxb,cxb))
        angle = np.arccos(np.vdot(bxa,cxb))
        # check sign of angle: if (b x a) . c > 0 then add pi
        if np.vdot(bxa,c) > 1e-20:
            add = np.pi
        else:
            add = 0
        return angle+add

    def set_dihedral(self,list,angle,mask=None):
        """
        set the dihedral angle between vectors list[0]->list[1] and 
        list[2]->list[3] by changing the atom indexed by list[3]
        if mask is not None, all the atoms described in mask 
        (read: the entire subgroup) are moved
        
        example: the following defines a very crude 
        ethane-like molecule and twists one half of it by 30 degrees.
        atoms = Atoms('HHCCHH',[[-1,1,0],[-1,-1,0],[0,0,0],[1,0,0],[2,1,0],[2,-1,0]])
        atoms.set_dihedral([1,2,3,4],pi/6,mask=[0,0,0,1,1,1])
        """
        # if not provided, set mask to the last atom in the dihedral description
        if mask is None:
            mask = np.zeros(len(self))
            mask[list[3]] = 1
        # compute necessary in dihedral change, from current value
        current =self.get_dihedral(list)
        diff    = angle - current
        # do rotation of subgroup by copying it to temporary atoms object and then rotating that
        axis   = self.positions[list[2]]-self.positions[list[1]]
        center = self.positions[list[2]]
        # recursive object definition might not be the most elegant thing, more generally useful might be a rotation function with a mask?
        group  = Atoms()
        for i in range(len(self)):
            if mask[i]:
                group += self[i]
        group.translate(-center)
        group.rotate(axis,diff)
        group.translate(center)
        # set positions in original atoms object
        j = 0
        for i in range(len(self)):
            if mask[i]:
                self.positions[i] = group[j].get_position()
                j += 1
        
    def rattle(self, stdev=0.001, seed=42):
        """Randomly displace atoms.

        This method adds random displacements to the atomic positions,
        taking a possible constraint into account.  The random numbers are
        drawn from a normal distribution of standard deviation stdev.

        For a parallel calculation, it is important to use the same
        seed on all processors!  """
        
        rs = np.random.RandomState(seed)
        positions = self.arrays['positions']
        self.set_positions(positions +
                           rs.normal(scale=stdev, size=positions.shape))
        
    def get_distance(self, a0, a1, mic=False):
        """Return distance between two atoms.

        Use mic=True to use the Minimum Image Convention.
        """

        R = self.arrays['positions']
        D = R[a1] - R[a0]
        if mic:
            raise NotImplemented  # XXX
        return np.linalg.norm(D)

    def set_distance(self, a0, a1, distance, fix=0.5):
        """Set the distance between two atoms.

        Set the distance between atoms *a0* and *a1* to *distance*.
        By default, the center of the two atoms will be fixed.  Use
        *fix=0* to fix the first atom, *fix=1* to fix the second
        atom and *fix=0.5* (default) to fix the center of the bond."""

        R = self.arrays['positions']
        D = R[a1] - R[a0]
        x = 1.0 - distance / np.linalg.norm(D)
        R[a0] += (x * fix) * D
        R[a1] -= (x * (1.0 - fix)) * D

    def get_scaled_positions(self):
        """Get positions relative to unit cell.

        Atoms outside the unit cell will be wrapped into the cell in
        those directions with periodic boundary conditions so that the
        scaled coordinates are beween zero and one."""

        scaled = np.linalg.solve(self._cell.T, self.arrays['positions'].T).T
        for i in range(3):
            if self._pbc[i]:
                scaled[:, i] %= 1.0
        return scaled

    def set_scaled_positions(self, scaled):
        """Set positions relative to unit cell."""
        self.arrays['positions'][:] = np.dot(scaled, self._cell)

    def __eq__(self, other):
        """Check for identity of two atoms objects.

        Identity means: same positions, atomic numbers, unit cell and
        periodic boundary conditions."""
        try:
            a = self.arrays
            b = other.arrays
            return (len(self) == len(other) and
                    (a['positions'] == b['positions']).all() and
                    (a['numbers'] == b['numbers']).all() and
                    (self._cell == other.cell).all() and
                    (self._pbc == other.pbc).all())
        except AttributeError:
            return NotImplemented

    def __ne__(self, other):
        eq = self.__eq__(other)
        if eq is NotImplemented:
            return eq
        else:
            return not eq

    __hash__ = None

    def get_volume(self):
        """Get volume of unit cell."""
        return abs(np.linalg.det(self._cell))
    
    def _get_positions(self):
        """Return reference to positions-array for inplace manipulations."""
        return self.arrays['positions']

    def _set_positions(self, pos):
        """Set positions directly, bypassing constraints."""
        self.arrays['positions'][:] = pos

    positions = property(_get_positions, _set_positions,
                         doc='Attribute for direct ' +
                         'manipulation of the positions.')

    def _get_atomic_numbers(self):
        """Return reference to atomic numbers for inplace manipulations."""
        return self.arrays['numbers']

    numbers = property(_get_atomic_numbers, set_atomic_numbers,
                       doc='Attribute for direct ' +
                       'manipulation of the atomic numbers.')

    def _get_cell(self):
        """Return reference to unit cell for inplace manipulations."""
        return self._cell
    
    cell = property(_get_cell, set_cell, doc='Attribute for direct ' +
                       'manipulation of the unit cell.')

    def _get_pbc(self):
        """Return reference to pbc-flags for inplace manipulations."""
        return self._pbc
    
    pbc = property(_get_pbc, set_pbc,
                   doc='Attribute for direct manipulation ' +
                   'of the periodic boundary condition flags.')

    def get_name(self):
        """Return a name extracted from the elements."""
        elements = {}
        for a in self:
            try:
                elements[a.symbol] += 1
            except:
                elements[a.symbol] = 1
        name = ''
        for element in elements:
            name += element
            if elements[element] > 1:
                name += str(elements[element])
        return name

    def write(self, filename, format=None):
        """Write yourself to a file."""
        from ase.io import write
        write(filename, self, format)
        
def string2symbols(s):
    """Convert string to list of chemical symbols."""
    n = len(s)

    if n == 0:
        return []
    
    c = s[0]
    
    if c.isdigit():
        i = 1
        while i < n and s[i].isdigit():
            i += 1
        return int(s[:i]) * string2symbols(s[i:])

    if c == '(':
        p = 0
        for i, c in enumerate(s):
            if c == '(':
                p += 1
            elif c == ')':
                p -= 1
                if p == 0:
                    break
        j = i + 1
        while j < n and s[j].isdigit():
            j += 1
        if j > i + 1:
            m = int(s[i + 1:j])
        else:
            m = 1
        return m * string2symbols(s[1:i]) + string2symbols(s[j:])

    if c.isupper():
        i = 1
        if 1 < n and s[1].islower():
            i += 1
        j = i
        while j < n and s[j].isdigit():
            j += 1
        if j > i:
            m = int(s[i:j])
        else:
            m = 1
        return m * [s[:i]] + string2symbols(s[j:])
    else:
        raise ValueError

def symbols2numbers(symbols):
    if isinstance(symbols, str):
        symbols = string2symbols(symbols)
    numbers = []
    for s in symbols:
        if isinstance(s, str):
            numbers.append(atomic_numbers[s])
        else:
            numbers.append(s)
    return numbers

def string2vector(v):
    if isinstance(v, str):
        if v[0] == '-':
            return -string2vector(v[1:])
        w = np.zeros(3)
        w['xyz'.index(v)] = 1.0
        return w
    return np.asarray(v, float)

def default(data, dflt):
    """Helper function for setting default values."""
    if data is None:
        return None
    elif isinstance(data, (list, tuple)):
        newdata = []
        allnone = True
        for x in data:
            if x is None:
                newdata.append(dflt)
            else:
                newdata.append(x)
                allnone = False
        if allnone:
            return None
        return newdata
    else:
        return data
                               
