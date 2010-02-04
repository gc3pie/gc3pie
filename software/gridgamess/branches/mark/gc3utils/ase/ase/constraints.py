from math import sqrt

import numpy as np

def slice2enlist(s):
    """Convert a slice object into a list of (new, old) tuples."""
    if isinstance(s, (list, tuple)):
        return enumerate(s)
    if s.step == None:
        step = 1
    else:
        step = s.step
    if s.start == None:
        start = 0
    else:
        start = s.start
    return enumerate(range(start, s.stop, step))

class FixConstraint:
    """Base class for classes that fix one or more atoms in some way."""

    def index_shuffle(self, ind):
        """Change the indices.

        When the ordering of the atoms in the Atoms object changes,
        this method can be called to shuffle the indices of the
        constraints.

        ind -- List or tuple of indices.

        """
        raise NotImplementedError

class FixConstraintSingle(FixConstraint):
    "Base class for classes that fix a single atom."

    def index_shuffle(self, ind):
        "The atom index must be stored as self.a."
        newa = -1 # Signal error
        for new, old in slice2enlist(ind):
            if old == self.a:
                newa = new
                break
        if newa == -1:
            raise IndexError('Constraint not part of slice')
        self.a = newa

class FixAtoms(FixConstraint):
    """Constraint object for fixing some chosen atoms."""
    def __init__(self, indices=None, mask=None):
        """Constrain chosen atoms.

        Parameters
        ----------
        indices : list of int
           Indices for those atoms that should be constrained.
        mask : list of bool
           One boolean per atom indicating if the atom should be
           constrained or not.
           
        Examples
        --------
        Fix all Copper atoms:

        >>> c = FixAtoms(mask=[s == 'Cu' for s in atoms.get_chemical_symbols()])
        >>> atoms.set_constraint(c)

        Fix all atoms with z-coordinate less than 1.0 Angstrom:

        >>> c = FixAtoms(mask=atoms.positions[:, 2] < 1.0)
        >>> atoms.set_constraint(c)
        """

        if indices is None and mask is None:
            raise ValueError('Use "indices" or "mask".')
        if indices is not None and mask is not None:
            raise ValueError('Use only one of "indices" and "mask".')

        if mask is not None:
            self.index = np.asarray(mask, bool)
        else:
            self.index = np.asarray(indices, int)

        if self.index.ndim != 1:
            raise ValueError('Wrong argument to FixAtoms class!')

    def adjust_positions(self, old, new):
        new[self.index] = old[self.index]

    def adjust_forces(self, positions, forces):
        forces[self.index] = 0.0

    def index_shuffle(self, ind):
        # See docstring of superclass
        if self.index.dtype == bool:
            self.index = self.index[ind]
        else:
            index = []
            for new, old in slice2enlist(ind):
                if old in self.index:
                    index.append(new)
            if len(index) == 0:
                raise IndexError('All indices in FixAtoms not part of slice')
            self.index = np.asarray(index, int)

    def copy(self):
        if self.index.dtype == bool:
            return FixAtoms(mask=self.index.copy())
        else:
            return FixAtoms(indices=self.index.copy())
    
    def __repr__(self):
        if self.index.dtype == bool:
            return 'FixAtoms(mask=%s)' % ints2string(self.index.astype(int))
        return 'FixAtoms(indices=%s)' % ints2string(self.index)

def ints2string(x, threshold=10):
    """Convert ndarray of ints to string."""
    if len(x) <= threshold:
        return str(x.tolist())
    return str(x[:threshold].tolist())[:-1] + ', ...]'

class FixBondLengths(FixConstraint):
    def __init__(self, pairs, iterations=10):
        self.constraints = [FixBondLength(a1, a2)
                            for a1, a2 in pairs]
        self.iterations = iterations

    def adjust_positions(self, old, new):
        for i in range(self.iterations):
            for constraint in self.constraints:
                constraint.adjust_positions(old, new)

    def adjust_forces(self, positions, forces):
        for i in range(self.iterations):
            for constraint in self.constraints:
                constraint.adjust_forces(positions, forces)

    def copy(self):
        return FixBondLengths([constraint.indices for constraint in self.constraints])

class FixBondLength(FixConstraint):
    """Constraint object for fixing a bond length."""
    def __init__(self, a1, a2):
        """Fix distance between atoms with indices a1 and a2."""
        self.indices = [a1, a2]

    def adjust_positions(self, old, new):        
        p1, p2 = old[self.indices]
        d = p2 - p1
        p = sqrt(np.dot(d, d))
        q1, q2 = new[self.indices]
        d = q2 - q1
        q = sqrt(np.dot(d, d))
        d *= 0.5 * (p - q) / q
        new[self.indices] = (q1 - d, q2 + d)

    def adjust_forces(self, positions, forces):
        d = np.subtract.reduce(positions[self.indices])
        d2 = np.dot(d, d)
        d *= 0.5 * np.dot(np.subtract.reduce(forces[self.indices]), d) / d2
        forces[self.indices] += (-d, d)

    def index_shuffle(self, ind):
        'Shuffle the indices of the two atoms in this constraint'
        newa = [-1, -1] # Signal error
        for new, old in slice2enlist(ind):
            for i, a in enumerate(self.indices):
                if old == a:
                    newa[i] = new
        if newa[0] == -1 or newa[1] == -1:
            raise IndexError('Constraint not part of slice')
        self.indices = newa
    
    def copy(self):
        return FixBondLength(*self.indices)

    def __repr__(self):
        return 'FixBondLength(%d, %d)' % tuple(self.indices)


class FixedPlane(FixConstraintSingle):
    """Constrain an atom *a* to move in a given plane only.

    The plane is defined by its normal: *direction*."""
    
    def __init__(self, a, direction):
        self.a = a
        self.dir = np.asarray(direction) / sqrt(np.dot(direction, direction))

    def adjust_positions(self, oldpositions, newpositions):
        step = newpositions[self.a] - oldpositions[self.a]
        newpositions[self.a] -= self.dir * np.dot(step, self.dir)

    def adjust_forces(self, positions, forces):
        forces[self.a] -= self.dir * np.dot(forces[self.a], self.dir)

    def copy(self):
        return FixedPlane(self.a, self.dir)

    def __repr__(self):
        return 'FixedPlane(%d, %s)' % (self.a, self.dir.tolist())


class FixedLine(FixConstraintSingle):
    """Constrain an atom *a* to move on a given line only.

    The line is defined by its *direction*."""
    
    def __init__(self, a, direction):
        self.a = a
        self.dir = np.asarray(direction) / sqrt(np.dot(direction, direction))

    def adjust_positions(self, oldpositions, newpositions):
        step = newpositions[self.a] - oldpositions[self.a]
        x = np.dot(step, self.dir)
        newpositions[self.a] = oldpositions[self.a] + x * self.dir

    def adjust_forces(self, positions, forces):
        forces[self.a] = self.dir * np.dot(forces[self.a], self.dir)

    def copy(self):
        return FixedLine(self.a, self.dir)

    def __repr__(self):
        return 'FixedLine(%d, %s)' % (self.a, self.dir.tolist())

class FixCartesian(FixConstraintSingle):
    "Fix an atom in the directions of the cartesian coordinates."
    def __init__(self, a, mask=[1,1,1]):
        self.a=a
        self.mask = -(np.array(mask)-1)
    
    def adjust_positions(self, old, new):
        step = new - old
        step[self.a] *= self.mask
        new = old + step

    def adjust_forces(self, positions, forces):
        forces[self.a] *= self.mask

    def copy(self):
        return FixCartesian(self.a, self.mask)

    def __repr__(self):
        return 'FixCartesian(indice=%s mask=%s)' % (self.a, self.mask)

class fix_cartesian(FixCartesian):
    "Backwards compatibility for FixCartesian."
    def __init__(self, a, mask=[1,1,1]):
        import warnings
        super(fix_cartesian, self).__init__(a, mask)
        warnings.warn('fix_cartesian is deprecated. Please use FixCartesian' \
                      ' instead.', DeprecationWarning, stacklevel=2)

class FixScaled(FixConstraintSingle):
    "Fix an atom in the directions of the unit vectors."
    def __init__(self, cell, a, mask=[1,1,1]):
        self.cell = cell
        self.a = a
        self.mask = np.array(mask)

    def adjust_positions(self, old, new):
        scaled_old = np.linalg.solve(self.cell.T, old.T).T
        scaled_new = np.linalg.solve(self.cell.T, new.T).T
        for n in range(3):
            if self.mask[n]:
                scaled_new[self.a, n] = scaled_old[self.a, n]
        new[self.a] = np.dot(scaled_new, self.cell)[self.a]

    def adjust_forces(self, positions, forces):
        scaled_forces = np.linalg.solve(self.cell.T, forces.T).T
        scaled_forces[self.a] *= -(self.mask-1)
        forces[self.a] = np.dot(scaled_forces, self.cell)[self.a]

    def copy(self):
        return FixScaled(self.cell ,self.a, self.mask)

    def __repr__(self):
        return 'FixScaled(indice=%s mask=%s)' % (self.a, self.mask)

class fix_scaled(FixScaled):
    "Backwards compatibility for FixScaled."
    def __init__(self, cell, a, mask=[1,1,1]):
        import warnings
        super(fix_scaled, self).__init__(cell, a, mask)
        warnings.warn('fix_scaled is deprecated. Please use FixScaled ' \
                      'instead.', DeprecationWarning, stacklevel=2)

class Filter:
    """Subset filter class."""
    def __init__(self, atoms, indices=None, mask=None):
        """Filter atoms.

        This filter can be used to hide degrees of freedom in an Atoms
        object.

        Parameters
        ----------
        indices : list of int
           Indices for those atoms that should remain visible.
        mask : list of bool
           One boolean per atom indicating if the atom should remain
           visible or not.
        """

        self.atoms = atoms
        self.constraints = []

        if indices is None and mask is None:
            raise ValuError('Use "indices" or "mask".')
        if indices is not None and mask is not None:
            raise ValuError('Use only one of "indices" and "mask".')

        if mask is not None:
            self.index = np.asarray(mask, bool)
            self.n = self.index.sum()
        else:
            self.index = np.asarray(indices, int)
            self.n = len(self.index)

    def get_cell(self):
        """Returns the computational cell.

        The computational cell is the same as for the original system.
        """
        return self.atoms.get_cell()
    
    def get_pbc(self):
        """Returns the periodic boundary conditions.

        The boundary conditions are the same as for the original system.
        """
        return self.atoms.get_pbc()
    
    def get_positions(self):
        "Return the positions of the visible atoms."
        return self.atoms.get_positions()[self.index]

    def set_positions(self, positions):
        "Set the positions of the visible atoms."
        pos = self.atoms.get_positions()
        pos[self.index] = positions
        self.atoms.set_positions(pos)

    def get_momenta(self):
        "Return the momenta of the visible atoms."
        return self.atoms.get_momenta()[self.index]

    def set_momenta(self, momenta):
        "Set the momenta of the visible atoms."
        mom = self.atoms.get_momenta()
        mom[self.index] = momenta
        self.atoms.set_momenta(mom)

    def get_atomic_numbers(self):
        "Return the atomic numbers of the visible atoms."
        return self.atoms.get_atomic_numbers()[self.index]

    def set_atomic_numbers(self, atomic_numbers):
        "Set the atomic numbers of the visible atoms."
        z = self.atoms.get_atomic_numbers()
        z[self.index] = atomic_numbers
        self.atoms.set_atomic_numbers(z)

    def get_tags(self):
        "Return the tags of the visible atoms."
        return self.atoms.get_tags()[self.index]

    def set_tags(self, tags):
        "Set the tags of the visible atoms."
        tg = self.atoms.get_tags()
        tg[self.index] = tags
        self.atoms.set_tags(tg)

    def get_forces(self, *args, **kwargs):
        return self.atoms.get_forces(*args, **kwargs)[self.index]

    def get_stress(self):
        return self.atoms.get_stress()

    def get_stresses(self):
        return self.atoms.get_stresses()[self.index]
    
    def get_masses(self):
        return self.atoms.get_masses()[self.index]

    def get_potential_energy(self):
        """Calculate potential energy.

        Returns the potential energy of the full system.
        """
        return self.atoms.get_potential_energy()

    def get_calculator(self):
        """Returns the calculator.

        WARNING: The calculator is unaware of this filter, and sees a
        different number of atoms.
        """
        return self.atoms.get_calculator()

    def has(self, name):
        """Check for existance of array."""
        return self.atoms.has(name)

    def __len__(self):
        "Return the number of movable atoms."
        return self.n

    def __getitem__(self, i):
        "Return an atom."
        return self.atoms[self.index[i]]


class StrainFilter:
    """Modify the supercell while keeping the scaled positions fixed.

    Presents the strain of the supercell as the generalized positions,
    and the global stress tensor (times the volume) as the generalized
    force.

    This filter can be used to relax the unit cell until the stress is
    zero.  If MDMin is used for this, the timestep (dt) to be used
    depends on the system size. 0.01/x where x is a typical dimension
    seems like a good choice.

    The stress and strain are presented as 6-vectors, the order of the
    components follow the standard engingeering practice: xx, yy, zz,
    yz, xz, xy.  
    
    """
    def __init__(self, atoms, mask=None):
        """Create a filter applying a homogeneous strain to a list of atoms.

        The first argument, atoms, is the atoms object.

        The optional second argument, mask, is a list of six booleans,
        indicating which of the six independent components of the
        strain that are allowed to become non-zero.  It defaults to
        [1,1,1,1,1,1].
        
        """
        
        self.atoms = atoms
        self.strain = np.zeros(6)

        if mask is None:
            self.mask = np.ones(6)
        else:
            self.mask = np.array(mask)

        self.origcell = atoms.get_cell()
        
    def get_positions(self):
        return self.strain.reshape((2, 3))

    def set_positions(self, new):
        new = new.ravel() * self.mask
        eps = np.array([[1.0 + new[0], 0.5 * new[5], 0.5 * new[4]],
                        [0.5 * new[5], 1.0 + new[1], 0.5 * new[3]],
                        [0.5 * new[4], 0.5 * new[3], 1.0 + new[2]]])

        self.atoms.set_cell(np.dot(self.origcell, eps), scale_atoms=True)
        self.strain[:] = new

    def get_forces(self):
        stress = self.atoms.get_stress()
        return -self.atoms.get_volume() * (stress * self.mask).reshape((2, 3))

    def get_potential_energy(self):
        return self.atoms.get_potential_energy()

    def has(self, x):
	    return self.atoms.has(x)

    def __len__(self):
        return 2

class UnitCellFilter:
    """Modify the supercell and the atom positions. """
    def __init__(self, atoms, mask=None):
        """Create a filter that returns the atomic forces and unit
        cell stresses together, so they can simultaneously be
        minimized.

        The first argument, atoms, is the atoms object.

        The optional second argument, mask, is a list of booleans,
        indicating which of the six independent
        components of the strain are relaxed.
        1, True = relax to zero
        0, False = fixed, ignore this component
        
        use atom Constraints, e.g. FixAtoms, to control relaxation of
        the atoms.

        #this should be equivalent to the StrainFilter
        >>> atoms = Atoms(...)
        >>> atoms.set_constraint(FixAtoms(mask=[True for atom in atoms]))
        >>> ucf = UCFilter(atoms)

        You should not attach this UCFilter object to a
        trajectory. Instead, create a trajectory for the atoms, and
        attach it to an optimizer like this:

        >>> atoms = Atoms(...)
        >>> ucf = UCFilter(atoms)
        >>> qn = QuasiNewton(ucf)
        >>> traj = PickleTrajectory('TiO2.traj','w',atoms)
        >>> qn.attach(traj)
        >>> qn.run(fmax=0.05)

        Helpful conversion table
        ========================
        0.05 eV/A^3   = 8 GPA
        0.003 eV/A^3  = 0.48 GPa
        0.0006 eV/A^3 = 0.096 GPa
        0.0003 eV/A^3 = 0.048 GPa
        0.0001 eV/A^3 = 0.02 GPa
        """
        
        self.atoms = atoms
        self.strain = np.zeros(6)
                
        if mask is None:
            self.mask = np.ones(6)
        else:
            self.mask = np.array(mask)

        self.origcell = atoms.get_cell()
        
    def get_positions(self):
        '''
        this returns an array with shape (natoms+2,3).

        the first natoms rows are the positions of the atoms, the last
        two rows are the strains associated with the unit cell
        '''

        atom_positions = self.atoms.get_positions()
        strains = self.strain.reshape((2, 3))

        natoms = len(self.atoms)
        all_pos = np.zeros((natoms+2,3),np.float)
        all_pos[0:natoms,:] = atom_positions
        all_pos[natoms:,:] = strains

        return all_pos

    def set_positions(self, new):
        '''
        new is an array with shape (natoms+2,3).
        
        the first natoms rows are the positions of the atoms, the last
        two rows are the strains used to change the cell shape.

        The atom positions are set first, then the unit cell is
        changed keeping the atoms in their scaled positions.
        '''        

        natoms = len(self.atoms)
        
        atom_positions = new[0:natoms,:]
        self.atoms.set_positions(atom_positions)

        new = new[natoms:,:] #this is only the strains
        new = new.ravel() * self.mask
        eps = np.array([[1.0 + new[0], 0.5 * new[5], 0.5 * new[4]],
                        [0.5 * new[5], 1.0 + new[1], 0.5 * new[3]],
                        [0.5 * new[4], 0.5 * new[3], 1.0 + new[2]]])

        self.atoms.set_cell(np.dot(self.origcell, eps), scale_atoms=True)
        self.strain[:] = new

    def get_forces(self,apply_constraint=False):
        '''
        returns an array with shape (natoms+2,3) of the atomic forces
        and unit cell stresses.

        the first natoms rows are the forces on the atoms, the last
        two rows are the stresses on the unit cell, which have been
        reshaped to look like "atomic forces". i.e.,

        f[-2] = -vol*[sxx,syy,szz]*mask[0:3]
        f[-1] = -vol*[syz, sxz, sxy]*mask[3:]

        apply_constraint is an argument expected by ase
        '''
        
        stress = self.atoms.get_stress()
        atom_forces = self.atoms.get_forces()

        natoms = len(self.atoms)
        all_forces = np.zeros((natoms+2,3),np.float)
        all_forces[0:natoms,:] = atom_forces

        vol = self.atoms.get_volume()
        stress_forces = -vol * (stress * self.mask).reshape((2, 3))
        all_forces[natoms:,:] = stress_forces 
        return all_forces

    def get_potential_energy(self):
        return self.atoms.get_potential_energy()

    def has(self, x):
        return self.atoms.has(x)

    def __len__(self):
        return (2 + len(self.atoms))
