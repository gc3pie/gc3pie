"""This module defines the Atom object."""

import numpy as np

from ase.data import atomic_numbers, chemical_symbols


#        singular,    plural,     type,  shape
data = {'symbol':   ('symbols',   str,   ()  ),
        'number':   ('numbers',   int,   ()  ),
        'position': ('positions', float, (3,)),
        'tag':      ('tags',      int,   ()  ),
        'momentum': ('momenta',   float, (3,)),
        'mass':     ('masses',    float, ()  ),
        'magmom':   ('magmoms',   float, ()  ),
        'charge':   ('charges',   float, ()  ),
        }

class Atom(object):
    """Class for representing a single atom.

        Parameters:

        symbol: str or int
            Can be a chemical symbol (str) or an atomic number (int).
        position: sequence of 3 floats
            Atomi position.
        tag: int
            Special purpose tag.
        momentum: sequence of 3 floats
            Momentum for atom.
        mass: float
            Atomic mass in atomic units.
        magmom: float
            Magnetic moment.
        charge: float
            Atomic charge.

        Examples:

        >>> a = Atom('O', charge=-2)
        >>> b = Atom(8, charge=-2)
        >>> c = Atom('H', (1, 2, 3), magmom=1)
        >>> print a.charge, a.position
        -2 [ 0. 0. 0.]
        >>> c.x = 0.0
        >>> c.position
        array([ 0.,  2.,  3.])
        >>> b.symbol
        'O'
        >>> c.tag = 42
        >>> c.number
        1
        >>> c.symbol = 'Li'
        >>> c.number
        3

        If the atom object belongs to an Atoms object, then assigning
        values to the atom attributes will change the corresponding
        arrays of the atoms object:

        >>> OH = Atoms('OH')
        >>> OH[0].charge = -1
        >>> OH.get_charges()
        array([-1.,  0.])

        Another example:

        >>> for atom in bulk:
        ...     if atom.symbol = 'Ni':
        ...         atom.magmom = 0.7
        

        """

    __slots__ = ['_number', '_symbol', '_position', '_tag', '_momentum',
                 '_mass', '_magmom', '_charge', 'atoms', 'index']

    def __init__(self, symbol='X', position=(0, 0, 0),
                 tag=None, momentum=None, mass=None,
                 magmom=None, charge=None,
                 atoms=None, index=None):
        if atoms is None:
            # This atom is not part of any Atoms object:
            if isinstance(symbol, str):
                self._number = atomic_numbers[symbol]
                self._symbol = symbol
            else:
                self._number = symbol
                self._symbol = chemical_symbols[symbol]
            self._position = np.array(position, float)
            self._tag = tag
            if momentum is not None:
                momentum = np.array(momentum, float)
            self._momentum = momentum
            self._mass = mass
            self._magmom = magmom
            self._charge = charge

        self.index = index
        self.atoms = atoms

    def __repr__(self):
        s = "Atom('%s', %s" % (self.symbol, list(self.position))
        for attr in ['tag', 'momentum', 'mass', 'magmom', 'charge']:
            value = getattr(self, attr)
            if value is not None:
                if isinstance(value, np.ndarray):
                    value = value.tolist()
                s += ', %s=%s' % (attr, value)
        if self.atoms is None:
            s += ')'
        else:
            s += ', index=%d)' % self.index
        return s

    def get_data(self):
        """Helper method."""
        return (self.position, self.number,
                self.tag, self.momentum, self.mass,
                self.magmom, self.charge)

    def cut_reference_to_atoms(self):
        """Cut reference to atoms object."""
        data = self.get_data()
        self.index = None
        self.atoms = None
        (self._position,
         self._number,
         self._tag,
         self._momentum,
         self._mass,
         self._magmom,
         self._charge) = data
        self._symbol = chemical_symbols[self._number]
        
    def _get(self, name):
        if self.atoms is None:
            return getattr(self, '_' + name)
        elif name == 'symbol':
            return chemical_symbols[self.number]
        else:
            plural = data[name][0]
            if plural in self.atoms.arrays:
                return self.atoms.arrays[plural][self.index]
            else:
                return None

    def _get_copy(self, name, copy=False):
        if self.atoms is None:
            return getattr(self, '_' + name)
        elif name == 'symbol':
            return chemical_symbols[self.number]
        else:
            plural = data[name][0]
            if plural in self.atoms.arrays:
                return self.atoms.arrays[plural][self.index].copy()
            else:
                return None

    def _set(self, name, value):
        if self.atoms is None:
            setattr(self, '_' + name, value)
            if name == 'symbol':
                self._number = atomic_numbers[value]
            elif name == 'number':
                self._symbol = chemical_symbols[value]
        elif name == 'symbol':
            self.number = atomic_numbers[value]
        else:
            plural, dtype, shape = data[name]
            if plural in self.atoms.arrays:
                self.atoms.arrays[plural][self.index] = value
            else:
                array = np.zeros((len(self.atoms),) + shape, dtype)
                array[self.index] = value
                self.atoms.new_array(plural, array)

    def get_symbol(self): return self._get('symbol')
    def get_atomic_number(self): return self._get('number')
    def get_position(self): return self._get_copy('position')
    def _get_position(self): return self._get('position')
    def get_tag(self): return self._get('tag')
    def get_momentum(self): return self._get_copy('momentum')
    def _get_momentum(self): return self._get('momentum')
    def get_mass(self): return self._get('mass')
    def get_initial_magnetic_moment(self): return self._get('magmom')
    def get_charge(self): return self._get('charge')

    def set_symbol(self, symbol): self._set('symbol', symbol)
    def set_atomic_number(self, number): self._set('number', number)
    def set_position(self, position):
        self._set('position', np.array(position, float))
    def set_tag(self, tag): self._set('tag', tag)
    def set_momentum(self, momentum): self._set('momentum', momentum)
    def set_mass(self, mass): self._set('mass', mass)
    def set_initial_magnetic_moment(self, magmom): self._set('magmom', magmom)
    def set_charge(self, charge): self._set('charge', charge)

    def set_magmom(self, magmom):
        "Deprecated, use set_initial_magnetic_moment instead."
        import warnings
        warnings.warn('set_magmom is deprecated. Please use set_initial_magnetic_moment' \
                      ' instead.', DeprecationWarning, stacklevel=2)
        return self.set_initial_magnetic_moment(magmom)

    def get_number(self):
        "Deprecated, use get_atomic_number instead."
        import warnings
        warnings.warn(
            'get_number is deprecated. Please use get_atomic_number instead.',
            DeprecationWarning, stacklevel=2)
        return self.get_atomic_number()
        
    def set_number(self, number):
        "Deprecated, use set_atomic_number instead."
        import warnings
        warnings.warn(
            'set_number is deprecated. Please use set_atomic_number instead.',
            DeprecationWarning, stacklevel=2)
        return self.set_atomic_number(number)
        
    symbol = property(get_symbol, set_symbol, doc='Chemical symbol')
    number = property(get_atomic_number, set_atomic_number, doc='Atomic number')
    position = property(_get_position, set_position, doc='XYZ-coordinates')
    tag = property(get_tag, set_tag, doc='Integer tag')
    momentum = property(_get_momentum, set_momentum, doc='XYZ-momentum')
    mass = property(get_mass, set_mass, doc='Atomic mass')
    magmom = property(get_initial_magnetic_moment, set_initial_magnetic_moment,
                      doc='Initial magnetic moment')
    charge = property(get_charge, set_charge, doc='Atomic charge')

    def get_x(self): return self.position[0]
    def get_y(self): return self.position[1]
    def get_z(self): return self.position[2]
    
    def set_x(self, x): self.position[0] = x
    def set_y(self, y): self.position[1] = y
    def set_z(self, z): self.position[2] = z

    x = property(get_x, set_x, doc='X-coordiante')
    y = property(get_y, set_y, doc='Y-coordiante')
    z = property(get_z, set_z, doc='Z-coordiante')

