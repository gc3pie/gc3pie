import numpy as np
import ase.cluster.data as data
from ase.atom import Atom, data as olddata
from ase.data import atomic_numbers, chemical_symbols, reference_states

class ClusterAtom(Atom):
    """Cluster Atom"""
    _datasyn = olddata
    _datasyn['neighbors'] = ('neighbors', int, None)
    _datasyn['coordination'] = ('coordinations', int, ())
    _datasyn['type'] = ('types', int, ())

    _data = []

    def __init__(self, symbol='X', position=(0.0, 0.0, 0.0), atoms=None, index=None):
        self.atoms = atoms
        self.index = index

        if atoms is None:
            if isinstance(symbol, str):
                self.number = atomic_numbers[symbol]
            else:
                self.number = symbol
 
            self.position = np.array(position, float)

    def __repr__(self):
        output = 'ClusterAtom(%s, %s' % (self.symbol, self.position.tolist())

        for name in self._data:
            if name != 'number' and name != 'position' and self._get(name) is not None:
                output += ', %s=%s' % (name, self._get(name))

        return output + ')'

    def _get(self, name, copy=False):
        if self.atoms is None:
            if not self.has(name): return None

            return getattr(self, '_' + name)
        else:
            plural = self._datasyn[name][0]
            if not self.atoms.has(plural): return None

            if copy:
                return self.atoms.arrays[plural][self.index].copy()
            else:
                return self.atoms.arrays[plural][self.index]

    def _get_copy(self, name): self._get(name, copy=True)

    def _set(self, name, value, copy=False):
        if name not in self._data: self._data.append(name)

        if self.atoms is None or copy is True:
            setattr(self, '_' + name, value)
        else:
            plural, dtype, shape = self._datasyn[name]
            if plural == 'neighbors':
                symmetry = reference_states[self.number]['symmetry'].lower()
                shape = (data.lattice[symmetry]['neighbor_count'],)

            if self.atoms.has(plural):
                self.atoms.arrays[plural][self.index] = value
            else:
                array = np.zeros((len(self.atoms),) + shape, dtype)
                array[self.index] = value
                self.atoms.set_array(plural, array)

    def has(self, name):
        return name in self._data

    def cut_reference_to_atoms(self):
        for name, a in self.atoms.arrays.items():
            self._set(self.atoms._datasyn[name][0], a[self.index].copy(), True)
        self.atoms = None
        self.index = None

    def get_symbol(self): return chemical_symbols[self._get('number')]
    def get_neighbors(self): return self._get('neighbors')
    def get_type(self): return self._get('type')
    def get_coordination(self): return self._get('coordination')
    #def get_(self): return self._get('')

    def set_symbol(self, value): self._set('number', atomic_numbers[value])
    def set_neighbors(self, value): self._set('neighbors', np.array(value, int))
    def set_type(self, value): self._set('type', value)
    def set_coordination(self, value): self._set('coordination', value)
    #def get_(self, value): return self._set('', value)

    symbol = property(get_symbol, set_symbol, doc='Chemical symbol')
    neighbors = property(get_neighbors, set_neighbors, doc='List of nearest neighbors')
    type = property(get_type, set_type, doc='Atom type')
    coordination = property(get_coordination, set_coordination, doc='Atom coordination')
