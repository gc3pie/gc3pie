import os
import math
import random
import numpy as np

import cPickle as pickle
import new

from ase.cluster.clusteratom import ClusterAtom
import ase.cluster.data as data
from ase import Atoms
from ase.data import atomic_numbers, chemical_symbols, reference_states
from ase.lattice.cubic import SimpleCubic, BodyCenteredCubic, FaceCenteredCubic, Diamond

class Cluster(Atoms):
    _datasyn = {'numbers':       ('number',       int,   ()  ),
                'positions':     ('position',     float, (3,)),
                'tags':          ('tag',          int,   ()  ),
                'momenta':       ('momentum',     float, (3,)),
                'masses':        ('mass',         float, ()  ),
                'magmoms':       ('magmom',       float, ()  ),
                'charges':       ('charge',       float, ()  ),
                'neighbors':     ('neighbors',    int,   None),
                'coordinations': ('coordination', int,   ()  ),
                'types':         ('type',         int,   ()  ),
               }

    def __init__(self, symbol=None, layers=None, positions=None,
                 latticeconstant=None, symmetry=None, cell=None, 
                 center=None, multiplicity=1, filename=None, debug=0):

        self.debug = debug
        self.multiplicity = multiplicity

        if filename is not None:
            self.read(filename)
            return
        
        #Find the atomic number
        if symbol is not None:
            if isinstance(symbol, str):
                self.atomic_number = atomic_numbers[symbol]
            else:
                self.atomic_number = symbol
        else:
            raise Warning('You must specify a atomic symbol or number!')

        #Find the crystal structure
        if symmetry is not None:
            if symmetry.lower() in ['bcc', 'fcc', 'hcp']:
                self.symmetry = symmetry.lower()
            else:
                raise Warning('The %s symmetry does not exist!' % symmetry.lower())
        else:
            self.symmetry = reference_states[self.atomic_number]['symmetry'].lower()

        if self.debug:
            print 'Crystal structure:', self.symmetry

        #Find the lattice constant
        if latticeconstant is None:
            if self.symmetry == 'fcc':
                self.lattice_constant = reference_states[self.atomic_number]['a']
            else:
                raise Warning(('Cannot find the lattice constant ' +
                               'for a %s structure!' % self.symmetry))
        else:
            self.lattice_constant = latticeconstant

        if self.debug:
            print 'Lattice constant(s):', self.lattice_constant

        #Make the cluster of atoms
        if layers is not None and positions is None:
            layers = list(layers)

            #Make base crystal based on the found symmetry
            if self.symmetry == 'fcc':
                if len(layers) != data.lattice[self.symmetry]['surface_count']:
                    raise Warning('Something is wrong with the defined number of layers!')

                xc = int(np.ceil(layers[1] / 2.0)) + 1
                yc = int(np.ceil(layers[3] / 2.0)) + 1
                zc = int(np.ceil(layers[5] / 2.0)) + 1

                xs = xc + int(np.ceil(layers[0] / 2.0)) + 1
                ys = yc + int(np.ceil(layers[2] / 2.0)) + 1
                zs = zc + int(np.ceil(layers[4] / 2.0)) + 1

                center = np.array((xc, yc, zc)) * self.lattice_constant
                size = (xs, ys, zs)

                if self.debug:
                    print 'Base crystal size:', size
                    print 'Center cell position:', center

                atoms = FaceCenteredCubic(symbol=symbol,
                                          size=size,
                                          latticeconstant=self.lattice_constant,
                                          align=False)

            else:
                raise Warning(('The %s crystal structure is not' +
                               ' supported yet.') % self.symmetry)

            positions = atoms.get_positions()
            numbers = atoms.get_atomic_numbers()
            cell = atoms.get_cell()
        elif positions is not None:
            numbers = [self.atomic_number] * len(positions)
        else:
            numbers = None

        #Load the constructed atoms object into this object
        self.set_center(center)
        Atoms.__init__(self, numbers=numbers, positions=positions,
                       cell=cell, pbc=False)

        #Construct the particle with the assigned surfasces
        if layers is not None:
            self.set_layers(layers)

    def __repr__(self):
        output = ('Cluster(symbols=%s%s, latticeconstant=%.2f' % 
                  (len(self), chemical_symbols[self.atomic_number], self.lattice_constant))

        for name in self.arrays:
            output += ', %s=...' % name

        output += ', cell=%s' % self._cell.diagonal().tolist()

        if self.get_center() is not None:
            output += ', center=%s' % self.get_center().tolist()

        return output + ')'

    def __getitem__(self, i):
        c = ClusterAtom(atoms=self, index=i)
        return c

    def __delitem__(self, i):
        Atoms.__delitem__(self, i)

        #Subtract one from all neighbor references that is greater than "index"
        if self.has('neighbors'):
            neighbors = self.get_neighbors()
            neighbors = neighbors - (neighbors > i).astype(int)
            self.set_neighbors(neighbors)

    def pop(self, i=-1):
        atom = self[i]
        atom.cut_reference_to_atoms()

        #Subtract one from all neighbor references that is greater than "i"
        if atom.has('neighbors'):
            atom.neighbors = atom.neighbors - (atom.neighbors > i).astype(int)

        del self[i]
        return atom

    def __setitem__(self, i, atom):
        #raise Warning('Use direct assignment like atoms[i].type = x!')

        #If implemented make sure that all values are cleared before copied
        if not isinstance(atom, ClusterAtom):
            raise Warning('The added atom is not a ClusterAtom instance!')

        for name in self.arrays.keys():
            singular, dtype, shape = self._datasyn[name]
            self[i]._set(singular, np.zeros(shape, dtype))

        self[i]._set('number', atom._get('number', True))

        for name in atom._data:
            self[i]._set(name, atom._get(name, True))

    def append(self, atom):
        if not isinstance(atom, ClusterAtom):
            raise Warning('The added atom is not a ClusterAtom instance!')

        n = len(self)

        for name, a in self.arrays.items():
            b = np.zeros((n + 1,) + a.shape[1:], a.dtype)
            b[:n] = a
            self.arrays[name] = b

        for name in atom._data:
            self[-1]._set(name, atom._get(name, True))

    def extend(self, atoms):
        if not isinstance(atoms, Cluster):
            raise Warning('The added atoms is not a Cluster instance!')

        for atom in atoms:
            self.append(atom)

    def copy(self):
        cluster = Cluster(symbol=self.atomic_number,
                          latticeconstant=self.lattice_constant,
                          symmetry=self.symmetry,
                          cell=self.get_cell(),
                          center=self.get_center())

        for name, a in self.arrays.items():
            cluster.arrays[name] = a.copy()

        return cluster

    #Special Monte Carlo functions to alter the atoms
    def add_atom(self, atom):
        atom.index = len(self)
        self.append(atom)

        if self.has('neighbors'):
            self.update_neighborlist(i=atom.index, new=atom.neighbors)

    def remove_atom(self, i):
        atom = self.pop(i)

        if self.has('neighbors'):
            self.update_neighborlist(old=atom.neighbors)

        return atom

    def move_atom(self, i, other):
        atom = self[i]
        atom.cut_reference_to_atoms()
        self[i] = other

        if self.has('neighbors'):
            self.update_neighborlist(i=i, new=other.neighbors, old=atom.neighbors)

    def update_neighborlist(self, i=None, new=None, old=None):
        """Updates the neighbor list around atoms that is removed and added."""

        neighbor_mapping = data.lattice[self.symmetry]['neighbor_mapping']

        #Add "i" in the new neighbors neighborlists
        if new is not None and i is not None:
            for j, n in enumerate(new):
                if n >= 0 and n != i:
                    self[n].neighbors[neighbor_mapping[j]] = i
                    self[n].coordination += 1
                    self[n].type = self.get_atom_type(self[n].neighbors)
                elif n == i:
                    self[n].neighbors[j] = -1
                    self[n].coordination -= 1
                    self[n].type = self.get_atom_type(self[n].neighbors)

        #Set "-1" in the old neighbors neighborlists
        if old is not None:
            for j, n in enumerate(old):
                if n >= 0:
                    self[n].neighbors[neighbor_mapping[j]] = -1
                    self[n].coordination -= 1
                    self[n].type = self.get_atom_type(self[n].neighbors)

    def make_neighborlist(self):
        """Generate a lists with nearest neighbors, types and coordinations"""
        from asap3 import FullNeighborList
        neighbor_cutoff = data.lattice[self.symmetry]['neighbor_cutoff']
        neighbor_cutoff *= self.lattice_constant
        neighbor_numbers = data.lattice[self.symmetry]['neighbor_numbers']
        neighbor_count = data.lattice[self.symmetry]['neighbor_count']

        get_neighbors = FullNeighborList(neighbor_cutoff, self)

        positions = self.get_positions()
        neighbors = []
        coordinations = []
        types = []

        for i, pos in enumerate(positions):
            nl = get_neighbors[i]
            dl = (positions[nl] - pos) / self.lattice_constant

            neighbors.append([-1] * neighbor_count)
            for n, d in zip(nl, dl):
                name = tuple(d.round(1))
                if name in neighbor_numbers:
                    neighbors[i][neighbor_numbers[name]] = n

            coordinations.append(self.get_atom_coordination(neighbors[i]))
            types.append(self.get_atom_type(neighbors[i]))

        self.set_neighbors(neighbors)
        self.set_coordinations(coordinations)
        self.set_types(types)

    def get_number_of_layers(self, normal):
        """Get the number of layers in the direction given by 'normal'"""
        positions = self.get_positions() - self.get_center()
        
        d = self.lattice_constant * self.get_surface_data('d', normal)
        n = np.array(normal)
        r = np.dot(positions, n) / np.linalg.norm(n)
        layers = np.int(np.round(r.max() / d))

        return layers

    def set_number_of_layers(self, normal, layers):
        """Set the number of layers in the direction given by 'normal'"""
        positions = self.get_positions() - self.get_center()
        
        d = self.lattice_constant * self.get_surface_data('d', normal)
        n = np.array(normal)
        r = np.dot(positions, n) / np.linalg.norm(n)

        actual_layers = self.get_number_of_layers(normal)
        
        if layers > actual_layers:
            if self.debug:
                print ('Expanding the %s plane to %i from %i layers (Not supported yet)'
                       % (normal, layers, actual_layers))
            #raise Warning('Your cluster is not right!')
        
        elif layers < actual_layers:
            if self.debug:
                print ('Cutting the %s plane to %i from %i layers.'
                       % (normal, layers, actual_layers))

            rmax = (layers + 0.5) * d
            mask = np.less(r, rmax)

            for name in self.arrays.keys():
                self.arrays[name] = self.arrays[name][mask]

        else:
            if self.debug:
                print ('Keeping the %s plane at %i layers.'
                       % (normal, layers))

    def get_layers(self):
        surface_names = data.lattice[self.symmetry]['surface_names']
        layers = []

        for n in surface_names:
            layers.append(self.get_number_of_layers(n))

        return np.array(layers, int)

    def set_layers(self, layers):
        surface_names = data.lattice[self.symmetry]['surface_names']
        surface_count = data.lattice[self.symmetry]['surface_count']

        if len(layers) != surface_count:
            raise Warning(('The number of surfaces is not right: %i != %i' %
                           (len(layers), surface_count)))

        for n, l in zip(surface_names, layers):
            self.set_number_of_layers(normal=n, layers=l)

    def get_diameter(self):
        """Makes an estimate of the cluster diameter based on the average
        distance between opposit layers"""
        surface_mapping = data.lattice[self.symmetry]['surface_mapping']
        surface_names = data.lattice[self.symmetry]['surface_names']
        surface_numbers = data.lattice[self.symmetry]['surface_numbers']
        surface_data = data.lattice[self.symmetry]['surface_data']
        surface_count = data.lattice[self.symmetry]['surface_count']

        d = 0.0
        for s1 in surface_names:
            s2 = surface_names[surface_mapping[surface_numbers[s1]]]
            l1 = self.get_number_of_layers(s1)
            l2 = self.get_number_of_layers(s2)
            dl = surface_data[surface_numbers[s1]]['d'] * self.lattice_constant
            d += (l1 + l2) * dl / surface_count

        return d

    def get_atom_coordination(self, neighbors):
        neighbors = np.array(neighbors)
        neighbors = neighbors[neighbors >= 0]
        return len(neighbors)

    def get_atom_type(self, neighbors):
        neighbors = np.array(neighbors)
        name = tuple((neighbors >= 0).astype(int))

        type_names = data.lattice[self.symmetry]['type_names']
        type_numbers = data.lattice[self.symmetry]['type_numbers']
        type_data = data.lattice[self.symmetry]['type_data']

        if name in type_names:
            return type_data[type_numbers[name]]['type']
        else:
            return 0

    #Functions to acces the properties
    def get_center(self):
        return self._center.copy()

    def set_center(self, center):
        self._center = np.array(center, float)

    def get_positions(self):
        return self.get_array('positions')

    def set_positions(self, positions):
        self.set_array('positions', positions, float)

    def get_neighbors(self):
        return self.get_array('neighbors')

    def get_neighbors_bool(self, index):
        return tuple((self.get_neighbors()[index] >= 0).astype(int))

    def set_neighbors(self, neighbors):
        self.set_array('neighbors', neighbors, int)

    def get_types(self):
        return self.get_array('types')

    def set_types(self, types):
        self.set_array('types', types, int)

    def get_coordinations(self):
        return self.get_array('coordinations')

    def set_coordinations(self, coordinations):
        self.set_array('coordinations', coordinations, int)

    #Functions to store the cluster
    def write(self, filename=None):
        if not isinstance(filename, str):
            raise Warning('You must specify a valid filename.')

        if os.path.isfile(filename):
            os.rename(filename, filename + '.bak')

        d = {'symbol': self.atomic_number,
             'latticeconstant': self.lattice_constant,
             'symmetry': self.symmetry,
             'multiplicity': self.multiplicity,
             'center': self.get_center(),
             'cell': self.get_cell(),
             'pbc': self.get_pbc()}

        f = open(filename, 'w')
        f.write('Cluster')
        pickle.dump(d, f)
        pickle.dump(self.arrays, f)
        f.close()

    def read(self, filename):
        if not os.path.isfile(filename):
            raise Warning('The file specified do not exist.')

        f = open(filename, 'r')

        try:
            if f.read(len('Cluster')) != 'Cluster':
                raise Warning('This is not a compatible file.')
            d = pickle.load(f)
            self.arrays = pickle.load(f)
        except EOFError:
            raise Warinig('Bad file.')

        f.close()

        if 'multiplicity' in d:
            self.multiplicity = d['multiplicity']
        else:
            self.multiplicity = 1

        self.atomic_number = d['symbol']
        self.lattice_constant = d['latticeconstant']
        self.symmetry = d['symmetry']
        self.set_center(d['center'])
        self.set_cell(d['cell'])
        self.set_pbc(d['pbc'])
        self.set_constraint()
        self.adsorbate_info = {}
        self.calc = None

    #Helping fucntions
    def get_surface_data(self, name=None, surface=None):
        if name is None or surface is None:
            return None
        else:
            surface_numbers = data.lattice[self.symmetry]['surface_numbers']
            surface_data = data.lattice[self.symmetry]['surface_data']

            if isinstance(surface, tuple):
                return surface_data[surface_numbers[surface]][name]
            else:
                return surface_data[surface][name]
