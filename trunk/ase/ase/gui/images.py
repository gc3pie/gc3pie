from math import sqrt

import numpy as np

from ase.data import covalent_radii
from ase.atoms import Atoms
from ase.calculators import SinglePointCalculator
from ase.io import read, write, string2index


class Images:
    def __init__(self, images=None):
        if images is not None:
            self.initialize(images)
    
    def initialize(self, images, filenames=None):
        self.natoms = len(images[0])
        self.nimages = len(images)
        if filenames is None:
            filenames = [None] * self.nimages
        self.filenames = filenames
        self.P = np.empty((self.nimages, self.natoms, 3))
        self.E = np.empty(self.nimages)
        self.K = np.empty(self.nimages)
        self.F = np.empty((self.nimages, self.natoms, 3))
        self.M = np.empty((self.nimages, self.natoms))
        self.A = np.empty((self.nimages, 3, 3))
        self.Z = images[0].get_atomic_numbers()
        self.pbc = images[0].get_pbc()
        warning = False
        for i, atoms in enumerate(images):
            natomsi = len(atoms)
            if (natomsi != self.natoms or
                (atoms.get_atomic_numbers() != self.Z).any()):
                raise RuntimeError('Can not handle different images with ' +
                                   'different numbers of atoms or different ' +
                                   'kinds of atoms!')
            self.P[i] = atoms.get_positions()
            self.A[i] = atoms.get_cell()
            if (atoms.get_pbc() != self.pbc).any():
                warning = True
            try:
                self.E[i] = atoms.get_potential_energy()
            except RuntimeError:
                self.E[i] = np.nan
            self.K[i] = atoms.get_kinetic_energy()
            try:
                self.F[i] = atoms.get_forces(apply_constraint=False)
            except RuntimeError:
                self.F[i] = np.nan
            try:
                self.M[i] = atoms.get_magnetic_moments()
            except RuntimeError:
                self.M[i] = np.nan

        if warning:
            print('WARNING: Not all images have the same bondary conditions!')
            
        self.selected = np.zeros(self.natoms, bool)
        self.visible = np.ones(self.natoms, bool)
        self.nselected = 0
        self.set_dynamic()
        self.repeat = np.ones(3, int)
        self.set_radii(0.89)

    def set_radii(self, scale):
        self.r = covalent_radii[self.Z] * scale
                
    def read(self, filenames, index=-1):
        images = []
        names = []
        for filename in filenames:
            i = read(filename, index)
            if not isinstance(i, list):
                i = [i]
            images.extend(i)
            names.extend([filename] * len(i))
        self.initialize(images, names)

    def repeat_images(self, repeat):
        n = self.repeat.prod()
        repeat = np.array(repeat)
        self.repeat = repeat
        N = repeat.prod()
        natoms = self.natoms // n
        P = np.empty((self.nimages, natoms * N, 3))
        F = np.empty((self.nimages, natoms * N, 3))
        Z = np.empty(natoms * N, int)
        r = np.empty(natoms * N)
        dynamic = np.empty(natoms * N, bool)
        a0 = 0
        for i0 in range(repeat[0]):
            for i1 in range(repeat[1]):
                for i2 in range(repeat[2]):
                    a1 = a0 + natoms
                    for i in range(self.nimages):
                        P[i, a0:a1] = (self.P[i, :natoms] +
                                       np.dot((i0, i1, i2), self.A[i]))
                    F[:, a0:a1] = self.F[:, :natoms]
                    Z[a0:a1] = self.Z[:natoms]
                    r[a0:a1] = self.r[:natoms]
                    dynamic[a0:a1] = self.dynamic[:natoms]
                    a0 = a1
        self.P = P
        self.F = F
        self.Z = Z
        self.r = r
        self.dynamic = dynamic
        self.natoms = natoms * N
        self.selected = np.zeros(natoms * N, bool)
        self.visible = np.ones(natoms * N, bool)
        self.nselected = 0
        
    def graph(self, expr):
        code = compile(expr + ',', 'atoms.py', 'eval')

        n = self.nimages
        def d(n1, n2):
            return sqrt(((R[n1] - R[n2])**2).sum())
        S = self.selected
        D = self.dynamic[:, np.newaxis]
        E = self.E
        s = 0.0
        data = []
        for i in range(n):
            R = self.P[i]
            F = self.F[i]
            M = self.M[i]
            A = self.A[i]
            f = ((F * D)**2).sum(1)**.5
            fmax = max(f)
            fave = f.mean()
            epot = E[i]
            ekin = self.K[i]
            e = epot + ekin
            data = eval(code)
            if i == 0:
                m = len(data)
                xy = np.empty((m, n))
            xy[:, i] = data
            if i + 1 < n:
                s += sqrt(((self.P[i + 1] - R)**2).sum())
        return xy

    def set_dynamic(self):
        if self.nimages == 1:
            self.dynamic = np.ones(self.natoms, bool)
        else:
            self.dynamic = np.zeros(self.natoms, bool)
            R0 = self.P[0]
            for R in self.P[1:]:
                self.dynamic |= (np.abs(R - R0) > 1.0e-10).any(1)

    def write(self, filename, rotations='', show_unit_cell=False, bbox=None):
        indices = range(self.nimages)
        p = filename.rfind('@')
        if p != -1:
            try:
                slice = string2index(filename[p + 1:])
            except ValueError:
                pass
            else:
                indices = indices[slice]
                filename = filename[:p]
                if isinstance(indices, int):
                    indices = [indices]

        images = [self.get_atoms(i) for i in indices]
        try:
            write(filename, images, 
                  rotation=rotations, show_unit_cell=show_unit_cell,
                  bbox=bbox)
        except:
            write(filename, images)

    def get_atoms(self, frame):
        atoms = Atoms(positions=self.P[frame],
                      numbers=self.Z,
                      cell=self.A[frame],
                      pbc=self.pbc)
        atoms.set_calculator(SinglePointCalculator(self.E[frame],
                                                   self.F[frame],
                                                   None, None, atoms))
        return atoms
                           
    def delete(self, i):
        self.nimages -= 1
        P = np.empty((self.nimages, self.natoms, 3))
        F = np.empty((self.nimages, self.natoms, 3))
        A = np.empty((self.nimages, 3, 3))
        E = np.empty(self.nimages)
        P[:i] = self.P[:i]
        P[i:] = self.P[i + 1:]
        self.P = P
        F[:i] = self.F[:i]
        F[i:] = self.F[i + 1:]
        self.F = F
        A[:i] = self.A[:i]
        A[i:] = self.A[i + 1:]
        self.A = A
        E[:i] = self.E[:i]
        E[i:] = self.E[i + 1:]
        self.E = E
        del self.filenames[i]

    def aneb(self):
        n = self.nimages
        assert n % 5 == 0
        levels = n // 5
        n = self.nimages = 2 * levels + 3
        P = np.empty((self.nimages, self.natoms, 3))
        F = np.empty((self.nimages, self.natoms, 3))
        E = np.empty(self.nimages)
        for L in range(levels):
            P[L] = self.P[L * 5]
            P[n - L - 1] = self.P[L * 5 + 4]
            F[L] = self.F[L * 5]
            F[n - L - 1] = self.F[L * 5 + 4]
            E[L] = self.E[L * 5]
            E[n - L - 1] = self.E[L * 5 + 4]
        for i in range(3):
            P[levels + i] = self.P[levels * 5 - 4 + i]
            F[levels + i] = self.F[levels * 5 - 4 + i]
            E[levels + i] = self.E[levels * 5 - 4 + i]
        self.P = P
        self.F = F
        self.E = E

    def interpolate(self, m):
        assert self.nimages == 2
        self.nimages = 2 + m
        P = np.empty((self.nimages, self.natoms, 3))
        F = np.empty((self.nimages, self.natoms, 3))
        A = np.empty((self.nimages, 3, 3))
        E = np.empty(self.nimages)
        P[0] = self.P[0]
        F[0] = self.F[0]
        A[0] = self.A[0]
        E[0] = self.E[0]
        for i in range(1, m + 1):
            x = i / (m + 1.0)
            y = 1 - x
            P[i] = y * self.P[0] + x * self.P[1]
            F[i] = y * self.F[0] + x * self.F[1]
            A[i] = y * self.A[0] + x * self.A[1]
            E[i] = y * self.E[0] + x * self.E[1]
        P[-1] = self.P[1]
        F[-1] = self.F[1]
        A[-1] = self.A[1]
        E[-1] = self.E[1]
        self.P = P
        self.F = F
        self.A = A
        self.E = E
        self.filenames[1:1] = [None] * m
