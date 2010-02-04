"""Effective medium theory potential."""

from math import sqrt, exp, log, pi

import numpy as np
import sys

from ase.data import atomic_numbers, chemical_symbols
from ase.units import Bohr


parameters = {
#          E0     s0    V0     eta2    kappa   lambda  n0
#          eV     bohr  eV     bohr^-1 bohr^-1 bohr^-1 bohr^-3
    'H':  (-2.21, 0.71, 2.132, 1.652,  2.790,  1.892,  0.00547, 'dimer'),
    'Al': (-3.28, 3.00, 1.493, 1.240,  2.000,  1.169,  0.00700, 'fcc'),
    'Cu': (-3.51, 2.67, 2.476, 1.652,  2.740,  1.906,  0.00910, 'fcc'),
    'Ag': (-2.96, 3.01, 2.132, 1.652,  2.790,  1.892,  0.00547, 'fcc'),
    'Au': (-3.80, 3.00, 2.321, 1.674,  2.873,  2.182,  0.00703, 'fcc'),
    'Ni': (-4.44, 2.60, 3.673, 1.669,  2.757,  1.948,  0.01030, 'fcc'),
    'Pd': (-3.90, 2.87, 2.773, 1.818,  3.107,  2.155,  0.00688, 'fcc'),
    'Pt': (-5.85, 2.90, 4.067, 1.812,  3.145,  2.192,  0.00802, 'fcc'),
    'C':  (-1.97, 1.18, 0.132, 3.652,  5.790,  2.892,  0.01322, 'dimer'),
    'N':  (-4.97, 1.18, 0.132, 2.652,  3.790,  2.892,  0.01222, 'dimer'),
    'O':  (-2.97, 1.25, 2.132, 3.652,  5.790,  4.892,  0.00850, 'dimer')}

beta = 1.809#(16 * pi / 3)**(1.0 / 3) / 2**0.5
eta1 = 0.5 / Bohr
acut = 25.0  # Use the same value as ASAP XXX

class EMT:

    acut = 5.9
    disabled = False  # Set to a message to disable (asap does this).
    
    def __init__(self):
        self.energy = None
        if self.disabled:
            print >>sys.stderr, """
            ase.EMT has been disabled by Asap.  Most likely, you
            intended to use Asap's EMT calculator, but accidentally
            imported ase's EMT calculator after Asap's.  This could
            happen if your script contains the lines
              from asap3 import *
              from ase import *
            Swap the two lines to solve the problem.

            In the UNLIKELY event that you actually wanted to use
            ase.EMT although asap3 is loaded into memory, please
            reactivate it with the command
              ase.EMT.disabled = False
            """
            raise RuntimeError('ase.EMT has been disabled.  ' +
                               'See message printed above.')
        
    def get_spin_polarized(self):
        return False
    
    def initialize(self, atoms):
        self.par = {}
        self.rc = 0.0
        self.numbers = atoms.get_atomic_numbers()
        for Z in self.numbers:
            if Z not in self.par:
                p = parameters[chemical_symbols[Z]]
                s0 = p[1] * Bohr
                eta2 = p[3] / Bohr
                kappa = p[4] / Bohr
                rc = beta * s0 * 0.5 * (sqrt(3) + sqrt(4))
                x = eta2 * beta * s0
                gamma1 = 0.0
                gamma2 = 0.0
                if p[7] == 'fcc':
                    for i, n in enumerate([12, 6, 24, 12]):
                        r = s0 * beta * sqrt(i + 1)
                        x = n / (12 * (1.0 + exp(acut * (r - rc))))
                        gamma1 += x * exp(-eta2 * (r - beta * s0))
                        gamma2 += x * exp(-kappa / beta * (r - beta * s0))
                elif p[7] == 'dimer':
                    r = s0 * beta
                    n = 1
                    x = n / (12 * (1.0 + exp(acut * (r - rc))))
                    gamma1 += x * exp(-eta2 * (r - beta * s0))
                    gamma2 += x * exp(-kappa / beta * (r - beta * s0))
                else:
                    raise RuntimeError
                    
                self.par[Z] = {'E0': p[0],
                               's0': s0,
                               'V0': p[2],
                               'eta2': eta2,
                               'kappa': kappa,
                               'lambda': p[5] / Bohr,
                               'n0': p[6] / Bohr**3,
                               'rc': rc,
                               'gamma1': gamma1,
                               'gamma2': gamma2}
                if rc + 0.5 > self.rc:
                    self.rc = rc + 0.5

        self.ksi = {}
        for s1, p1 in self.par.items():
            self.ksi[s1] = {}
            for s2, p2 in self.par.items():
                self.ksi[s1][s2] = (p2['n0'] / p1['n0'] *
                                    exp(eta1 * (p1['s0'] - p2['s0'])))
                
        self.forces = np.empty((len(atoms), 3))
        self.sigma1 = np.empty(len(atoms))
        self.deds = np.empty(len(atoms))
                    
    def update(self, atoms):
        if (self.energy is None or
            len(self.numbers) != len(atoms) or
            (self.numbers != atoms.get_atomic_numbers()).any()):
            self.initialize(atoms)
            self.calculate(atoms)
        elif ((self.positions != atoms.get_positions()).any() or
              (self.pbc != atoms.get_pbc()).any() or
              (self.cell != atoms.get_cell()).any()):
            self.calculate(atoms)

    def calculation_required(self, atoms, quantities):
        if len(quantities) == 0:
            return False

        return (self.energy is None or
                len(self.numbers) != len(atoms) or
                (self.numbers != atoms.get_atomic_numbers()).any() or
                (self.positions != atoms.get_positions()).any() or
                (self.pbc != atoms.get_pbc()).any() or
                (self.cell != atoms.get_cell()).any())
                
    def get_potential_energy(self, atoms):
        self.update(atoms)
        return self.energy

    def get_numeric_forces(self, atoms):
        self.update(atoms)
        p = atoms.positions
        p0 = p.copy()
        forces = np.empty_like(p)
        eps = 0.0001
        for a in range(len(p)):
            for c in range(3):
                p[a, c] += eps
                self.calculate(atoms)
                de = self.energy
                p[a, c] -= 2 * eps
                self.calculate(atoms)
                de -= self.energy
                p[a, c] += eps
                forces[a, c] = -de / (2 * eps)
        p[:] = p0
        return forces

    def get_forces(self, atoms):
        self.update(atoms)
        return self.forces.copy()
    
    def get_stress(self, atoms):
        raise NotImplementedError
    
    def calculate(self, atoms):
        self.positions = atoms.get_positions().copy()
        self.cell = atoms.get_cell().copy()
        self.pbc = atoms.get_pbc().copy()
        
        icell = np.linalg.inv(self.cell)
        scaled = np.dot(self.positions, icell)
        N = []
        for i in range(3):
            if self.pbc[i]:
                scaled[:, i] %= 1.0
                v = icell[:, i]
                h = 1 / sqrt(np.dot(v, v))
                N.append(int(self.rc / h) + 1)
            else:
                N.append(0)

        R = np.dot(scaled, self.cell)
        
        self.energy = 0.0
        self.sigma1[:] = 0.0
        self.forces[:] = 0.0
        
        N1, N2, N3 = N
        natoms = len(atoms)
        for i1 in range(-N1, N1 + 1):
            for i2 in range(-N2, N2 + 1):
                for i3 in range(-N3, N3 + 1):
                    C = np.dot((i1, i2, i3), self.cell)
                    Q = R + C
                    c = (i1 == 0 and i2 == 0 and i3 == 0)
                    for a1 in range(natoms):
                        Z1 = self.numbers[a1]
                        p1 = self.par[Z1]
                        ksi = self.ksi[Z1]
                        for a2 in range(natoms):
                            if c and a2 == a1:
                                continue
                            d = Q[a2] - R[a1]
                            r = sqrt(np.dot(d, d))
                            if r < p1['rc'] + 0.5:
                                Z2 = self.numbers[a2]
                                self.interact1(a1, a2, d, r, p1, ksi[Z2])
                                
        for a in range(natoms):
            Z = self.numbers[a]
            p = self.par[Z]
            try:
                ds = -log(self.sigma1[a] / 12) / (beta * p['eta2'])
            except (OverflowError, ValueError):
                self.deds[a] = 0.0
                self.energy -= p['E0']
                continue
            x = p['lambda'] * ds
            y = exp(-x)
            z = 6 * p['V0'] * exp(-p['kappa'] * ds)
            self.deds[a] = ((x * y * p['E0'] * p['lambda'] + p['kappa'] * z) /
                            (self.sigma1[a] * beta * p['eta2']))
            e = p['E0'] * ((1 + x) * y - 1) + z
            self.energy += p['E0'] * ((1 + x) * y - 1) + z

        for i1 in range(-N1, N1 + 1):
            for i2 in range(-N2, N2 + 1):
                for i3 in range(-N3, N3 + 1):
                    C = np.dot((i1, i2, i3), self.cell)
                    Q = R + C
                    c = (i1 == 0 and i2 == 0 and i3 == 0)
                    for a1 in range(natoms):
                        Z1 = self.numbers[a1]
                        p1 = self.par[Z1]
                        ksi = self.ksi[Z1]
                        for a2 in range(natoms):
                            if c and a2 == a1:
                                continue
                            d = Q[a2] - R[a1]
                            r = sqrt(np.dot(d, d))
                            if r < p1['rc'] + 0.5:
                                Z2 = self.numbers[a2]
                                self.interact2(a1, a2, d, r, p1, ksi[Z2])

    def interact1(self, a1, a2, d, r, p, ksi):
        x = exp(acut * (r - p['rc']))
        theta = 1.0 / (1.0 + x)
        y = (0.5 * p['V0'] * exp(-p['kappa'] * (r / beta - p['s0'])) *
             ksi / p['gamma2'] * theta)
        self.energy -= y
        f = y * (p['kappa'] / beta + acut * theta * x) * d / r
        self.forces[a1] += f
        self.forces[a2] -= f
        self.sigma1[a1] += (exp(-p['eta2'] * (r - beta * p['s0'])) *
                            ksi * theta / p['gamma1'])

    def interact2(self, a1, a2, d, r, p, ksi):
        x = exp(acut * (r - p['rc']))
        theta = 1.0 / (1.0 + x)
        y = (exp(-p['eta2'] * (r - beta * p['s0'])) *
             ksi / p['gamma1'] * theta * self.deds[a1])
        f = y * (p['eta2'] + acut * theta * x) * d / r
        self.forces[a1] -= f
        self.forces[a2] += f


class ASAP:
    def __init__(self):
        self.atoms = None
        
    def get_potential_energy(self, atoms):
        self.update(atoms)
        return self.atoms.GetPotentialEnergy()

    def get_forces(self, atoms):
        self.update(atoms)
        return np.array(self.atoms.GetCartesianForces())

    def get_stress(self, atoms):
        self.update(atoms)
        return np.array(self.atoms.GetStress())

    def update(self, atoms):
        from Numeric import array
        from Asap import ListOfAtoms, EMT as AsapEMT
        if self.atoms is None:
            self.atoms = ListOfAtoms(positions=array(atoms.positions),
                                     cell=array(atoms.get_cell()),
                                     periodic=tuple(atoms.get_pbc()))
            self.atoms.SetAtomicNumbers(array(atoms.get_atomic_numbers()))
            self.atoms.SetCalculator(AsapEMT())
        else:
            self.atoms.SetUnitCell(array(atoms.get_cell()), fix=True)
            self.atoms.SetCartesianPositions(array(atoms.positions))
        
    
