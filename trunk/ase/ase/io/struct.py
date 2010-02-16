from math import sin, cos, pi, sqrt

import numpy as np

from ase.atoms import Atoms, Atom
from ase.units import Bohr

def read_struct(fileobj):
    cell, lattice, pos, atomtype = extract_struct(fileobj)
    a = cell[0]
    b = cell[1]
    c = cell[2]
    alpha = cell[3] * pi / 180
    beta = cell[4] * pi / 180
    gamma = cell[5] * pi / 180
    va = a * np.array([1, 0, 0])
    vb = b * np.array([cos(gamma), sin(gamma), 0])
    cx = cos(beta)
    cy = (cos(alpha) - cos(beta) * cos(gamma)) / sin(gamma)
    cz = sqrt(1. - cx * cx - cy * cy)
    vc = c * np.array([cx, cy, cz])
    cell2 = np.array([va, vb, vc])
    atoms = Atoms(cell = cell2)
    for i in range(len(atomtype)):
        pos2 = (pos[i, 0] * va + pos[i, 1] * vb + pos[i, 2] * vc)
        atoms.append(Atom(atomtype[i].strip(), pos2))
    return atoms
    
def read_scf(filename):
    f = open(filename + '.scf', 'r')
    pip = f.readlines()
    for line in pip:
        if line[0:4] == ':ENE':
            ene = float(line[43:59])
    return ene * Ry

def extract_struct(filename):
    f = open(filename, 'r')
    pip = f.readlines()
    lattice = pip[1][0]
    nat = int(pip[1][27:30])
    cell = np.zeros(6)
    for i in range(6):
        cell[i] = float(pip[3][0 + i * 10:10 + i * 10])
    cell[0:3] = cell[0:3] * Bohr
    pos = np.array([])
    atomtype = []
    neq = np.zeros(nat)
    iline = 4
    indif = 0
    for iat in range(nat):
        indifini = indif
        if len(pos) == 0:
            pos = np.array([[float(pip[iline][12:22]),
                             float(pip[iline][25:35]),
                             float(pip[iline][38:48])]])
        else:
            pos = np.append(pos, np.array([[float(pip[iline][12:22]),
                                            float(pip[iline][25:35]),
                                            float(pip[iline][38:48])]]),
                            axis = 0)
        indif += 1
        iline += 1
        neq[iat] = int(pip[iline][15:17])
        iline += 1
        for ieq in range(1, int(neq[iat])):
            pos = np.append(pos, np.array([[float(pip[iline][12:22]),
                                            float(pip[iline][25:35]),
                                            float(pip[iline][38:48])]]),
                            axis = 0)
            indif += 1
            iline += 1
        for i in range(indif - indifini):
            atomtype.append(pip[iline][0:2])
        iline += 4
    #return cell, lattice, pos[:, :indif], atomtype
    return cell, lattice, pos, atomtype

def write_struct(filename, atoms = None):
    f = file(filename, 'w')
    f.write('ASE generated\n')
    nat = len(atoms)
    f.write('P   LATTICE,NONEQUIV.ATOMS:%3i\nMODE OF CALC=RELA\n'%nat)
    cell = atoms.get_cell()
    metT = np.dot(cell, np.transpose(cell))
    cell2 = cellconst(metT)
    cell2[0:3] = cell2[0:3] / Bohr
    f.write(('%10.6f' * 6) % tuple(cell2) + '\n')
    #print atoms.get_positions()[0]
    for ii in range(nat):
        f.write('ATOM %3i: ' % (ii + 1))
        pos = atoms.get_scaled_positions()[ii]
        f.write('X=%10.8f Y=%10.8f Z=%10.8f\n' % tuple(pos))
        f.write('          MULT= 1          ISPLIT= 1\n')
        zz = atoms.get_atomic_numbers()[ii]
        if zz > 71:
            ro = 0.000005 
        elif zz > 36:
            ro = 0.00001 
        else:
            ro = 0.0001
        f.write('%-10s NPT=%5i  R0=%9.8f RMT=%10.4f   Z:%10.5f\n' %
                (atoms.get_chemical_symbols()[ii], 781, ro, 2., zz))
        f.write('LOCAL ROT MATRIX:    %9.7f %9.7f %9.7f\n' % (1.0, 0.0, 0.0))
        f.write('                     %9.7f %9.7f %9.7f\n' % (0.0, 1.0, 0.0))
        f.write('                     %9.7f %9.7f %9.7f\n' % (0.0, 0.0, 1.0))
    f.write('   0\n')

def cellconst(metT):
    aa = np.sqrt(metT[0, 0])
    bb = np.sqrt(metT[1, 1])
    cc = np.sqrt(metT[2, 2])
    gamma = np.arccos(metT[0, 1] / (aa * bb)) / np.pi * 180.0
    beta  = np.arccos(metT[0, 2] / (aa * cc)) / np.pi * 180.0
    alpha = np.arccos(metT[1, 2] / (bb * cc)) / np.pi * 180.0
    return np.array([aa, bb, cc, alpha, beta, gamma])
