from math import sin, cos, pi, sqrt
import numpy as np

from ase.atoms import Atoms, Atom
from ase.parallel import paropen

"""Module to read and write atoms in cif file format"""


def read_cif(fileobj, index=-1):
    if isinstance(fileobj, str):
        fileobj = open(fileobj)

    def search_key(fobj, key):
        for line in fobj:
            if key in line:
                return line
        return None

    def get_key(fobj, key, pos=1):
        line = search_key(fobj, key)
        if line:
            return float(line.split()[pos].split('(')[0])
        return None

    a = get_key(fileobj, '_cell_length_a')
    b = get_key(fileobj, '_cell_length_b')
    c = get_key(fileobj, '_cell_length_c')
    alpha =  pi * get_key(fileobj, '_cell_angle_alpha') / 180
    beta = pi * get_key(fileobj, '_cell_angle_beta') / 180
    gamma =  pi * get_key(fileobj, '_cell_angle_gamma') / 180

    va = a * np.array([1, 0, 0])
    vb = b * np.array([cos(gamma), sin(gamma), 0])
    cx = cos(beta)
    cy = (cos(alpha) - cos(beta) * cos(gamma)) / sin(gamma)
    cz = sqrt(1. - cx*cx - cy*cy)
    vc = c * np.array([cx, cy, cz])
    cell = np.array([va, vb, vc])

    atoms = Atoms(cell=cell)
    read = False
    for line in fileobj:
        if not read:
            if '_atom_site_disorder_group' in line:
                read = True
        else:
            word = line.split()
            if len(word) < 5:
                break
            symbol = word[1]
            pos = (float(word[2].split('(')[0]) * va +
                   float(word[3].split('(')[0]) * vb +
                   float(word[4].split('(')[0]) * vc   )
            atoms.append(Atom(symbol, pos))

    return atoms

