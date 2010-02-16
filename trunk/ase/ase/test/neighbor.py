import numpy.random as random
from ase import *
from ase.calculators.neighborlist import NeighborList

atoms = Atoms(numbers=range(10),
              cell=[(0.2, 1.2, 1.4),
                    (1.4, 0.1, 1.6),
                    (1.3, 2.0, -0.1)])
atoms.set_scaled_positions(3 * random.random((10, 3)) - 1)

def count(nl, atoms):
    c = np.zeros(len(atoms), int)
    R = atoms.get_positions()
    cell = atoms.get_cell()
    d = 0.0
    for a in range(len(atoms)):
        i, offsets = nl.get_neighbors(a)
        for j in i:
            c[j] += 1
        c[a] += len(i)
        d += (((R[i] + np.dot(offsets, cell) - R[a])**2).sum(1)**0.5).sum()
    return d, c

for sorted in [False, True]:
    for p1 in range(2):
        for p2 in range(2):
            for p3 in range(2):
                print p1, p2, p3
                atoms.set_pbc((p1, p2, p3))
                nl = NeighborList(atoms.numbers * 0.2 + 0.5,
                                  skin=0.0, sorted=sorted)
                nl.update(atoms)
                d, c = count(nl, atoms)
                atoms2 = atoms.repeat((p1 + 1, p2 + 1, p3 + 1))
                nl2 = NeighborList(atoms2.numbers * 0.2 + 0.5,
                                   skin=0.0, sorted=sorted)
                nl2.update(atoms2)
                d2, c2 = count(nl2, atoms2)
                c2.shape = (-1, 10)
                dd = d * (p1 + 1) * (p2 + 1) * (p3 + 1) - d2
                print dd
                print c2 - c
                assert abs(dd) < 1e-10
                assert not (c2 - c).any()

h2 = Atoms('H2', positions=[(0, 0, 0), (0, 0, 1)])
nl = NeighborList([0.5, 0.5], skin=0.1, sorted=True, self_interaction=False)
assert nl.update(h2)
assert not nl.update(h2)
assert (nl.get_neighbors(0)[0] == [1]).all()

h2[1].z += 0.09
assert not nl.update(h2)
assert (nl.get_neighbors(0)[0] == [1]).all()

h2[1].z += 0.09
assert nl.update(h2)
assert (nl.get_neighbors(0)[0] == []).all()
assert nl.nupdates == 2
