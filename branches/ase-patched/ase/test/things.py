from ase.dft import monkhorst_pack

assert [0, 0, 0] in  monkhorst_pack((1, 3, 5)).tolist()
assert [0, 0, 0] not in  monkhorst_pack((1, 3, 6)).tolist()
assert len(monkhorst_pack((3, 4, 6))) == 3 * 4 * 6

from ase.units import *
print Hartree, Bohr, kJ/mol, kcal/mol, kB*300, fs, 1/fs
