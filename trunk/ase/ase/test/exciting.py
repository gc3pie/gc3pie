import os
from ase import Atoms
from ase.io import *
from ase.calculators import Exciting
from ase.units import Bohr, Hartree
from ase.test import NotAvailable

try:
    import lxml
except ImportError:
    raise NotAvailable('This test need lxml module.')

a = Atoms('N3O',
          [(0, 0, 0), (1, 0, 0), (0, 0, 1), (0.5, 0.5, 0.5)],
          pbc=True)

raise NotAvailable('Problem with lxml module.')

write('geo.exi', a)
b = read('geo.exi')

print a
print a.get_positions()
print b
print b.get_positions()

calculator = Exciting(dir='excitingtestfiles',
                      kpts=(4, 4, 3),
                      maxscl=3,
                      #bin='/fshome/chm/git/exciting/bin/excitingser'
                      )
