from ase import *
from ase.vibrations import Vibrations

n2 = Atoms('N2',
           positions=[(0, 0, 0), (0, 0, 1.1)],
           calculator=EMT())
QuasiNewton(n2).run(fmax=0.01)
vib = Vibrations(n2)
vib.run()
print vib.get_frequencies()
vib.summary()
print vib.get_mode(-1)
vib.write_mode(-1, nimages=20)
