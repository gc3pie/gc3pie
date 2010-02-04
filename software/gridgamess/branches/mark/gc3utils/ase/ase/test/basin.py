import numpy as np
from ase import *
from ase.calculators.lj import LennardJones
from ase.optimize.basin import BasinHopping

N = 7
R = N**(1./3.)
pos = np.random.uniform(-R, R, (N, 3))
s = Atoms('He' + str(N),
          positions = pos)
s.set_calculator(LennardJones())

ftraj = 'lowest.traj'
bh = BasinHopping(s, 
                  temperature=100 * kB, dr=0.5, 
                  trajectory=ftraj,
                  optimizer_logfile=None)
bh.run(10)

Emin, smin = bh.get_minimum()

# recalc energy
smin.set_calculator(LennardJones())
E = smin.get_potential_energy()
assert abs(E - Emin) < 1e-15
smim = read(ftraj)
E = smin.get_potential_energy()
assert abs(E - Emin) < 1e-15

#view(smin)
