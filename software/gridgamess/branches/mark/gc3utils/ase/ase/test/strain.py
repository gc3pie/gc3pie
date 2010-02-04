from ase import *
from ase.constraints import StrainFilter
a = 3.6
b = a / 2
cu = Atoms('Cu', cell=[(0,b,b),(b,0,b),(b,b,0)], pbc=1) * (6, 6, 6)
try:
    import Asap
except ImportError:
    pass
else:
    cu.set_calculator(ASAP())
    f = StrainFilter(cu, [1, 1, 1, 0, 0, 0])
    opt = MDMin(f, dt=0.01)
    t = PickleTrajectory('Cu.traj', 'w', cu)
    opt.attach(t)
    opt.run(0.001)

# HCP:
from ase.lattice.surface import hcp0001
cu = hcp0001('Cu', (1, 1, 2), a=a / sqrt(2))
cu.cell[1,0] += 0.05
cu *= (6, 6, 3)
try:
    import Asap
except ImportError:
    pass
else:
    cu.set_calculator(ASAP())
    f = StrainFilter(cu)
    opt = MDMin(f, dt=0.01)
    t = PickleTrajectory('Cu.traj', 'w', cu)
    opt.attach(t)
    opt.run(0.01)

