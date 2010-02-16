from ase import *
from ase.optimize.lbfgs import LBFGS
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
    f = UnitCellFilter(cu, [1, 1, 1, 0, 0, 0])
    opt = LBFGS(f)
    t = PickleTrajectory('Cu-fcc.traj', 'w', cu)
    opt.attach(t)
    opt.run(5.0)

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
    print cu.get_forces()
    print cu.get_stress()
    f = UnitCellFilter(cu)
    opt = MDMin(f,dt=0.01)
    t = PickleTrajectory('Cu-hcp.traj', 'w', cu)
    opt.attach(t)
    opt.run(0.2)

