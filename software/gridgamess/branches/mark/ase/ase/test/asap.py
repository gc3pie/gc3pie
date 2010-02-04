from ase import *
from ase.calculators.emt import ASAP

try:
    import Asap
except ImportError:
    pass
else:
    a = Atoms('Cu2', positions=[(0, 0, 0), (0, 0, 2.7)],
              calculator=ASAP())
    print a.get_distance(0, 1), a.get_potential_energy()
    QuasiNewton(a).run(0.01)
    print a.get_distance(0, 1), a.get_potential_energy()
