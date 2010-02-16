from ase import *

atoms = Atoms('H7',
              positions=[(0, 0, 0),
                         (1, 0, 0),
                         (0, 1, 0),
                         (1, 1, 0),
                         (0, 2, 0),
                         (1, 2, 0),
                         (0.5, 0.5, 1)],
              constraint=[FixAtoms(range(6))],
              calculator=LennardJones())

traj = PickleTrajectory('H.traj', 'w', atoms)
dyn = QuasiNewton(atoms, maxstep=0.2)
dyn.attach(traj.write)
dyn.run(fmax=0.01, steps=100)

try:
    del atoms[-1]
except RuntimeError:
    pass
else:
    raise RuntimeError

