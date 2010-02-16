from ase import *
from ase.calculators.fleur import FLEUR
from ase.structure import bulk
from numpy import linspace

atoms = bulk('Ni', 'fcc', a=3.52)
calc = FLEUR(xc='PBE', kmax=3.6, kpts=(10, 10, 10), workdir='lat_const')
atoms.set_calculator(calc)
traj = PickleTrajectory('Ni.traj','w', atoms)
cell0 = atoms.get_cell()
for s in linspace(0.95, 1.05, 7):
    cell = cell0 * s
    atoms.set_cell((cell))
    ene = atoms.get_potential_energy()
    traj.write()

