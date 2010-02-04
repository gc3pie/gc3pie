#!/usr/bin/env python
from ase import *

# Read in the geometry from a xyz file, set the cell, boundary conditions and center
atoms = read('geom.xyz')
atoms.set_cell([7.66348,7.66348,7.66348*2])
atoms.set_pbc((1,1,1))
atoms.center()

# Set initial velocities for hydrogen atoms along the z-direction
p = atoms.get_momenta()
p[0,2]= -1.5
p[1,2]= -1.5
atoms.set_momenta(p)

# Keep some atoms fixed during the simulation
atoms.set_constraint(FixAtoms(indices=range(18,38)))

# Set the calculator and attach it to the system
calc = Siesta('si001+h2',basis='SZ',xc='PBE',meshcutoff=50*Ry)
calc.set_fdf('PAO.EnergyShift', 0.25 * eV) 
calc.set_fdf('PAO.SplitNorm', 0.15)       
atoms.set_calculator(calc)

# Set the VelocityVerlet algorithm and run it
dyn = VelocityVerlet(atoms,dt=1.0 * fs,trajectory='si001+h2.traj')
dyn.run(steps=100)

