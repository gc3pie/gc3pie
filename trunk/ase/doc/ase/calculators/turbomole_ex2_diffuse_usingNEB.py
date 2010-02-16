#!/usr/bin/env python
from ase import *
import os

initial = read('initial.coord')
final = read('final.coord')
os.system('rm -f coord; cp initial.coord coord')

# Make a band consisting of 5 configs:
configs = [initial]
configs += [initial.copy() for i in range(3)]
configs += [final]

band = NEB(configs, climb=True)
# Interpolate linearly the positions of the not-endpoint-configs:
band.interpolate()

#Set calculators
for config in configs:
    config.set_calculator(Turbomole())

# Optimize the Path:
relax = QuasiNewton(band, trajectory='neb.traj')
relax.run(fmax=0.05)


