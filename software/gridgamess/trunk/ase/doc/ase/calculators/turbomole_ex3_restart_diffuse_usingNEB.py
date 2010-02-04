#!/usr/bin/env python
from ase import *
import os

initial = read('initial.coord')
final = read('final.coord')
os.system('rm -f coord; cp initial.coord coord')

#restart
configs = read('neb.traj@-5:')

band = NEB(configs, climb=True)

#Set calculators
for config in configs:
    config.set_calculator(Turbomole())

# Optimize:
relax = QuasiNewton(band, trajectory='neb.traj')
relax.run(fmax=0.05)

