"""Define a helper function for running tests

The skeleton for making a new setup is as follows:

from ase.optimize.test import run_test

def get_atoms():
    return Atoms('H')

def get_calculator():
    return EMT()

run_test(get_atoms, get_calculator, 'Hydrogen')
"""
import matplotlib
matplotlib.rcParams['backend']="PDF"

from ase.optimize.bfgs import BFGS
from ase.optimize.lbfgs import LBFGS, LineSearchLBFGS
from ase.optimize.fire import FIRE
from ase.optimize.mdmin import MDMin
from ase.optimize.sciopt import SciPyFminCG
from ase.optimize.sciopt import SciPyFminBFGS

import matplotlib.pyplot as pl
import numpy as np

optimizers = [
    'BFGS',
    'LBFGS',
    'LineSearchLBFGS',
    'FIRE',
    'MDMin',
    'SciPyFminCG',
    'SciPyFminBFGS',
]

def get_optimizer(optimizer):
    if optimizer == 'BFGS': return BFGS
    elif optimizer == 'LBFGS': return LBFGS
    elif optimizer == 'LineSearchLBFGS': return LineSearchLBFGS
    elif optimizer == 'FIRE': return FIRE
    elif optimizer == 'MDMin': return MDMin
    elif optimizer == 'SciPyFminCG': return SciPyFminCG
    elif optimizer == 'SciPyFminBFGS': return SciPyFminBFGS

def run_test(get_atoms, get_calculator, name,
             fmax=0.05, steps=100, plot=True):

    plotter = Plotter(name, fmax)
    for optimizer in optimizers:
        note = ''
        logname = name + '-' + optimizer

        atoms = get_atoms()
        atoms.set_calculator(get_calculator())
        opt = get_optimizer(optimizer)
        relax = opt(atoms,
                    logfile = logname + '.log',
                    trajectory = logname + '.traj')
    
        obs = DataObserver(atoms)
        relax.attach(obs)
        try:
            relax.run(fmax = fmax, steps = steps)
            E = atoms.get_potential_energy()
        except:
            note = 'An exception occurred'
            E = np.nan

        nsteps = relax.get_number_of_steps()
        if hasattr(relax, 'force_calls'):
            fc = relax.force_calls
            print '%-15s %-15s %3i %8.3f (%3i) %s' % (name, optimizer, nsteps, E, fc, note)
        else:
            print '%-15s %-15s %3i %8.3f       %s' % (name, optimizer, nsteps, E, note)

        plotter.plot(optimizer, obs.get_E(), obs.get_fmax())

    plotter.save()

class Plotter:
    def __init__(self, name, fmax):
        self.name = name
        self.fmax = fmax

        self.fig = pl.figure(figsize=[12.0, 9.0])
        self.axes0 = self.fig.add_subplot(2, 1, 1)
        self.axes1 = self.fig.add_subplot(2, 1, 2)

    def plot(self, optimizer, E, fmax):
        self.axes0.plot(E, label = optimizer)
        self.axes1.plot(fmax)

    def save(self, format='pdf'):
        self.axes0.legend()
        self.axes0.set_title(self.name)
        self.axes0.set_ylabel('E')
        #self.axes0.set_yscale('log')

        self.axes1.set_xlabel('steps')
        self.axes1.set_ylabel('fmax')
        self.axes1.set_yscale('log')
        self.axes1.axhline(self.fmax, color='k', linestyle='--')
        self.fig.savefig(self.name + '.' + format)

class DataObserver:
    def __init__(self, atoms):
        self.atoms = atoms
        self.E = []
        self.fmax = []

    def __call__(self):
        self.E.append(self.atoms.get_potential_energy())
        self.fmax.append(np.sqrt((self.atoms.get_forces()**2).sum(axis=1)).max())

    def get_E(self):
        return np.array(self.E)

    def get_fmax(self):
        return np.array(self.fmax)
