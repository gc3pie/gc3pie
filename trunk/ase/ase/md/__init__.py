"""Molecular Dynamics."""

import numpy as np

from ase.optimize import Dynamics
from ase.data import atomic_masses
from ase.md.logger import MDLogger

class MolecularDynamics(Dynamics):
    """Base-class for all MD classes."""
    def __init__(self, atoms, timestep, trajectory, logfile=None,
                 loginterval=1):
        Dynamics.__init__(self, atoms, logfile=None, trajectory=trajectory)
        self.dt = timestep
        self.masses = self.atoms.get_masses()
        self.masses.shape = (-1, 1)
        if logfile:
            self.attach(MDLogger(dyn=self, atoms=atoms, logfile=logfile),
                        interval=loginterval)

    def run(self, steps=50):
        """Integrate equation of motion."""
        f = self.atoms.get_forces()

        if not self.atoms.has('momenta'):
            self.atoms.set_momenta(np.zeros_like(f))

        for step in xrange(steps):
            f = self.step(f)
            self.nsteps += 1
            self.call_observers()

    def get_time(self):
        return self.nsteps * self.dt
    
