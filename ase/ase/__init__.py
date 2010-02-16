# Copyright 2008, 2009 CAMd
# (see accompanying license files for details).

"""Atomic Simulation Environment."""

from ase.utils import memory
from ase.atom import Atom
from ase.atoms import Atoms
from ase.units import *
from ase.io import read, write
from ase.io.trajectory import PickleTrajectory
from ase.dft import STM, monkhorst_pack, DOS
from ase.optimize.mdmin import MDMin
from ase.optimize.lbfgs import HessLBFGS
from ase.optimize.lbfgs import LineLBFGS
from ase.optimize.fire import FIRE
from ase.optimize.lbfgs import LBFGS, LineSearchLBFGS
from ase.optimize.bfgs import BFGS
from ase.md.verlet import VelocityVerlet
from ase.md.langevin import Langevin
from ase.constraints import *
from ase.calculators import LennardJones, EMT, ASAP, Siesta, Dacapo, \
     Vasp, Aims, AimsCube, Turbomole
from ase.neb import NEB, SingleCalculatorNEB
from ase.visualize import view
from ase.data import chemical_symbols, atomic_numbers, atomic_names, \
     atomic_masses, covalent_radii, reference_states
from ase.data.molecules import molecule

from math import sqrt, pi
import numpy as np
#import scipy as sp
#import matplotlib.pyplot as plt

QuasiNewton = BFGS
