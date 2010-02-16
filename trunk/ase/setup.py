#!/usr/bin/env python

# Copyright (C) 2007  CAMP
# Please see the accompanying LICENSE file for further information.

from distutils.core import setup
from glob import glob
from os.path import join

import os
import sys

long_description = """\
ASE is a python package providing an open source Atomic Simulation
Environment in the python scripting language."""


if sys.version_info < (2, 3, 0, 'final', 0):
    raise SystemExit, 'Python 2.3 or later is required!'

packages = ['ase',
            'ase.io',
            'ase.md',
            'ase.dft',
            'ase.gui',
            'ase.data',
            'ase.test',
            'ase.utils',
            'ase.lattice',
            'ase.examples',
            'ase.optimize',
            'ase.visualize',
            'ase.visualize.vtk',
            'ase.transport',
            'ase.calculators',
            'ase.gui.languages',
            'ase.calculators.jacapo']

# Get the current version number:
execfile('ase/version.py')

setup(name = 'python-ase',
      version=version,
      description='Atomic Simulation Environment',
      url='https://wiki.fysik.dtu.dk/ase',
      maintainer='CAMd',
      maintainer_email='camd@fysik.dtu.dk',
      license='LGPL',
      platforms=['linux'],
      packages=packages,
      scripts=['tools/ag', 'tools/ASE2ase.py', 'tools/testase.py'],
      long_description=long_description)
