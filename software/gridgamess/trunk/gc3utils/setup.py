#!/usr/bin/env python

from distutils.core import setup

setup(name='gc3utils',
      version='0.1',
      description='Python GC3 Utilities',
      author='Grid Computing Competency Center',
      author_email='kbadmin@oci.uzh.ch',
      url='http://www.gc3.uzh.ch/index.html',
      packages=['gc3utils'],
      requires=['ase>=1.0',elixir>=1.0],
     )

