.. module:: abinit

======
ABINIT
======

Introduction
============

ABINIT_ is a density-functional theory code
based on pseudopotentials and a planewave basis.


.. _ABINIT: http://www.abinit.org



Environment variables
=====================

You need to write a script called :file:`abinit.py` containing
something like this::

  import os
  abinit = '/usr/bin/abinis'
  exitcode = os.system('%s < %s.files > %s.log' % (abinit, label, label))

The environment variable :envvar:`ABINIT_SCRIPT` must point to that file.

A directory containing the pseudopotential files :file:`.fhi` is also
needed, and it is to be put in the environment variable
:envvar:`ABINIT_PP_PATH`.

Set both environment variables in your in your shell configuration file:

.. highlight:: bash
 
::

  $ export ABINIT_SCRIPT=$HOME/bin/abinit.py
  $ export ABINIT_PP_PATH=$HOME/mypps

.. highlight:: python



ABINIT Calculator
================= 

The default parameters are very close to those that the ABINIT Fortran
code uses.  These are the exceptions:

.. class:: Abinit(label='abinit', xc='LDA', pulay=5, mix=0.1)
    
Here is a detailed list of all the keywords for the calculator:

============== ========= ================  =====================================
keyword        type      default value     description
============== ========= ================  =====================================
``kpts``       ``list``  ``[1,1,1]``       Monkhorst-Pack k-point sampling
``nbands``     ``int``   ``0``             Number of bands (default: 0)
``ecut``       ``float`` ``None``          Planewave cutoff energy in eV (default: None)
``xc``         ``str``   ``'LDA'``         Exchange-correlation functional.
``pulay``      ``int``   ``5``             Number of old densities to use for
                                           Pulay mixing
``mix``        ``float`` ``0.1``           Pulay mixing weight 
``width``      ``float`` ``0.04 Ha``       Fermi-distribution width in eV (default: 0.04 Ha)
``charge``     ``float`` ``0``             Total charge of the system (default: 0)
``label``      ``str``   ``'abinit'``      Name of the output file
``toldfe``     ``float`` ``1.0e-6``        TOLerance on the DiFference of total Energy
============== ========= ================  =====================================

A value of ``None`` means that ABINIT's default value is used.

**Warning**: abinit does not specify a default value for
``Number of bands`` nor ``Planewave cutoff energy in eV`` - you need to set them as in the example at thei bottom of the page, otherwise calculation will fail.

**Warning**: calculations wihout k-points are not parallelized by default
and will fail! To enable band paralellization specify ``Number of BanDs in a BLOCK`` 
(``nbdblock``) as `Extra parameters`_ -
see `<http://www.abinit.org/Infos_v5.2/tutorial/lesson_parallelism.html>`_.

Extra parameters
================

The ABINIT code reads the input parameters for any calculation from a 
:file:`.in` file and :file:`.files` file.
This means that you can set parameters by manually setting 
entries in this input :file:`.in` file. This is done by the syntax:

>>> calc.set_inp('name_of_the_entry', value)

For example, the ``nstep`` can be set using

>>> calc.set_inp('nstep', 30)

The complete list of keywords can be found in the official `ABINIT
manual`_.

.. _ABINIT manual: http://www.abinit.org/Infos_v5.4/input_variables/keyhr.html



Pseudopotentials
================

Pseudopotentials in the ABINIT format are available on the
`pseudopotentials`_ website.
A database of user contributed pseudopotentials is also available there.

.. _pseudopotentials: http://www.abinit.org/Psps/?text=psps



Example 1
=========

Here is an example of how to calculate the total energy for bulk Silicon::
        
  #!/usr/bin/env python
  from ase import *
  from ase.calculators.abinit import Abinit
  
  a0 = 5.43
  bulk = Atoms('Si2', [(0, 0, 0),
                       (0.25, 0.25, 0.25)],
               pbc=True)
  b = a0 / 2
  bulk.set_cell([(0, b, b),
                 (b, 0, b),
                 (b, b, 0)], scale_atoms=True)
  
  calc = Abinit(label='Si',
                nbands=8, 
                xc='PBE',
                ecut=50 * Ry,
                mix=0.01,
                kpts=[10, 10, 10])
   
  bulk.set_calculator(calc)
  e = bulk.get_potential_energy()

Example 2
=========

Here is an example of how to calculate band structure of bulk Na (compare the same example
in gpaw `<https://wiki.fysik.dtu.dk/gpaw/exercises/band_structure/bands.html>`_)::

  #!/usr/bin/env python

  import numpy as np
  from ase.calculators.abinit import Abinit
  from ase import Atoms, Ry

  a = 4.23
  atoms = Atoms('Na2', cell=(a, a, a), pbc=True,
                scaled_positions=[[0, 0, 0], [.5, .5, .5]])

  nbands = 3
  label = 'Na_sc'
  # Make self-consistent calculation and save results
  calc = Abinit(label=label,
                nbands=nbands,
                xc='PBE',
                ecut=70 * Ry,
                width=0.05,
                kpts=[8, 8, 8])

  # parameters for calculation of band structure
  # see http://www.abinit.org/Infos_v5.6/tutorial/lesson_3.html#35

  calc.set_inp('ndtset', 2) # two datasets are used
  calc.set_inp('iscf2', -2) # make a non-self-consistent calculation ;
  calc.set_inp('getden2', -1) # to take the output density of dataset 1
  calc.set_inp('kptopt2', -1) # to define one segment in the brillouin Zone
  nband2 = 7
  calc.set_inp('nband2', nband2) # use 7 bands in band structure calculation
  calc.set_inp('ndivk2', 50) # with 51 divisions of the first segment
  calc.set_inp('kptbounds2', "\n0.5  0.0  0.0\n0.0  0.0  0.0\n0.0  0.5  0.5\n1.0  1.0  1.0\n")
  calc.set_inp('tolwfr2', 1.0e-12) #
  calc.set_inp('enunit2', 1) # in order to have eigenenergies in eV (in the second dataset)

  atoms.set_calculator(calc)
  atoms.get_potential_energy()

  # Subtract Fermi level from the self-consistent calculation
  e_fermi = calc.get_fermi_level()
  assert nbands == calc.get_number_of_bands()

  # Calculate band structure along Gamma-X i.e. from 0 to 0.5

  kpts2 = calc.get_ibz_k_points()
  nkpts2 = len(kpts2)

  eigs = np.empty((nband2, nkpts2), float)

  for k in range(nkpts2):
      eigs[:, k] = calc.get_eigenvalues(kpt=k)

  def plot_save(directory_name, out_prefix):
      from os.path import exists, sep
      assert exists(directory_name)
      import matplotlib
      matplotlib.use('Agg')
      from matplotlib import pylab

      pylab.savefig(directory_name + sep + out_prefix +'.png')

  import matplotlib
  matplotlib.use('Agg')
  from matplotlib import pylab

  eigs -= e_fermi
  for n in range(nband2):
      pylab.plot(kpts2[:, 0], eigs[n], '.m')
  plot_save(".", label)

