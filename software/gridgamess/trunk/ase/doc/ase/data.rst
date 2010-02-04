.. module:: data

===============
The data module
===============


Atomic data
===========

This module defines the following variables:

.. data:: atomic_masses
.. data:: atomic_names
.. data:: chemical_symbols
.. data:: covalent_radii
.. data:: cpk_colors
.. data:: reference_states

All of these are lists that should be indexed with an atomic number:

>>> from ase import *
>>> atomic_names[92]
'Uranium'
>>> atomic_masses[2]
4.0026000000000002


.. data:: atomic_numbers

If you don't know the atomic number of some element, then you can look
it up in the :data:`atomic_numbers` dictionary:

>>> atomic_numbers['Cu']
29
>>> covalent_radii[29]
1.1699999999999999



Molecular data
==============

The G2-database is available in the :mod:`molecules` module.

Example::

>>> from ase.data.molecules import molecule
>>> atoms = molecule('H2O')
