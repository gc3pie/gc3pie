.. module:: atom

The Atom object
===============

ASE defines a python class called :class:`Atom` to setup and handle atoms
in electronic structure and molecular simulations. From a python
script, atoms can be created like this:

>>> from ase import Atom
>>> a1 = Atom('Si', (0, 0, 0))
>>> a2 = Atom('H', (1.3, 0, 0), mass=2)
>>> a3 = Atom(position=(0, 0, 0), Z=14)  # same is a1


.. class:: Atom(symbol='X', position=(0, 0, 0), tag=None, momentum=None, mass=None, magmom=None, charge=None, atoms=None, index=None)


The first argument to the constructor of an :class:`Atom` object is
the chemical symbol, and the second argument is the position in Å
units (see :mod:`units`).  The position can be any numerical sequence
of length three.  The properties of an atom can also be set using
keywords like it is done in the *a2* and *a3* examples above.

The different properties of an atom can generally be obtained with a
"get-method" and changed with a "set-method". For example for the position of the atom:

>>> a1.set_position([1,0,0])
>>> a1.get_position()
array([ 1.,  0.,  0.])
>>> a1.z = 2.5
>>> a1.position
array([ 1. ,  0. ,  2.5])
>>> a2.magmom = 1.0

Getting an Atom from an Atoms object
------------------------------------

Indexing an :class:`Atoms` object returns an :class:`Atom` object
still remembering that it belongs to the collective :class:`Atoms`:
Modifying it will also change the atoms object:

>>> atoms = ase.data.molecules.molecule("CH4")
>>> atoms.get_positions()
array([[ 0.      ,  0.      ,  0.      ],
       [ 0.629118,  0.629118,  0.629118],
       [-0.629118, -0.629118,  0.629118],
       [ 0.629118, -0.629118, -0.629118],
       [-0.629118,  0.629118, -0.629118]])
>>> a = atoms[2]
>>> a
Atom('H', [-0.62911799999999996, -0.62911799999999996, 0.62911799999999996], index=2)
>>> a.x = 0
>>> atoms.get_positions()
array([[ 0.      ,  0.      ,  0.      ],
       [ 0.629118,  0.629118,  0.629118],
       [ 0.      , -0.629118,  0.629118],
       [ 0.629118, -0.629118, -0.629118],
       [-0.629118,  0.629118, -0.629118]])
                                                   

.. seealso::

   :epydoc:`atom.Atom`:
     All the details!

   :mod:`atoms`:
     More information about how to use collections of atoms.

   :mod:`calculators`:
     Information about how to calculate forces and energies of atoms.

