.. module:: constraints
   :synopsis: Constraining some degrees of freedom

===========
Constraints
===========


When performing minimizations or dynamics one may wish to keep some
degrees of freedom in the system fixed. One way of doing this is by
attaching constraint object(s) directly to the atoms object.


The FixAtoms class
==================

This class is used for fixing some of the atoms.

.. class:: FixAtoms(indices=None, mask=None)



**XXX positive or negative mask???**



You must supply either the indices of the atoms that should be fixed
or a mask. The mask is a list of booleans, one for each atom, being true
if the atoms should be kept fixed.

Example of use:

>>> c = FixAtoms(mask=[a.symbol == 'Cu' for a in atoms])
>>> atoms.set_constraint(c)

This will fix the positions of all the Cu atoms in a
simulation.


The FixBondLength class
=======================

This class is used to fix the distance between two atoms specified by
their indices (*a1* and *a2*)

.. class:: FixBondLength(a1, a2)

Example of use::

  >>> c = FixBondLength(0, 1)
  >>> atoms.set_constraint(c)

In this example the distance between the atoms
with indices 0 and 1 will be fixed in all following dynamics and/or
minimizations performed on the *atoms* object.

This constraint is useful for finding minimum energy barriers for
reactions where the path can be described well by a single bond
length (see the :ref:`mep2` tutorial).



The FixBondLengths class
========================

More than one bond length can be fixed by using this class. Especially
for cases in which more than one bond length constraint is applied on 
the same atom. It is done by specifying the indices of the two atoms 
forming the bond in pairs.

.. class:: FixBondLengths(pairs)

Example of use::

  >>> c = FixBondLengths([[0, 1], [0, 2]])
  >>> atoms.set_constraint(c)

Here the distances between atoms with indices 0 and 1 and atoms with 
indices 0 and 2 will be fixed. The constraint is for the same purpose 
as the FixBondLength class. 

The FixedPlane class
====================

.. autoclass:: ase.constraints.FixedPlane

Example of use: :ref:`constraints_diffusion_tutorial`.



Combining constraints
=====================

It is possible to supply several constraints on an atoms object. For
example one may wish to keep the distance between two nitrogen atoms
fixed while relaxing it on a fixed ruthenium surface::

  >>> pos = [[0.00000, 0.00000,  9.17625],
  ...        [0.00000, 0.00000, 10.27625],
  ...        [1.37715, 0.79510,  5.00000],
  ...        [0.00000, 3.18039,  5.00000],
  ...        [0.00000, 0.00000,  7.17625],
  ...        [1.37715, 2.38529,  7.17625]]
  >>> unitcell = [5.5086, 4.7706, 15.27625]

  >>> atoms = Atoms(positions=pos,
  ...               symbols='N2Ru4',
  ...               cell=unitcell,
  ...               pbc=[True,True,False])

  >>> fa = FixAtoms(mask=[a.symbol == 'Ru' for a in atoms])
  >>> fb = FixBondLength(0, 1)
  >>> atoms.set_constraint([fa, fb])

When applying more than one constraint they are passed as a list in
the :meth:`set_constraint` method, and they will be applied one after
the other.



Making your own constraint class
================================

A constraint class must have these two methods:

.. method:: adjust_positions(oldpositions, newpositions)

   Adjust the *newpositions* array inplace.

.. method:: adjust_forces(positions, forces)

   Adjust the *forces* array inplace.


A simple example::

  import numpy as np
  class MyConstraint:
      """Constrain an atom to move along a given direction only."""
      def __init__(self, a, direction):
          self.a = a
          self.dir = direction / sqrt(np.dot(direction, direction))
  
      def adjust_positions(self, oldpositions, newpositions):
          step = newpositions[self.a] - oldpositions[self.a]
          step = np.dot(step, self.dir)
          newpositions[self.a] = oldpositions[self.a] + step * self.dir
  
      def adjust_forces(self, positions, forces):
          forces[self.a] = self.dir * np.dot(forces[self.a], self.dir)




The Filter class
================

Constraints can also be applied via filters, which acts as a wrapper
around an atoms object. A typical use case will look like this::

   -------       --------       ----------
  |       |     |        |     |          |
  | Atoms |<----| Filter |<----| Dynamics |
  |       |     |        |     |          |
   -------       --------       ----------

and in Python this would be::

  >>> atoms = Atoms(...)
  >>> filter = Filter(atoms, ...)
  >>> dyn = Dynamics(filter, ...)


This class hides some of the atoms in an Atoms object.

.. class:: Filter(atoms, indices=None, mask=None)

You must supply either the indices of the atoms that should be kept
visible or a mask. The mask is a list of booleans, one for each atom,
being true if the atom should be kept visible.

Example of use::

  >>> from ase import Atoms, Filter
  >>> atoms=Atoms(positions=[[ 0    , 0    , 0],
  ...                        [ 0.773, 0.600, 0],
  ...                        [-0.773, 0.600, 0]],
  ...             symbols='OH2')
  >>> f1 = Filter(atoms, indices=[1, 2])
  >>> f2 = Filter(atoms, mask=[0, 1, 1])
  >>> f3 = Filter(atoms, mask=[a.Z == 1 for a in atoms])
  >>> f1.get_positions()
  [[ 0.773  0.6    0.   ]
   [-0.773  0.6    0.   ]]

In all three filters only the hydrogen atoms are made
visible.  When asking for the positions only the positions of the
hydrogen atoms are returned.

