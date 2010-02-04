.. module:: gui
   :synopsis: Simple graphical user-interface for ASE.


.. index:: gui, ag, ase-gui

=======
ASE-GUI
=======

.. image:: ag.png
   :height: 200 pt


Files
-----

The :program:`ag` program can read all the file formats the ASE's
:func:`~ase.io.read` function can understand.

.. highlight:: bash

::
  
  $ ag N2Fe110-path.traj


Selecting part of a trajectory
------------------------------
  
A Python-like syntax for selecting a subset of configurations can be
used.  Instead of the Python syntax ``list[start:stop:step]``, you use
:file:`filaname@start:stop:step`::

  $ ag x.traj@0:10:1  # first 10 images
  $ ag x.traj@0:10    # first 10 images
  $ ag x.traj@:10     # first 10 images
  $ ag x.traj@-10:    # last 10 images
  $ ag x.traj@0       # first image
  $ ag x.traj@-1      # last image
  $ ag x.traj@::2     # every second image

If you want to select the same range from many files, the you can use
the :option:`-n` or :option:`--image-number` option::

  $ ag -n -1 *.traj   # last image from all files
  $ ag -n 0 *.traj    # first image from all files

.. tip::

  Type :program:`ag -h` for a description of all command line options.

XXX latex shows --image-number as -image-number!



Writing files
-------------

::

  $ ag -n -1 a*.traj -o new.traj

Possible formats are: ``traj``, ``xyz``, ``cube``, ``pdb``, ``eps``,
``png``, and ``pov``.  For details, see the :mod:`~ase.io` module
documentation.



Plotting data
-------------

Plot the energy relative to the energy of the first image as a
function of the distance between atom 0 and 5::

  $ ag -g "d(0,5),e-E[0]" x.traj
  $ ag -t -g "d(0,5),e-E[0]" x.traj > x.dat  # No GUI, write data to stdout

These are the symbols that can be used:

==========  ================================
e           total energy
epot        potential energy
ekin        kinetic energy
fmax        maximum force
fave        average force
d(n1,n2)    distance between two atoms
R[n,0-2]    position of atom number n
i           current image number
E[i]        energy of image number i
F[n,0-2]    force on atom number n
M[n]        magnetic moment of atom number n
A[0-2,0-2]  unit-cell basis vectors 
s           path length
==========  ================================



Interactive use
---------------

The :program:`ag` program can also be launched directly from a Python
script or interactive session:

>>> from ase import *
>>> atoms = ...
>>> view(atoms)

or

>>> view(atoms, repeat=(3, 3, 2))




NEB calculations
----------------

Use :menuselection:`Tools --> NEB` to plot energy barrier.

::
  
  $ ag --interpolate 3 initial.xyz final.xyz -o interpolated_path.traj
