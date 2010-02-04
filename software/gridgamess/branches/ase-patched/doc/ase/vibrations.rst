.. module:: vibrations

Vibration analysis
------------------

You can calculate the vibrational modes of a an
:class:`~ase.atoms.Atoms` object in the harmonic approximation using
the :class:`~ase.vibration.Vibrations`.

.. autoclass:: ase.vibrations.Vibrations
   :members:

filetoken is a string that is prefixed to the names of all the files
created. atoms is a Atoms that is either at a
fully relaxed ground state or at a saddle point. freeatoms is a
list of the atoms which the vibrational modes will be calculated for,
the rest of the atoms are considered frozen. displacements is a
list of displacements, one for each free atom that are used in the
finite difference method to calculate the Hessian matrix. method is -1
for backward differences, 0 for centered differences, and 1 for
forward differences.

.. warning::
   Using the `dacapo` caculator you must make sure that the symmetry
   program in dacapo finds the same number of symmetries for the
   displaced configurations in the vibrational modules as found in
   the ground state used as input.
   This is because the wavefunction is reused from one displacement
   to the next.
   One way to ensure this is to tell dacapo not to use symmetries.

   This will show op as a python error 'Frames are not aligned'.
   This could be the case for other calculators as well.


You can get a NetCDF trajectory corresponding to a specific mode by
using:

>>> mode=0
>>> vib.create_mode_trajectory(mode=mode,scaling=5)

This will create a NetCDF trajectory file `CO_vib_mode_0.traj`,
corresponding to the highest frequency mode.
`scaling` is an option argument, that will give the amplitude of
the mode, default is 10.

The summary() method can also be used to calculated thermodyamic
properties (e.g. zero point energy, enthalpy, entropy) for a known
set of frequencies. This is done by creating an instance of Vibrations
with an empty list of atoms and supplying the frequencies as a
numpy array. For example:

>>> vib = Vibrations(atoms=[])
>>> vib.summary(T=500,freq=np.array([2800,400,300]),threshold=10)
>>> ---------------------
>>>   #    meV     cm^-1
>>> ---------------------
>>>   0  347.2    2800.0 
>>>   1   49.6     400.0 
>>>   2   37.2     300.0 
>>> ---------------------
>>> Zero-point energy: 0.217 eV
>>> Thermodynamic properties at 500.00 K
>>> Enthalpy: 0.050 eV
>>> Entropy : 0.180 meV/K
>>> T*S     : 0.090 eV
>>> E->G    : 0.177 eV

