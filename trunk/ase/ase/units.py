from math import pi, sqrt

# Constants from Konrad Hinsen's PhysicalQuantities module:
_c = 299792458.              # speed of light
_mu0 = 4.e-7 * pi            # permeability of vacuum
_eps0 = 1 / _mu0 / _c**2     # permittivity of vacuum
_Grav = 6.67259e-11          # gravitational constant
_hplanck = 6.6260755e-34     # Planck constant
_hbar = _hplanck / (2 * pi)  # Planck constant / 2pi
_e = 1.60217733e-19          # elementary charge
_me = 9.1093897e-31          # electron mass
_mp = 1.6726231e-27          # proton mass
_Nav = 6.0221367e23          # Avogadro number
_k = 1.380658e-23            # Boltzmann constant
_amu = 1.6605402e-27 

Ang = Angstrom = 1.0
nm = 0.1
Bohr = 4e10 * pi * _eps0 * _hbar**2 / _me / _e**2  # Bohr radius

eV = 1.0
Hartree = _me * _e**3 / 16 / pi**2 / _eps0**2 / _hbar**2
kJ = 1000.0 / _e
kcal = 4.184 * kJ
mol = _Nav
Rydberg = 0.5 * Hartree
Ry = Rydberg
Ha = Hartree

second = 1e10 * sqrt(_e / _amu)
fs = 1e-15 * second

kB = _k / _e

Pascal = (1 / _e) / 1e30  # J/m^3
GPa = 1e9 * Pascal

Debye = 1e11 *_e * _c
