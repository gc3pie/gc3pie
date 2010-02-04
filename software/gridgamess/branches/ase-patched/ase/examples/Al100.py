from gpaw import *
from ASE import *
a = 4.0
b = a / 2**.5
L = 7.0
al = ListOfAtoms([Atom('Al')], cell=(b, b, L), periodic=True)
calc = Calculator(kpts=(4, 4, 1))
al.SetCalculator(calc)
al.GetPotentialEnergy()
calc.write('Al100.gpw', 'all')
