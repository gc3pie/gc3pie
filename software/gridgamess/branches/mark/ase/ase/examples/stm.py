from ase import *
from gpaw import *
a0 = Calculator.ReadAtoms('Al100.gpw')
a1 = Atoms(a0, calculator=a0.calculator)
stm = STM(a1, [0, 1, 2])
c = stm.averaged_current(2.5)
h = stm.scan(c)
print h[8]-h[:, 8]


