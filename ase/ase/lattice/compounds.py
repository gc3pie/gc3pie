"""Function-like objects creating lattices with more than one element.

These lattice creators are mainly intended as examples for how to build you
own.  The following crystal structures are defined:

    B1 = NaCl = Rocksalt
    B2 = CsCl
    B3 = ZnS = Zincblende
    L1_2 = AuCu3
    L1_0 = AuCu
    
"""
from ase.lattice.cubic import FaceCenteredCubicFactory,\
    BodyCenteredCubicFactory, DiamondFactory, SimpleCubicFactory
from ase.lattice.tetragonal import SimpleTetragonalFactory
import numpy as np
from ase.data import reference_states as _refstate


# To prevent a layer of element one on one side, and a layer of
# element two on the other side, NaCl is based on SimpleCubic instead
# of on FaceCenteredCubic
class NaClFactory(SimpleCubicFactory):
    "A factory for creating NaCl (B1, Rocksalt) lattices."

    bravais_basis = [[0, 0, 0], [0, 0, 0.5], [0, 0.5, 0], [0, 0.5, 0.5],
                     [0.5, 0, 0], [0.5, 0, 0.5], [0.5, 0.5, 0],
                     [0.5, 0.5, 0.5]]
    element_basis = (0, 1, 1, 0, 1, 0, 0, 1)
    

B1 = NaCl = Rocksalt = NaClFactory()

class CsClFactory(SimpleCubicFactory):
    "A factory for creating CsCl (B2) lattices."
    bravais_basis = [[0, 0, 0], [0.5, 0.5, 0.5]]
    element_basis = (0, 1)

B2 = CsCl = CsClFactory()


#The zincblende structure is easily derived from Diamond, which
#already has the right basis.
class ZnSFactory(DiamondFactory):
    "A factory for creating ZnS (B3, Zincblende) lattices."
    element_basis = (0, 1)

B3 = ZnS = Zincblende = ZnSFactory()


# The L1_0 structure is "based on FCC", but is a tetragonal distortion
# of fcc.  It must therefore be derived from the base-centered
# tetragonal structure.  That structure, however, does not exist,
# since it is equivalent to a simple tetragonal structure rotated 45
# degrees along the z-axis.  Basing L1_2 on that would however give
# unexpected miller indices.  L1_2 will therefore be based on a simple
# tetragonal structure, but with a basis corresponding to a
# base-centered tetragonal.
class AuCuFactory(SimpleTetragonalFactory):
    "A factory for creating AuCu (L1_0) lattices (tetragonal symmetry)."
    bravais_basis = [[0, 0, 0], [0, 0.5, 0.5], [0.5, 0, 0.5], [0.5, 0.5, 0]]
    element_basis = (0, 1, 1, 0)

AuCu = L1_0 = AuCuFactory()

# The L1_2 structure is "based on FCC", but is really simple cubic
# with a basis.
class AuCu3Factory(SimpleCubicFactory):
    "A factory for creating AuCu3 (L1_2) lattices."
    bravais_basis = [[0, 0, 0], [0, 0.5, 0.5], [0.5, 0, 0.5], [0.5, 0.5, 0]]
    element_basis = (0, 1, 1, 1)

AuCu3 = L1_2 = AuCu3Factory()
