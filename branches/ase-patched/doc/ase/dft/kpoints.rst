.. module:: dft.kpoints
   :synopsis: Brillouin zone sampling

=======================
Brillouin zone sampling
=======================

The **k**-points are always given relative to the basis vectors of the
reciprocal unit cell.


Monkhorst-Pack
--------------

.. function:: dft.kpoints.monkhorst_pack

Example:

>>> from ase.dft.kpoints import monkhorst_pack
>>> monkhorst_pack((4, 1, 1))
array([[-0.375,  0.   ,  0.   ],
       [-0.125,  0.   ,  0.   ],
       [ 0.125,  0.   ,  0.   ],
       [ 0.375,  0.   ,  0.   ]])


Chadi-Cohen
-----------

Predefined sets of **k**-points:

.. data:: dft.kpoints.cc6_1x1
.. data:: dft.kpoints.cc12_2x3
.. data:: dft.kpoints.cc18_sq3xsq3
.. data:: dft.kpoints.cc18_1x1
.. data:: dft.kpoints.cc54_sq3xsq3
.. data:: dft.kpoints.cc54_1x1
.. data:: dft.kpoints.cc162_sq3xsq3
.. data:: dft.kpoints.cc162_1x1


Naming convention: ``cc18_sq3xsq3`` is 18 **k**-points for a
sq(3)xsq(3) cell.

Try this:

>>> import numpy as np
>>> import pylab as plt
>>> from ase.dft.kpoints import cc162_1x1
>>> B = [(1, 0, 0), (-0.5, 3**0.5 / 2, 0), (0, 0, 1)]
>>> k = np.dot(cc162_1x1, B)
>>> plt.plot(k[:, 0], k[:, 1], 'o')
[<matplotlib.lines.Line2D object at 0x9b61dcc>]
>>> p.show()

.. image:: cc.png
