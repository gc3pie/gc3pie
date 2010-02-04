from math import sin, cos, radians, atan2, degrees

import numpy as np


class DevNull:
    def write(self, string): pass
    def flush(self): pass
    def tell(self): return 0
    def close(self): pass

devnull = DevNull()


def rotate(rotations, rotation=np.diag([1.0, -1, 1])):
    """Convert string of format '50x,-10y,120z' to a rotation matrix.

    Note that the order of rotation matters, i.e. '50x,40z' is different
    from '40z,50x'.
    """
    if rotations == '':
        return rotation
    
    for i, a in [('xyz'.index(s[-1]), radians(float(s[:-1])))
                 for s in rotations.split(',')]:
        s = sin(a)
        c = cos(a)
        if i == 0:
            rotation = np.dot(rotation, [( 1,  0,  0),
                                          ( 0,  c, -s),
                                          ( 0,  s,  c)])
        elif i == 1:
            rotation = np.dot(rotation, [( c,  0, -s),
                                          ( 0,  1,  0),
                                          ( s,  0,  c)])
        else:
            rotation = np.dot(rotation, [( c, -s,  0),
                                          ( s,  c,  0),
                                          ( 0,  0,  1)])
    return rotation


def givens(a, b):
    """Solve the equation system::

      [ c s]   [a]   [r]
      [    ] . [ ] = [ ]
      [-s c]   [b]   [0]
    """
    sgn = lambda x: cmp(x, 0)
    if b == 0:
        c = sgn(a)
        s = 0
        r = abs(a)
    elif abs(b) >= abs(a):
        cot = a / b
        u = sgn(b) * (1 + cot**2)**.5
        s = 1. / u
        c = s * cot
        r = b * u
    else:
        tan = b / a
        u = sgn(a) * (1 + tan**2)**.5
        c = 1. / u
        s = c * tan
        r = a * u
    return c, s, r


def irotate(rotation, initial=np.diag([1.0, -1, 1])):
    """Determine x, y, z rotation angles from rotation matrix."""
    a = np.dot(initial, rotation)
    cx, sx, rx = givens(a[2, 2], a[1, 2])
    cy, sy, ry = givens(rx, a[0, 2])
    cz, sz, rz = givens(cx * a[1, 1] - sx * a[2, 1],
                        cy * a[0, 1] - sy * (sx * a[1, 1] + cx * a[2, 1]))
    x = degrees(atan2(-sx, cx))
    y = degrees(atan2(-sy, cy))
    z = degrees(atan2(-sz, cz))
    return x, y, z


def hsv2rgb(h, s, v):
    """http://en.wikipedia.org/wiki/HSL_and_HSV

    h (hue) in [0, 360[
    s (saturation) in [0, 1]
    v (value) in [0, 1]

    return rgb in range [0, 1]
    """
    if v == 0:
        return 0, 0, 0
    if s == 0:
        return v, v, v

    i, f = divmod(h / 60., 1)
    p = v * (1 - s)
    q = v * (1 - s * f)
    t = v * (1 - s * (1 - f))

    if i == 0:
        return v, t, p
    elif i == 1:
        return q, v, p
    elif i == 2:
        return p, v, t
    elif i == 3:
        return p, q, v
    elif i == 4:
        return t, p, v
    elif i == 5:
        return v, p, q
    else:
        raise RuntimeError, 'h must be in [0, 360]'


def hsv(array, s=.9, v=.9):
    array = (array + array.min()) * 359. / (array.max() - array.min())
    result = np.empty((len(array.flat), 3))
    for rgb, h in zip(result, array.flat):
        rgb[:] = hsv2rgb(h, s, v)
    return np.reshape(result, array.shape + (3,))

## This code does the same, but requires pylab
## def cmap(array, name='hsv'):
##     import pylab
##     a = (array + array.min()) / array.ptp()
##     rgba = getattr(pylab.cm, name)(a)
##     return rgba[:-1] # return rgb only (not alpha) 
