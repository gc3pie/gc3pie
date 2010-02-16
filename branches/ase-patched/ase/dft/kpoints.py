import numpy as np


def monkhorst_pack(size):
    """Construct a uniform sampling of k-space of given size."""
    if np.less_equal(size, 0).any():
        raise ValueError('Illegal size: %s' % list(size))
    kpts = np.indices(size).transpose((1, 2, 3, 0)).reshape((-1, 3))
    return (kpts + 0.5) / size - 0.5


def get_monkhorst_shape(kpts, tol=1e-5):
    """Return the number of k-points along each axis of input Monkhorst pack.

    The set of k-points must not have been symmetry reduced.
    """
    nkpts = len(kpts)
    if nkpts == 1:
        return np.ones(3, int)
    
    Nk_c = np.zeros(3, int)
    for c in range(3):
        # Determine increment between kpoints along current axis
        DeltaK = max(np.diff(np.sort(kpts[:, c])))

        # Determine number of kpoints as inverse of distance between kpoints
        if DeltaK > tol:
            Nk_c[c] = int(round(1. / DeltaK))
        else:
            Nk_c[c] = 1
    return Nk_c


def kpoint_convert(cell_cv, skpts_kc=None, ckpts_kv=None):
    """Convert k-points between scaled and cartesian coordinates.

    Given the atomic unit cell, and either the scaled or cartesian k-point
    coordinates, the other is determined.

    The k-point arrays can be either a single point, or a list of points,
    i.e. the dimension k can be empty or multidimensional.
    """
    if ckpts_kv is None:
        icell_cv = 2 * np.pi * np.linalg.inv(cell_cv).T
        return np.dot(skpts_kc, icell_cv)
    elif skpts_kc is None:
        return np.dot(ckpts_kv, cell_cv.T) / (2 * np.pi)
    else:
        raise KeyError('Either scaled or cartesian coordinates must be given.')


def get_bandpath(points, cell, npoints=50):
    """Make a list of kpoints defining the path between the given points.

    points is a list of special IBZ point pairs, e.g.
    >>> points = [L, Gamma, Gamma, X, X, U, K, Gamma]
    These should be given in scaled coordinates.

    cell is the unitcell if the atoms.

    npoints is the approximate desired length of the output kpts list

    The output list point_indices, gives the indices in kpts of the special
    points.
    """
    assert len(points) % 2 == 0
    points = np.asarray(points)
    dists = points[1::2] - points[::2]
    lengths = [np.linalg.norm(d) for d in kpoint_convert(cell, skpts_kc=dists)]
    length = sum(lengths)
    kpts = []
    point_indices = [0]
    for P, d, L in zip(points[::2], dists, lengths):
        for t in np.linspace(0, 1, int(round(L / length * npoints)),
                             endpoint=False):
            kpts.append(P + t * d)
        point_indices.append(len(kpts))
    return np.array(kpts), point_indices


# The following is a list of the critical points in the 1. Brillouin zone
# for some typical crystal structures.
# (In units of the reciprocal basis vectors)
ibz_points = {'cubic': {'Gamma': [0/1., 0/1., 0/1.],
                        'X':     [0/1., 0/2., 1/2.],
                        'R':     [1/2., 1/2., 1/2.],
                        'M':     [0/2., 1/2., 1/2.]},
              'fcc':   {'Gamma': [0/1., 0/1., 0/1.],
                        'X':     [0/1., 1/2., 1/2.],
                        'W':     [1/2., 3/4., 1/4.],
                        'K':     [3/8., 3/8., 3/4.],
                        'U':     [1/4., 5/8., 5/8.],
                        'L':     [1/2., 1/2., 1/2.]},
              'bcc':   {'Gamma': [0/1., 0/1., 0/1.],
                        'H': [1/2., -1/2., 1/2.],
                        'N': [0/1., 0/1., 1/2.],
                        'P': [1/4., 1/4., 1/4.]},

              }

# ChadiCohen k point grids. The k point grids are given in units of the
# reciprocal unit cell. The variables are named after the following
# convention: cc+'<Nkpoints>'+_+'shape'. For example an 18 k point
# sq(3)xsq(3) is named 'cc18_sq3xsq3'.

cc6_1x1 = np.array([1, 1, 0, 1, 0, 0, 0, -1, 0, -1, -1, 0, -1, 0, 0,
    0, 1, 0] ).reshape((6, 3)) / 3.0

cc12_2x3 = np.array([3, 4, 0, 3, 10, 0, 6, 8, 0, 3, -2, 0, 6, -4, 0,
    6, 2, 0, -3, 8, 0, -3, 2, 0, -3, -4, 0, -6, 4, 0, -6, -2, 0, -6,
    -8, 0] ).reshape((12, 3)) / 18.0

cc18_sq3xsq3 = np.array([2, 2, 0, 4, 4, 0, 8, 2, 0, 4, -2, 0, 8, -4,
    0, 10, -2, 0, 10, -8, 0, 8, -10, 0, 2, -10, 0, 4, -8, 0, -2, -8,
    0, 2, -4, 0, -4, -4, 0, -2, -2, 0, -4, 2, 0, -2, 4, 0, -8, 4, 0,
    -4, 8, 0] ).reshape((18, 3)) / 18.0

cc18_1x1 = np.array([2, 4, 0, 2, 10, 0, 4, 8, 0, 8, 4, 0, 8, 10, 0,
    10, 8, 0, 2, -2, 0, 4, -4, 0, 4, 2, 0, -2, 8, 0, -2, 2, 0, -2, -4,
    0, -4, 4, 0, -4, -2, 0, -4, -8, 0, -8, 2, 0, -8, -4, 0, -10, -2,
    0] ).reshape((18, 3)) / 18.0

cc54_sq3xsq3 = np.array([4, -10, 0, 6, -10, 0, 0, -8, 0, 2, -8, 0, 6,
    -8, 0, 8, -8, 0, -4, -6, 0, -2, -6, 0, 2, -6, 0, 4, -6, 0, 8, -6,
    0, 10, -6, 0, -6, -4, 0, -2, -4, 0, 0, -4, 0, 4, -4, 0, 6, -4, 0,
    10, -4, 0, -6, -2, 0, -4, -2, 0, 0, -2, 0, 2, -2, 0, 6, -2, 0, 8,
    -2, 0, -8, 0, 0, -4, 0, 0, -2, 0, 0, 2, 0, 0, 4, 0, 0, 8, 0, 0,
    -8, 2, 0, -6, 2, 0, -2, 2, 0, 0, 2, 0, 4, 2, 0, 6, 2, 0, -10, 4,
    0, -6, 4, 0, -4, 4, 0, 0, 4, 0, 2, 4, 0, 6, 4, 0, -10, 6, 0, -8,
    6, 0, -4, 6, 0, -2, 6, 0, 2, 6, 0, 4, 6, 0, -8, 8, 0, -6, 8, 0,
    -2, 8, 0, 0, 8, 0, -6, 10, 0, -4, 10, 0]).reshape((54, 3)) / 18.0

cc54_1x1 = np.array([2, 2, 0, 4, 4, 0, 8, 8, 0, 6, 8, 0, 4, 6, 0, 6,
    10, 0, 4, 10, 0, 2, 6, 0, 2, 8, 0, 0, 2, 0, 0, 4, 0, 0, 8, 0, -2,
    6, 0, -2, 4, 0, -4, 6, 0, -6, 4, 0, -4, 2, 0, -6, 2, 0, -2, 0, 0,
    -4, 0, 0, -8, 0, 0, -8, -2, 0, -6, -2, 0, -10, -4, 0, -10, -6, 0,
    -6, -4, 0, -8, -6, 0, -2, -2, 0, -4, -4, 0, -8, -8, 0, 4, -2, 0,
    6, -2, 0, 6, -4, 0, 2, 0, 0, 4, 0, 0, 6, 2, 0, 6, 4, 0, 8, 6, 0,
    8, 0, 0, 8, 2, 0, 10, 4, 0, 10, 6, 0, 2, -4, 0, 2, -6, 0, 4, -6,
    0, 0, -2, 0, 0, -4, 0, -2, -6, 0, -4, -6, 0, -6, -8, 0, 0, -8, 0,
    -2, -8, 0, -4, -10, 0, -6, -10, 0] ).reshape((54, 3)) / 18.0

cc162_sq3xsq3 = np.array([-8, 16, 0, -10, 14, 0, -7, 14, 0, -4, 14,
    0, -11, 13, 0, -8, 13, 0, -5, 13, 0, -2, 13, 0, -13, 11, 0, -10,
    11, 0, -7, 11, 0, -4, 11, 0, -1, 11, 0, 2, 11, 0, -14, 10, 0, -11,
    10, 0, -8, 10, 0, -5, 10, 0, -2, 10, 0, 1, 10, 0, 4, 10, 0, -16,
    8, 0, -13, 8, 0, -10, 8, 0, -7, 8, 0, -4, 8, 0, -1, 8, 0, 2, 8, 0,
    5, 8, 0, 8, 8, 0, -14, 7, 0, -11, 7, 0, -8, 7, 0, -5, 7, 0, -2, 7,
    0, 1, 7, 0, 4, 7, 0, 7, 7, 0, 10, 7, 0, -13, 5, 0, -10, 5, 0, -7,
    5, 0, -4, 5, 0, -1, 5, 0, 2, 5, 0, 5, 5, 0, 8, 5, 0, 11, 5, 0,
    -14, 4, 0, -11, 4, 0, -8, 4, 0, -5, 4, 0, -2, 4, 0, 1, 4, 0, 4, 4,
    0, 7, 4, 0, 10, 4, 0, -13, 2, 0, -10, 2, 0, -7, 2, 0, -4, 2, 0,
    -1, 2, 0, 2, 2, 0, 5, 2, 0, 8, 2, 0, 11, 2, 0, -11, 1, 0, -8, 1,
    0, -5, 1, 0, -2, 1, 0, 1, 1, 0, 4, 1, 0, 7, 1, 0, 10, 1, 0, 13, 1,
    0, -10, -1, 0, -7, -1, 0, -4, -1, 0, -1, -1, 0, 2, -1, 0, 5, -1,
    0, 8, -1, 0, 11, -1, 0, 14, -1, 0, -11, -2, 0, -8, -2, 0, -5, -2,
    0, -2, -2, 0, 1, -2, 0, 4, -2, 0, 7, -2, 0, 10, -2, 0, 13, -2, 0,
    -10, -4, 0, -7, -4, 0, -4, -4, 0, -1, -4, 0, 2, -4, 0, 5, -4, 0,
    8, -4, 0, 11, -4, 0, 14, -4, 0, -8, -5, 0, -5, -5, 0, -2, -5, 0,
    1, -5, 0, 4, -5, 0, 7, -5, 0, 10, -5, 0, 13, -5, 0, 16, -5, 0, -7,
    -7, 0, -4, -7, 0, -1, -7, 0, 2, -7, 0, 5, -7, 0, 8, -7, 0, 11, -7,
    0, 14, -7, 0, 17, -7, 0, -8, -8, 0, -5, -8, 0, -2, -8, 0, 1, -8,
    0, 4, -8, 0, 7, -8, 0, 10, -8, 0, 13, -8, 0, 16, -8, 0, -7, -10,
    0, -4, -10, 0, -1, -10, 0, 2, -10, 0, 5, -10, 0, 8, -10, 0, 11,
    -10, 0, 14, -10, 0, 17, -10, 0, -5, -11, 0, -2, -11, 0, 1, -11, 0,
    4, -11, 0, 7, -11, 0, 10, -11, 0, 13, -11, 0, 16, -11, 0, -1, -13,
    0, 2, -13, 0, 5, -13, 0, 8, -13, 0, 11, -13, 0, 14, -13, 0, 1,
    -14, 0, 4, -14, 0, 7, -14, 0, 10, -14, 0, 13, -14, 0, 5, -16, 0,
    8, -16, 0, 11, -16, 0, 7, -17, 0, 10, -17, 0]).reshape((162, 3)) / 27.0

cc162_1x1 = np.array([-8, -16, 0, -10, -14, 0, -7, -14, 0, -4, -14,
    0, -11, -13, 0, -8, -13, 0, -5, -13, 0, -2, -13, 0, -13, -11, 0,
    -10, -11, 0, -7, -11, 0, -4, -11, 0, -1, -11, 0, 2, -11, 0, -14,
    -10, 0, -11, -10, 0, -8, -10, 0, -5, -10, 0, -2, -10, 0, 1, -10,
    0, 4, -10, 0, -16, -8, 0, -13, -8, 0, -10, -8, 0, -7, -8, 0, -4,
    -8, 0, -1, -8, 0, 2, -8, 0, 5, -8, 0, 8, -8, 0, -14, -7, 0, -11,
    -7, 0, -8, -7, 0, -5, -7, 0, -2, -7, 0, 1, -7, 0, 4, -7, 0, 7, -7,
    0, 10, -7, 0, -13, -5, 0, -10, -5, 0, -7, -5, 0, -4, -5, 0, -1,
    -5, 0, 2, -5, 0, 5, -5, 0, 8, -5, 0, 11, -5, 0, -14, -4, 0, -11,
    -4, 0, -8, -4, 0, -5, -4, 0, -2, -4, 0, 1, -4, 0, 4, -4, 0, 7, -4,
    0, 10, -4, 0, -13, -2, 0, -10, -2, 0, -7, -2, 0, -4, -2, 0, -1,
    -2, 0, 2, -2, 0, 5, -2, 0, 8, -2, 0, 11, -2, 0, -11, -1, 0, -8,
    -1, 0, -5, -1, 0, -2, -1, 0, 1, -1, 0, 4, -1, 0, 7, -1, 0, 10, -1,
    0, 13, -1, 0, -10, 1, 0, -7, 1, 0, -4, 1, 0, -1, 1, 0, 2, 1, 0, 5,
    1, 0, 8, 1, 0, 11, 1, 0, 14, 1, 0, -11, 2, 0, -8, 2, 0, -5, 2, 0,
    -2, 2, 0, 1, 2, 0, 4, 2, 0, 7, 2, 0, 10, 2, 0, 13, 2, 0, -10, 4,
    0, -7, 4, 0, -4, 4, 0, -1, 4, 0, 2, 4, 0, 5, 4, 0, 8, 4, 0, 11, 4,
    0, 14, 4, 0, -8, 5, 0, -5, 5, 0, -2, 5, 0, 1, 5, 0, 4, 5, 0, 7, 5,
    0, 10, 5, 0, 13, 5, 0, 16, 5, 0, -7, 7, 0, -4, 7, 0, -1, 7, 0, 2,
    7, 0, 5, 7, 0, 8, 7, 0, 11, 7, 0, 14, 7, 0, 17, 7, 0, -8, 8, 0,
    -5, 8, 0, -2, 8, 0, 1, 8, 0, 4, 8, 0, 7, 8, 0, 10, 8, 0, 13, 8, 0,
    16, 8, 0, -7, 10, 0, -4, 10, 0, -1, 10, 0, 2, 10, 0, 5, 10, 0, 8,
    10, 0, 11, 10, 0, 14, 10, 0, 17, 10, 0, -5, 11, 0, -2, 11, 0, 1,
    11, 0, 4, 11, 0, 7, 11, 0, 10, 11, 0, 13, 11, 0, 16, 11, 0, -1,
    13, 0, 2, 13, 0, 5, 13, 0, 8, 13, 0, 11, 13, 0, 14, 13, 0, 1, 14,
    0, 4, 14, 0, 7, 14, 0, 10, 14, 0, 13, 14, 0, 5, 16, 0, 8, 16, 0,
    11, 16, 0, 7, 17, 0, 10, 17, 0]).reshape((162, 3)) / 27.0
