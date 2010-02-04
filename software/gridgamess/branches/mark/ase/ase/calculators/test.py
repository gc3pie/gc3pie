from math import pi

import numpy as np

from ase.atoms import Atoms


def make_test_dft_calculation():
    a = b = 2.0
    c = 6.0
    atoms = Atoms(positions=[(0, 0, c / 2)],
                  symbols='H',
                  pbc=(1, 1, 0),
                  cell=(a, b, c),
                  calculator=TestCalculator())
    return atoms


class TestCalculator:
    def __init__(self, nk=8):
        assert nk % 2 == 0
        bzk = []
        weights = []
        ibzk = []
        w = 1.0 / nk**2
        for i in range(-nk + 1, nk, 2):
            for j in range(-nk + 1, nk, 2):
                k = (0.5 * i / nk, 0.5 * j / nk, 0)
                bzk.append(k)
                if i >= j > 0:
                    ibzk.append(k)
                    if i == j:
                        weights.append(4 * w)
                    else:
                        weights.append(8 * w)
        assert abs(sum(weights) - 1.0) < 1e-12
        self.bzk = np.array(bzk)
        self.ibzk = np.array(ibzk)
        self.weights = np.array(weights)

        # Calculate eigenvalues and wave functions:
        self.init()

    def init(self):
        nibzk = len(self.weights)
        nbands = 1

        V = -1.0
        self.eps = 2 * V * (np.cos(2 * pi * self.ibzk[:, 0]) +
                            np.cos(2 * pi * self.ibzk[:, 1]))
        self.eps.shape = (nibzk, nbands)

        self.psi = np.zeros((nibzk, 20, 20, 60), complex)
        phi = np.empty((2, 2, 20, 20, 60))
        z = np.linspace(-1.5, 1.5, 60, endpoint=False)
        for i in range(2):
            x = np.linspace(0, 1, 20, endpoint=False) - i
            for j in range(2):
                y = np.linspace(0, 1, 20, endpoint=False) - j
                r = (((x[:, None]**2 +
                       y**2)[:, :, None] +
                      z**2)**0.5).clip(0, 1)
                phi = 1.0 - r**2 * (3.0 - 2.0 * r)
                phase = np.exp(pi * 2j * np.dot(self.ibzk, (i, j, 0)))
                self.psi += phase[:, None, None, None] * phi

    def get_pseudo_wave_function(self, band=0, kpt=0, spin=0):
        assert spin == 0 and band == 0
        return self.psi[kpt]

    def get_eigenvalues(self, kpt=0, spin=0):
        assert spin == 0
        return self.eps[kpt]

    def get_number_of_bands(self):
        return 1

    def get_k_point_weights(self):
        return self.weights

    def get_number_of_spins(self):
        return 1

    def get_fermi_level(self):
        return 0.0

