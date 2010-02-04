try:
    import scipy
except ImportError:
    from ase.test import NotAvailable
    raise NotAvailable('This test needs scipy module.')
from ase import *
from ase.structure import bulk
a0 = 3.52 / sqrt(2)
c0 = sqrt(8 / 3.0) * a0
print '%.4f %.3f' % (a0, c0 / a0)
for i in range(3):
    traj = PickleTrajectory('Ni.traj', 'w')
    eps = 0.01
    for a in a0 * np.linspace(1 - eps, 1 + eps, 4):
        for c in c0 * np.linspace(1 - eps, 1 + eps, 4):
            ni = bulk('Ni', 'hcp', a=a, covera=c / a)
            ni.set_calculator(EMT())
            ni.get_potential_energy()
            traj.write(ni)

    configs = read('Ni.traj@:')
    energies = [config.get_potential_energy() for config in configs]
    ac = [(config.cell[0, 0], config.cell[2, 2]) for config in configs]
    from ase.optimize import polyfit
    p = polyfit(ac, energies)
    from scipy.optimize import fmin_bfgs
    a0, c0 = fmin_bfgs(p, (a0, c0))
    print '%.4f %.3f' % (a0, c0 / a0)
assert abs(a0 - 2.469) < 0.001
assert abs(c0 / a0 - 1.624) < 0.005


