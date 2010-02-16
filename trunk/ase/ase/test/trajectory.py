import os
from ase import *

co = Atoms([Atom('C', (0, 0, 0)),
            Atom('O', (0, 0, 1.2))])
traj = PickleTrajectory('1.traj', 'w', co)
for i in range(5):
    co.positions[:, 2] += 0.1
    traj.write()
del traj
traj = PickleTrajectory('1.traj', 'a')
co = traj[-1]
print co.positions
co.positions[:] += 1
traj.write(co)
del traj
t = PickleTrajectory('1.traj', 'a')
print t[-1].positions
print '.--------'
for a in t:
    print 1, a.positions[-1,2]
co.positions[:] += 1
t.write(co)
for a in t:
    print 2, a.positions[-1,2]
assert len(t) == 7

# append to a nonexisting file
fname = '2.traj'
if os.path.isfile(fname):
    os.remove(fname)
t = PickleTrajectory(fname, 'a', co)
del(t)
os.remove(fname)

