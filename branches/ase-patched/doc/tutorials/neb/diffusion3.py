from ase import *
from ase.parallel import rank, size

initial = read('initial.traj')
final = read('final.traj')

constraint = FixAtoms(mask=[atom.tag > 1 for atom in initial])

images = [initial]
j = rank * 3 // size  # my image number
for i in range(3):
    image = initial.copy()
    if i == j:
        image.set_calculator(EMT())
    image.set_constraint(constraint)
    images.append(image)
images.append(final)

neb = NEB(images)
neb.interpolate()
qn = QuasiNewton(neb)
if rank % (size // 3) == 0:
    traj = PickleTrajectory('neb%d.traj' % j, 'w', images[1 + j], master=True)
    qn.attach(traj)
qn.run(fmax=0.05)
