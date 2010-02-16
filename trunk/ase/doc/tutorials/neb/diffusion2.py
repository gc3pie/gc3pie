from ase import *

initial = read('initial.traj')
final = read('final.traj')

constraint = FixAtoms(mask=[atom.tag > 1 for atom in initial])

images = [initial]
for i in range(3):
    image = initial.copy()
    image.set_calculator(EMT())
    image.set_constraint(constraint)
    images.append(image)

images.append(final)

neb = NEB(images)
neb.interpolate()
qn = QuasiNewton(neb, trajectory='neb.traj')
qn.run(fmax=0.05)
