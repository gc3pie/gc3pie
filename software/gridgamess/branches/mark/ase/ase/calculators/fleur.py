"""This module defines an ASE interface to FLAPW code FLEUR.

http://www.flapw.de
"""

import os
try:
    from subprocess import Popen, PIPE
except ImportError:
    from os import popen3
else:
    def popen3(cmd):
        p = Popen(cmd, shell=True, close_fds=True,
                  stdin=PIPE, stdout=PIPE, stderr=PIPE)
        return p.stdin, p.stdout, p.stderr
import re

import numpy as np

from ase.units import Hartree, Bohr

class FLEUR:
    """Class for doing FLEUR calculations.

    In order to use fleur one has to define the following environment
    variables:

    FLEUR_INPGEN path to the input generator (inpgen.x) of fleur

    FLEUR path to the fleur executable. Note that fleur uses different
    executable for real and complex cases (systems with/without inversion
    symmetry), so FLEUR must point to the correct executable.

    It is probable that user needs to tune manually the input file before
    the actual calculation, so in addition to the standard
    get_potential_energy function this class defines the following utility
    functions:

    write_inp
        generate the input file `inp`
    initialize_density
        creates the initial density after possible manual edits of `inp`
    calculate
        convergence the total energy. With fleur, one specifies always
        only the number of SCF-iterations so this function launches
        the executable several times and monitors the convergence.
    relax
        Uses fleur's internal algorithm for structure
        optimization. Requires that the proper optimization parameters
        (atoms to optimize etc.) are specified by hand in `inp`
    
    """
    def __init__(self, xc='LDA', kpts=None, nbands=None, convergence=None,
                 width=None, kmax=None, mixer=None, workdir=None):

        """Construct FLEUR-calculator object.

        Parameters
        ==========
        xc: str
            Exchange-correlation functional. Must be one of LDA, PBE,
            RPBE.
        kpts: list of three int
            Monkhost-Pack sampling.
        nbands: int
            Number of bands. (not used at the moment)
        convergence: dictionary
            Convergence parameters (currently only energy in eV)
            {'energy' : float}
        width: float
            Fermi-distribution width in eV.
        kmax: float
            Plane wave cutoff in a.u.
        mixer: dictionary
            Mixing parameters imix, alpha, spinf 
            {'imix' : int, 'alpha' : float, 'spinf' : float}
        workdir: str
            Working directory for the calculation
        """
        
        self.xc = xc
        self.kpts = kpts
        self.nbands = nbands
        self.width = width
        self.kmax = kmax
        self.maxiter = 40
        self.maxrelax = 20
        self.mixer = mixer

        if convergence:
            self.convergence = convergence
            self.convergence['energy'] /= Hartree
        else:
            self.convergence = {'energy' : 0.0001}



        self.start_dir = None
        self.workdir = workdir
        if self.workdir:
            self.start_dir = os.getcwd()
            if not os.path.isdir(workdir):
                os.mkdir(workdir)
        else:
            self.workdir = '.'
            self.start_dir = '.'
        
        self.converged = False

    def update(self, atoms):
        """Update a FLEUR calculation."""
        
        if (not self.converged or
            len(self.numbers) != len(atoms) or
            (self.numbers != atoms.get_atomic_numbers()).any()):
            self.initialize(atoms)
            self.calculate(atoms)
        elif ((self.positions != atoms.get_positions()).any() or
              (self.pbc != atoms.get_pbc()).any() or
              (self.cell != atoms.get_cell()).any()):
            self.converged = False
            self.initialize(atoms)
            self.calculate(atoms)

    def initialize(self, atoms):
        """Create an input file inp and generate starting density."""

        self.converged = False
        self.initialize_inp(atoms)
        self.initialize_density()

    def initialize_inp(self, atoms):
        """Create a inp file"""
        os.chdir(self.workdir)

        self.numbers = atoms.get_atomic_numbers().copy()
        self.positions = atoms.get_positions().copy()
        self.cell = atoms.get_cell().copy()
        self.pbc = atoms.get_pbc().copy()

        # create the input
        self.write_inp(atoms)
        
        os.chdir(self.start_dir)

    def initialize_density(self):
        """Creates a new starting density."""

        os.chdir(self.workdir)
        # remove possible conflicting files
        files2remove = ['cdn1', 'fl7para', 'stars', 'wkf2',
                        'kpts', 'broyd', 'broyd.7', 'tmat', 'tmas']

        for f in files2remove:
            if os.path.isfile(f):
                os.remove(f)

        # generate the starting density
        os.system("sed -i -e 's/strho=./strho=T/' inp")
        try:
            fleur_exe = os.environ['FLEUR']
        except KeyError:
            raise RuntimeError('Please set FLEUR')
        cmd = popen3(fleur_exe)[2]
        stat = cmd.read()
        if '!' in stat:
            raise RuntimeError('FLEUR exited with a code %s' % stat)
        os.system("sed -i -e 's/strho=./strho=F/' inp")

        os.chdir(self.start_dir)
        
    def get_potential_energy(self, atoms, force_consistent=False):
        self.update(atoms)

        if force_consistent:
            return self.efree * Hartree
        else:
            # Energy extrapolated to zero Kelvin:
            return  (self.etotal + self.efree) / 2 * Hartree

    def get_forces(self, atoms):
        self.update(atoms)
        # electronic structure is converged, so let's calculate forces:
        # TODO
        return np.array((0.0, 0.0, 0.0))
    
    def get_stress(self, atoms):
        raise NotImplementedError

    def get_dipole_moment(self, atoms):
        """Returns total dipole moment of the system."""
        raise NotImplementedError

    def calculate(self, atoms):
        """Converge a FLEUR calculation to self-consistency.

           Input files should be generated before calling this function
        """
                      
        os.chdir(self.workdir)
        try:
            fleur_exe = os.environ['FLEUR']
        except KeyError:
            raise RuntimeError('Please set FLEUR')

        self.niter = 0
        out = ''
        err = ''
        while not self.converged:
            if self.niter > self.maxiter:
                raise RuntimeError('FLEUR failed to convergence in %d iterations' % self.maxiter)
            
            p = Popen(fleur_exe, shell=True, stdin=PIPE, stdout=PIPE,
                      stderr=PIPE)
            stat = p.wait()
            out = p.stdout.read()
            err = p.stderr.read()
            print err.strip()
            if stat != 0:
                raise RuntimeError('FLEUR exited with a code %d' % stat)
            # catenate new output with the old one
            os.system('cat out >> out.old')
            self.read()
            self.check_convergence()

        os.rename('out.old', 'out')
        # After convergence clean up broyd* files
        os.system('rm -f broyd*')
        os.chdir(self.start_dir)
        return out, err

    def relax(self, atoms):
        """Currently, user has to manually define relaxation parameters
           (atoms to relax, relaxation directions, etc.) in inp file
           before calling this function."""

        nrelax = 0
        relaxed = False
        while not relaxed:
            # Calculate electronic structure
            self.calculate(atoms)
            # Calculate the Pulay forces
            os.system("sed -i -e 's/l_f=./l_f=T/' inp")
            while True:
                self.converged = False
                out, err = self.calculate(atoms)
                if 'GEO new' in err:
                    os.chdir(self.workdir)
                    os.rename('inp_new', 'inp')
                    os.chdir(self.start_dir)
                    break
            if 'GEO: Des woas' in err:
                relaxed = True
                break
            nrelax += 1
            # save the out and cdn1 files
            os.system('cp out out_%d' % nrelax)
            os.system('cp cdn1 cdn1_%d' % nrelax)
            if nrelax > self.maxrelax:
                raise RuntimeError('Failed to relax in %d iterations' % self.maxrelax)
            self.converged = False
                

    def write_inp(self, atoms):
        """Write the `inp` input file of FLEUR.

        First, the information from Atoms is written to the simple input
        file and the actual input file `inp` is then generated with the
        FLEUR input generator. The location of input generator is specified
        in the environment variable FLEUR_INPGEN.

        Finally, the `inp` file is modified according to the arguments of
        the FLEUR calculator object.
        """

        fh = open('inp_simple', 'w')
        fh.write('FLEUR input generated with ASE\n')
        fh.write('\n')
        
        if atoms.pbc[2]:
            film = 'f'
        else:
            film = 't'
        fh.write('&input film=%s /' % film)
        fh.write('\n')

        for vec in atoms.get_cell():
            fh.write(' ')
            for el in vec:
                fh.write(' %21.16f' % (el/Bohr))
            fh.write('\n')
        fh.write(' %21.16f\n' % 1.0)
        fh.write(' %21.16f %21.16f %21.16f\n' % (1.0, 1.0, 1.0))
        fh.write('\n')
        
        natoms = len(atoms)
        fh.write(' %6d\n' % natoms)
        positions = atoms.get_scaled_positions()
        if not atoms.pbc[2]:
            # in film calculations z position has to be in absolute
            # coordinates and symmetrical
            cart_pos = atoms.get_positions()
            cart_pos[:, 2] -= atoms.get_cell()[2, 2]/2.0
            positions[:, 2] = cart_pos[:, 2] / Bohr
        atomic_numbers = atoms.get_atomic_numbers()
        for Z, pos in zip(atomic_numbers, positions):
            fh.write('%3d' % Z)
            for el in pos:
                fh.write(' %21.16f' % el)
            fh.write('\n')

        fh.close()
        try:
            inpgen = os.environ['FLEUR_INPGEN']
        except KeyError:
            raise RuntimeError('Please set FLEUR_INPGEN')

        # rename the previous inp if it exists
        if os.path.isfile('inp'):
            os.rename('inp', 'inp.bak')
        os.system('%s < inp_simple' % inpgen)

        # read the whole inp-file for possible modifications
        fh = open('inp', 'r')
        lines = fh.readlines()
        fh.close()


        for ln, line in enumerate(lines):
            # XC potential
            if line.startswith('pbe'):
                if self.xc == 'PBE':
                    pass
                elif self.xc == 'RPBE':
                    lines[ln] = 'rpbe   non-relativi\n'
                elif self.xc == 'LDA':
                    lines[ln] = 'mjw    non-relativic\n'
                    del lines[ln+1]
                else:
                    raise RuntimeError('XC-functional %s is not supported' % self.xc)
            # kmax
            if self.kmax and line.startswith('Window'):
                line = '%10.5f\n' % self.kmax
                lines[ln+2] = line
            
            # Fermi width
            if self.width and line.startswith('gauss'):
                line = 'gauss=F   %7.5ftria=F\n' % (self.width / Hartree)
                lines[ln] = line
            # kpts
            if self.kpts and line.startswith('nkpt'):
                line = 'nkpt=      nx=%2d,ny=%2d,nz=%2d\n' % (self.kpts[0],
                                                              self.kpts[1],
                                                              self.kpts[2])
                lines[ln] = line
            # Mixing
            if self.mixer and line.startswith('itmax'):
                imix = self.mixer['imix']
                alpha = self.mixer['alpha']
                spinf = self.mixer['spinf']
                line_end = 'imix=%2d,alpha=%6.2f,spinf=%6.2f\n' % (imix,
                                                                   alpha,
                                                                   spinf)
                line = line[:21] + line_end
                lines[ln] = line
                
        # write everything back to inp
        fh = open('inp', 'w')
        for line in lines:
            fh.write(line)
        fh.close()

    def read(self):
        """Read results from FLEUR's text-output file `out`."""

        lines = open('out', 'r').readlines()

        # total energies
        self.total_energies = []
        pat = re.compile('(.*total energy=)(\s)*([-0-9.]*)')
        for line in lines:
            m = pat.match(line)
            if m:
                self.total_energies.append(float(m.group(3)))
        self.etotal = self.total_energies[-1]
        
        # free_energies
        self.free_energies = []
        pat = re.compile('(.*free energy=)(\s)*([-0-9.]*)')
        for line in lines:
            m = pat.match(line)
            if m:
                self.free_energies.append(float(m.group(3)))
        self.efree = self.free_energies[-1]

        # TODO forces, charge density difference...

    def check_convergence(self):
        """Check the convergence of calculation"""
        energy_error = np.ptp(self.total_energies[-3:])
        self.converged = energy_error < self.convergence['energy']

        # TODO check charge convergence

        # reduce the itmax in inp
        lines = open('inp', 'r').readlines()
        pat = re.compile('(itmax=)([ 0-9]*)')
        fh = open('inp', 'w')
        for line in lines:
            m = pat.match(line)
            if m:
                itmax = int(m.group(2))
                self.niter += itmax
                itmax_new = itmax / 2
                itmax = max(5, itmax_new)
                line = 'itmax=%2d' % itmax + line[8:]
            fh.write(line)
        fh.close()
                
