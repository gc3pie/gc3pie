"""This module defines an ASE interface to ABINIT.

http://www.abinit.org/
"""

import os
from glob import glob
from os.path import join, isfile, islink

import numpy as np

from ase.data import chemical_symbols
from ase.data import atomic_numbers
from ase.units import Bohr, Hartree


class Abinit:
    """Class for doing ABINIT calculations.

    The default parameters are very close to those that the ABINIT
    Fortran code would use.  These are the exceptions::

      calc = Abinit(label='abinit', xc='LDA', pulay=5, mix=0.1)

    Use the set_inp method to set extra INPUT parameters::

      calc.set_inp('nstep', 30)

    """
    def __init__(self, label='abinit', xc='LDA', kpts=None, nbands=0,
                 width=0.04*Hartree, ecut=None, charge=0,
                 pulay=5, mix=0.1, pps='fhi', toldfe=1.0e-6
                 ):
        """Construct ABINIT-calculator object.

        Parameters
        ==========
        label: str
            Prefix to use for filenames (label.in, label.txt, ...).
            Default is 'abinit'.
        xc: str
            Exchange-correlation functional.  Must be one of LDA, PBE,
            revPBE, RPBE.
        kpts: list of three int
            Monkhost-Pack sampling.
        nbands: int
            Number of bands.
            Default is 0.
        width: float
            Fermi-distribution width in eV.
            Default is 0.04 Hartree.
        ecut: float
            Planewave cutoff energy in eV.
            No default.
        charge: float
            Total charge of the system.
            Default is 0.
        pulay: int
            Number of old densities to use for Pulay mixing.
        mix: float
            Mixing parameter between zero and one for density mixing.

        Examples
        ========
        Use default values:

        >>> h = Atoms('H', calculator=Abinit())
        >>> h.center(vacuum=3.0)
        >>> e = h.get_potential_energy()

        """

        if not nbands > 0:
            raise ValueError('Number of bands (nbands) not set')

        if ecut is None:
            raise ValueError('Planewave cutoff energy in eV (ecut) not set')

        self.label = label#################### != out
        self.xc = xc
        self.kpts = kpts
        self.nbands = nbands
        self.width = width
        self.ecut = ecut
        self.charge = charge
        self.pulay = pulay
        self.mix = mix
        self.pps = pps
        self.toldfe = toldfe
        if not pps in ['fhi', 'hgh', 'hgh.sc']:
            raise ValueError('Unexpected PP identifier %s' % pps)

        self.converged = False
        self.inp = {}
        self.n_entries_int = 20 # integer entries per line
        self.n_entries_float = 8 # float entries per line

    def update(self, atoms):
        if (not self.converged or
            len(self.numbers) != len(atoms) or
            (self.numbers != atoms.get_atomic_numbers()).any()):
            self.initialize(atoms)
            self.calculate(atoms)
        elif ((self.positions != atoms.get_positions()).any() or
              (self.pbc != atoms.get_pbc()).any() or
              (self.cell != atoms.get_cell()).any()):
            self.calculate(atoms)

    def initialize(self, atoms):
        self.numbers = atoms.get_atomic_numbers().copy()
        self.species = []
        for a, Z in enumerate(self.numbers):
            if Z not in self.species:
                self.species.append(Z)

        if 'ABINIT_PP_PATH' in os.environ:
            pppaths = os.environ['ABINIT_PP_PATH'].split(':')
        else:
            pppaths = []

        self.ppp_list = []
        if self.xc != 'LDA':
            xcname = 'GGA'
        else:
            xcname = 'LDA'

        for Z in self.species:
            symbol = chemical_symbols[abs(Z)]
            number = atomic_numbers[symbol]

            pps = self.pps
            if pps == 'fhi':
                name = '%02d-%s.%s.fhi' % (number, symbol, xcname)
            elif pps in ('hgh', 'hgh.sc'):
                hghtemplate = '%d%s.%s.hgh' # E.g. "42mo.6.hgh"
                # There might be multiple files with different valence
                # electron counts, so we must choose between
                # the ordinary and the semicore versions for some elements.
                #
                # Therefore we first use glob to get all relevant files,
                # then pick the correct one afterwards.
                name = hghtemplate % (number, symbol.lower(), '*')

            found = False
            for path in pppaths:
                if pps.startswith('hgh'):
                    filenames = glob(join(path, name))
                    if not filenames:
                        continue
                    assert len(filenames) in [0, 1, 2]
                    if pps == 'hgh':
                        selector = min # Lowest possible valence electron count
                    else:
                        assert pps == 'hgh.sc'
                        selector = max # Semicore - highest electron count
                    Z = selector([int(os.path.split(name)[1].split('.')[1])
                                  for name in filenames])
                    name = hghtemplate % (number, symbol.lower(), str(Z))
                filename = join(path, name)
                if isfile(filename) or islink(filename):
                    found = True
                    self.ppp_list.append(filename)
                    break
            if not found:
                raise RuntimeError('No pseudopotential for %s!' % symbol)

        self.converged = False

    def get_potential_energy(self, atoms, force_consistent=False):
        self.update(atoms)

        if force_consistent:
            return self.efree
        else:
            # Energy extrapolated to zero Kelvin:
            return  (self.etotal + self.efree) / 2

    def get_number_of_bands(self):
        return self.nbands

    def get_kpts_info(self, kpt=0, spin=0, mode='eigenvalues'):
        return self.read_kpts_info(kpt, spin, mode)

    def get_k_point_weights(self):
        return self.get_kpts_info(kpt=0, spin=0, mode='k_point_weights')

    def get_bz_k_points(self):
        raise NotImplementedError

    def get_ibz_k_points(self):
        return self.get_kpts_info(kpt=0, spin=0, mode='ibz_k_points')

    def get_fermi_level(self):
        return self.read_fermi()

    def get_eigenvalues(self, kpt=0, spin=0):
        return self.get_kpts_info(kpt, spin, 'eigenvalues')

    def get_occupations(self, kpt=0, spin=0):
        return self.get_kpts_info(kpt, spin, 'occupations')

    def get_forces(self, atoms):
        self.update(atoms)
        return self.forces.copy()

    def get_stress(self, atoms):
        self.update(atoms)
        return self.stress.copy()

    def calculate(self, atoms):
        self.positions = atoms.get_positions().copy()
        self.cell = atoms.get_cell().copy()
        self.pbc = atoms.get_pbc().copy()

        self.write_files()

        self.write_inp(atoms)

        abinit = os.environ['ABINIT_SCRIPT']
        locals = {'label': self.label}

        # Now, because (stupidly) abinit when it finds a name it uses nameA
        # and when nameA exists it uses nameB, etc.
        # we need to rename our *.txt file to *.txt.bak
        filename = self.label + '.txt'
        if islink(filename) or isfile(filename):
            os.rename(filename, filename+'.bak')

        execfile(abinit, {}, locals)
        exitcode = locals['exitcode']
        if exitcode != 0:
            raise RuntimeError(('Abinit exited with exit code: %d.  ' +
                                'Check %s.log for more information.') %
                               (exitcode, self.label))

        self.read()

        self.converged = True

    def write_files(self):
        """Write input parameters to files-file."""
        fh = open(self.label + '.files', 'w')

        import getpass
        #find a suitable default scratchdir (should be writeable!)
        username=getpass.getuser()

        if os.access("/scratch/"+username,os.W_OK):
                scratch = "/scratch/"+username
        elif os.access("/scratch/",os.W_OK):
                scratch = "/scratch/"
        else:
                if os.access(os.curdir,os.W_OK):
                        scratch = os.curdir #if no /scratch use curdir
                else:
                        raise IOError,"No suitable scratch directory and no write access to current dir"

        fh.write('%s\n' % (self.label+'.in')) # input
        fh.write('%s\n' % (self.label+'.txt')) # output
        fh.write('%s\n' % (self.label+'i')) # input
        fh.write('%s\n' % (self.label+'o')) # output
        # scratch files
        fh.write('%s\n' % (os.path.join(scratch, self.label+'.abinit')))
        # Provide the psp files
        for ppp in self.ppp_list:
            fh.write('%s\n' % (ppp)) # psp file path

        fh.close()

    def set_inp(self, key, value):
        """Set INPUT parameter."""
        self.inp[key] = value

    def write_inp(self, atoms):
        """Write input parameters to in-file."""
        fh = open(self.label + '.in', 'w')

        inp = {
            #'SystemLabel': self.label,
            #'LatticeConstant': 1.0,
            'natom': len(atoms),
            'charge': self.charge,
            'nband': self.nbands,
            #'DM.UseSaveDM': self.converged,
            #'SolutionMethod': 'diagon',
            'npulayit': self.pulay, # default 7
            'diemix': self.mix
            }

        if self.ecut is not None:
            inp['ecut'] = str(self.ecut)+' eV' # default Ha

        if self.width is not None:
            inp['tsmear'] = str(self.width)+' eV' # default Ha
            fh.write('occopt 3 # Fermi-Dirac smearing\n')

        inp['ixc'] = { # default 1
            'LDA':     7,
            'PBE':    11,
            'revPBE': 14,
            'RPBE':   15,
            'WC':     23
            }[self.xc]

        magmoms = atoms.get_initial_magnetic_moments()
        if magmoms.any():
            inp['nsppol'] = 2
            fh.write('spinat\n')
            for n, M in enumerate(magmoms):
                if M != 0:
                    fh.write('%.14f %.14f %.14f\n' % (0, 0, M))
        else:
            inp['nsppol'] = 1
            #fh.write('spinat\n')
            #for n, M in enumerate(magmoms):
            #    if M != 0:
            #        fh.write('%.14f\n' % (M))

        inp.update(self.inp)

        for key, value in inp.items():
            if value is None:
                continue

            if isinstance(value, list):
                fh.write('%block %s\n' % key)
                for line in value:
                    fh.write(' '.join(['%s' % x for x in line]) + '\n')
                fh.write('%endblock %s\n' % key)

            unit = keys_with_units.get(inpify(key))
            if unit is None:
                fh.write('%s %s\n' % (key, value))
            else:
                if 'fs**2' in unit:
                    value /= fs**2
                elif 'fs' in unit:
                    value /= fs
                fh.write('%s %f %s\n' % (key, value, unit))

        fh.write('#Definition of the unit cell\n')
        fh.write('acell\n')
        fh.write('%.14f %.14f %.14f Angstrom\n' %  (1.0, 1.0, 1.0))
        fh.write('rprim\n')
        for v in self.cell:
            fh.write('%.14f %.14f %.14f\n' %  tuple(v))

        fh.write('chkprim 0 # Allow non-primitive cells\n')

        fh.write('#Definition of the atom types\n')
        fh.write('ntypat %d\n' % (len(self.species)))
        fh.write('znucl')
        for n, Z in enumerate(self.species):
            fh.write(' %d' % (Z))
        fh.write('\n')
        fh.write('#Enumerate different atomic species\n')
        fh.write('typat')
        fh.write('\n')
        self.types = []
        for Z in self.numbers:
            for n, Zs in enumerate(self.species):
                if Z == Zs:
                    self.types.append(n+1)
        for n, type in enumerate(self.types):
            fh.write(' %d' % (type))
            if n > 1 and ((n % self.n_entries_int) == 1):
                fh.write('\n')
        fh.write('\n')

        fh.write('#Definition of the atoms\n')
        fh.write('xangst\n')
        a = 0
        for pos, Z in zip(self.positions, self.numbers):
            a += 1
            fh.write('%.14f %.14f %.14f\n' %  tuple(pos))

        if self.kpts is not None:
            fh.write('kptopt %d\n' % (1))
            fh.write('ngkpt ')
            fh.write('%d %d %d\n' %  tuple(self.kpts))

        fh.write('#Definition of the SCF procedure\n')
        fh.write('toldfe %.1g\n' %  self.toldfe)
        fh.write('chkexit 1 # abinit.exit file in the running directory terminates after the current SCF\n')

        fh.close()

    def read_fermi(self):
        """Method that reads Fermi energy in Hartree from the output file
        and returns it in eV"""
        E_f=None
        filename = self.label + '.txt'
        text = open(filename).read().lower()
        assert 'error' not in text
        for line in iter(text.split('\n')):
            if line.rfind('fermi (or homo) energy (hartree) =') > -1:
                E_f = float(line.split('=')[1].strip().split()[0])
        return E_f*Hartree

    def read_kpts_info(self, kpt=0, spin=0, mode='eigenvalues'):
        """ Returns list of eigenvalues, occupations, kpts weights, or
        kpts coordinates for given kpt and spin"""
        # output may look like this (or without occupation entries); 8 entries per line:
        #
        #  Eigenvalues (hartree) for nkpt=  20  k points:
        # kpt#   1, nband=  3, wtk=  0.01563, kpt=  0.0625  0.0625  0.0625 (reduced coord)
        #  -0.09911   0.15393   0.15393
        #      occupation numbers for kpt#   1
        #   2.00000   0.00000   0.00000
        # kpt#   2, nband=  3, wtk=  0.04688, kpt=  0.1875  0.0625  0.0625 (reduced coord)
        # ...
        #
        assert mode in ['eigenvalues' , 'occupations', 'ibz_k_points', 'k_point_weights'], 'mode not in [\'eigenvalues\' , \'occupations\', \'ibz_k_points\', \'k_point_weights\']'
        # number of lines of eigenvalues/occupations for a kpt
        n_entry_lines = max(1, int(self.nbands/self.n_entries_float))
        #
        filename = self.label + '.txt'
        text = open(filename).read().lower()
        assert 'error' not in text
        lines = iter(text.split('\n'))
        text_list = []
        # find the begining line of eigenvalues
        contains_eigenvalues = False
        for line in lines:
            #if line.rfind('eigenvalues (hartree) for nkpt') > -1: #MDTMP
            if line.rfind('eigenvalues (   ev  ) for nkpt') > -1:
                n_kpts = int(line.split('nkpt=')[1].strip().split()[0])
                contains_eigenvalues = True
                break
        # find the end line of eigenvalues starting from linenum
        for line in lines:
            text_list.append(line)
            if not line.strip(): # find a blank line
                break
        # remove last (blank) line
        text_list = text_list[:-1]

        assert contains_eigenvalues, 'No eigenvalues found in the output'

        # join text eigenvalues description with eigenvalues
        # or occupation numbers for kpt# with occupations
        contains_occupations = False
        for line in text_list:
            if line.rfind('occupation numbers') > -1:
                contains_occupations = True
                break
        if mode == 'occupations':
            assert contains_occupations, 'No occupations found in the output'

        if contains_occupations:
            range_kpts = 2*n_kpts
        else:
            range_kpts = n_kpts
        #
        values_list = []
        offset = 0
        for kpt_entry in range(range_kpts):
            full_line = ''
            for entry_line in range(n_entry_lines+1):
                full_line = full_line+str(text_list[offset+entry_line])
            first_line = text_list[offset]
            if mode == 'occupations':
                if first_line.rfind('occupation numbers') > -1:
                    # extract numbers
                    full_line = [float(v) for v in full_line.split('#')[1].strip().split()[1:]]
                    values_list.append(full_line)
            elif mode in ['eigenvalues', 'ibz_k_points', 'k_point_weights']:
                if first_line.rfind('reduced coord') > -1:
                    # extract numbers
                    if mode == 'eigenvalues':
                        #full_line = [Hartree*float(v) for v in full_line.split(')')[1].strip().split()[:]] # MDTMP
                        full_line = [float(v) for v in full_line.split(')')[1].strip().split()[:]]
                    elif mode == 'ibz_k_points':
                        full_line = [float(v) for v in full_line.split('kpt=')[1].strip().split('(')[0].split()]
                    else:
                        full_line = float(full_line.split('wtk=')[1].strip().split(',')[0].split()[0])
                    values_list.append(full_line)
            offset = offset+n_entry_lines+1
        #
        if mode in ['occupations', 'eigenvalues']:
            return np.array(values_list[kpt])
        else:
            return np.array(values_list)

    def read(self):
        """Read results from ABINIT's text-output file."""
        filename = self.label + '.txt'
        text = open(filename).read().lower()
        assert 'error' not in text
        assert 'was not enough scf cycles to converge' not in text
        # some consistency ckecks
        for line in iter(text.split('\n')):
            if line.rfind('natom  ') > -1:
                natom = int(line.split()[-1])
                assert natom == len(self.numbers)
        for line in iter(text.split('\n')):
            if line.rfind('znucl  ') > -1:
                znucl = [float(Z) for Z in line.split()[1:]]
                for n, Z in enumerate(self.species):
                    assert Z == znucl[n]
        lines = text.split('\n')
        for n, line in enumerate(lines):
            if line.rfind(' typat  ') > -1:
                nlines = len(self.numbers) / self.n_entries_int
                typat = [int(t) for t in line.split()[1:]] # first line
                for nline in range(nlines): # remaining lines
                    for t in lines[1 + n + nline].split()[:]:
                        typat.append(int(t))
        for n, t in enumerate(self.types):
            assert t == typat[n]

        lines = iter(text.split('\n'))
        # Stress:
        # Printed in the output in the following format [Hartree/Bohr^3]:
        # sigma(1 1)=  4.02063464E-04  sigma(3 2)=  0.00000000E+00
        # sigma(2 2)=  4.02063464E-04  sigma(3 1)=  0.00000000E+00
        # sigma(3 3)=  4.02063464E-04  sigma(2 1)=  0.00000000E+00
        for line in lines:
            if line.rfind('cartesian components of stress tensor (hartree/bohr^3)') > -1:
                self.stress = np.empty((3, 3))
                for i in range(3):
                    entries = lines.next().split()
                    self.stress[i,i] = float(entries[2])
                    self.stress[min(3, 4-i)-1, max(1, 2-i)-1] = float(entries[5])
                    self.stress[max(1, 2-i)-1, min(3, 4-i)-1] = float(entries[5])
                self.stress = self.stress*Hartree/Bohr**3
                break
        else:
            raise RuntimeError

        # Energy [Hartree]:
        # Warning: Etotal could mean both electronic energy and free energy!
        for line in iter(text.split('\n')):
            if line.rfind('>>>>> internal e=') > -1:
                self.etotal = float(line.split('=')[-1])*Hartree
                for line1 in iter(text.split('\n')):
                    if line1.rfind('>>>>>>>>> etotal=') > -1:
                        self.efree = float(line1.split('=')[-1])*Hartree
                        break
                else:
                    raise RuntimeError
                break
        else:
            for line2 in iter(text.split('\n')):
                if line2.rfind('>>>>>>>>> etotal=') > -1:
                    self.etotal = float(line2.split('=')[-1])*Hartree
                    self.efree = self.etotal
                    break
            else:
                raise RuntimeError

        # Forces:
        for line in lines:
            if line.rfind('cartesian forces (ev/angstrom) at end:') > -1:
                forces = []
                for i in range(len(self.numbers)):
                    forces.append(np.array([float(f) for f in lines.next().split()[1:]]))
                self.forces = np.array(forces)
                break
        else:
            raise RuntimeError

def inpify(key):
    return key.lower().replace('_', '').replace('.', '').replace('-', '')


keys_with_units = {
    }
#keys_with_units = {
#    'paoenergyshift': 'eV',
#    'zmunitslength': 'Bohr',
#    'zmunitsangle': 'rad',
#    'zmforcetollength': 'eV/Ang',
#    'zmforcetolangle': 'eV/rad',
#    'zmmaxdispllength': 'Ang',
#    'zmmaxdisplangle': 'rad',
#    'ecut': 'eV',
#    'dmenergytolerance': 'eV',
#    'electronictemperature': 'eV',
#    'oneta': 'eV',
#    'onetaalpha': 'eV',
#    'onetabeta': 'eV',
#    'onrclwf': 'Ang',
#    'onchemicalpotentialrc': 'Ang',
#    'onchemicalpotentialtemperature': 'eV',
#    'mdmaxcgdispl': 'Ang',
#    'mdmaxforcetol': 'eV/Ang',
#    'mdmaxstresstol': 'eV/Ang**3',
#    'mdlengthtimestep': 'fs',
#    'mdinitialtemperature': 'eV',
#    'mdtargettemperature': 'eV',
#    'mdtargetpressure': 'eV/Ang**3',
#    'mdnosemass': 'eV*fs**2',
#    'mdparrinellorahmanmass': 'eV*fs**2',
#    'mdtaurelax': 'fs',
#    'mdbulkmodulus': 'eV/Ang**3',
#    'mdfcdispl': 'Ang',
#    'warningminimumatomicdistance': 'Ang',
#    'rcspatial': 'Ang',
#    'kgridcutoff': 'Ang',
#    'latticeconstant': 'Ang'}
