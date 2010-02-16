"""This module defines an ASE interface to SIESTA.

http://www.uam.es/departamentos/ciencias/fismateriac/siesta
"""

import os
from os.path import join, isfile, islink
from cmath import exp
import array

import numpy as np

from ase.data import chemical_symbols
from ase.units import Rydberg


class Siesta:
    """Class for doing SIESTA calculations.

    The default parameters are very close to those that the SIESTA
    Fortran code would use.  These are the exceptions::
    
      calc = Siesta(label='siesta', xc='LDA', pulay=5, mix=0.1)

    Use the set_fdf method to set extra FDF parameters::
    
      calc.set_fdf('PAO.EnergyShift', 0.01 * Rydberg)

    """
    def __init__(self, label='siesta', xc='LDA', kpts=None, nbands=None,
                 width=None, meshcutoff=None, charge=None,
                 pulay=5, mix=0.1, maxiter=120,
                 basis=None, ghosts=[],
                 write_fdf=True):
        """Construct SIESTA-calculator object.

        Parameters
        ==========
        label: str
            Prefix to use for filenames (label.fdf, label.txt, ...).
            Default is 'siesta'.
        xc: str
            Exchange-correlation functional.  Must be one of LDA, PBE,
            revPBE, RPBE.
        kpts: list of three int
            Monkhost-Pack sampling.
        nbands: int
            Number of bands.
        width: float
            Fermi-distribution width in eV.
        meshcutoff: float
            Cutoff energy in eV for grid.
        charge: float
            Total charge of the system.
        pulay: int
            Number of old densities to use for Pulay mixing.
        mix: float
            Mixing parameter between zero and one for density mixing.
        write_fdf: bool
            Use write_fdf=False to use your own fdf-file.

        Examples
        ========
        Use default values:
        
        >>> h = Atoms('H', calculator=Siesta())
        >>> h.center(vacuum=3.0)
        >>> e = h.get_potential_energy()
        
        """
        
        self.label = label#################### != out
        self.xc = xc
        self.kpts = kpts
        self.nbands = nbands
        self.width = width
        self.meshcutoff = meshcutoff
        self.charge = charge
        self.pulay = pulay
        self.mix = mix
        self.maxiter = maxiter
        self.basis = basis
        self.ghosts = ghosts
        self.write_fdf_file = write_fdf
    
        self.converged = False
        self.fdf = {}

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
            if a in self.ghosts:
                Z = -Z
            if Z not in self.species:
                self.species.append(Z)

        if 'SIESTA_PP_PATH' in os.environ:
            pppaths = os.environ['SIESTA_PP_PATH'].split(':')
        else:
            pppaths = []

        for Z in self.species:
            symbol = chemical_symbols[abs(Z)]
            name = symbol + '.vps'
            name1 = symbol + '.psf'
            found = False
            for path in pppaths:
                filename = join(path, name)
                filename1 = join(path,name1)
                if isfile(filename) or islink(filename):
                    found = True
                    if path != '.':
                        if islink(name) or isfile(name):
                            os.remove(name)
                        os.symlink(filename, name)

                elif isfile(filename1) or islink(filename1):
                    found = True
                    if path != '.':
                        if islink(name1) or isfile(name1):
                            os.remove(name1)
                        os.symlink(filename1, name1)
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

    def get_forces(self, atoms):
        self.update(atoms)
        return self.forces.copy()
    
    def get_stress(self, atoms):
        self.update(atoms)
        return self.stress.copy()

    def get_dipole_moment(self, atoms):
        """Returns total dipole moment of the system."""
        self.update(atoms)
        return self.dipole

    def read_dipole(self):
        dipolemoment=np.zeros([1,3])
        for line in open(self.label + '.txt', 'r'):
            if line.rfind('Electric dipole (Debye)') > -1:
                dipolemoment=np.array([float(f) for f in line.split()[5:8]])
        #debye to e*Ang (the units of VASP)
        dipolemoment=dipolemoment*0.2081943482534
        return dipolemoment

    def calculate(self, atoms):
        self.positions = atoms.get_positions().copy()
        self.cell = atoms.get_cell().copy()
        self.pbc = atoms.get_pbc().copy()

        if self.write_fdf_file:
            self.write_fdf(atoms)

        siesta = os.environ['SIESTA_SCRIPT']
        locals = {'label': self.label}
        execfile(siesta, {}, locals)
        exitcode = locals['exitcode']
        if exitcode != 0:
            raise RuntimeError(('Siesta exited with exit code: %d.  ' +
                                'Check %s.txt for more information.') %
                               (exitcode, self.label))
        
        self.dipole = self.read_dipole()
        self.read()

        self.converged = True
        
    def set_fdf(self, key, value):
        """Set FDF parameter."""
        self.fdf[key] = value
                
    def write_fdf(self, atoms):
        """Write input parameters to fdf-file."""
        fh = open(self.label + '.fdf', 'w')

        fdf = {
            'SystemLabel': self.label,
            'AtomicCoordinatesFormat': 'Ang',
            'LatticeConstant': 1.0,
            'NumberOfAtoms': len(atoms),
            'MeshCutoff': self.meshcutoff,
            'NetCharge': self.charge,
            'ElectronicTemperature': self.width,
            'NumberOfEigenStates': self.nbands,
            'DM.UseSaveDM': self.converged,
            'PAO.BasisSize': self.basis,
            'SolutionMethod': 'diagon',
            'DM.NumberPulay': self.pulay,
            'DM.MixingWeight': self.mix,
            'MaxSCFIterations' : self.maxiter
            }
        
        if self.xc != 'LDA':
            fdf['xc.functional'] = 'GGA'
            fdf['xc.authors'] = self.xc

        magmoms = atoms.get_initial_magnetic_moments()
        if magmoms.any():
            fdf['SpinPolarized'] = True
            fh.write('%block InitSpin\n')
            for n, M in enumerate(magmoms):
                if M != 0:
                    fh.write('%d %.14f\n' % (n + 1, M))
            fh.write('%endblock InitSpin\n')
        
        fdf['Number_of_species'] = len(self.species)

        fdf.update(self.fdf)

        for key, value in fdf.items():
            if value is None:
                continue

            if isinstance(value, list):
                fh.write('%%block %s\n' % key)
                for line in value:
                    fh.write(line + '\n')
                fh.write('%%endblock %s\n' % key)
            else:
                unit = keys_with_units.get(fdfify(key))
                if unit is None:
                    fh.write('%s %s\n' % (key, value))
                else:
                    if 'fs**2' in unit:
                        value /= fs**2
                    elif 'fs' in unit:
                        value /= fs
                    fh.write('%s %f %s\n' % (key, value, unit))

        fh.write('%block LatticeVectors\n')
        for v in self.cell:
            fh.write('%.14f %.14f %.14f\n' %  tuple(v))
        fh.write('%endblock LatticeVectors\n')

        fh.write('%block Chemical_Species_label\n')
        for n, Z in enumerate(self.species):
            fh.write('%d %s %s\n' % (n + 1, Z, chemical_symbols[abs(Z)]))
        fh.write('%endblock Chemical_Species_label\n')

        fh.write('%block AtomicCoordinatesAndAtomicSpecies\n')
        a = 0
        for pos, Z in zip(self.positions, self.numbers):
            if a in self.ghosts:
                Z = -Z
            a += 1
            fh.write('%.14f %.14f %.14f' %  tuple(pos))
            fh.write(' %d\n' % (self.species.index(Z) + 1))
        fh.write('%endblock AtomicCoordinatesAndAtomicSpecies\n')

        if self.kpts is not None:
            fh.write('%block kgrid_Monkhorst_Pack\n')
            for i in range(3):
                for j in range(3):
                    if i == j:
                        fh.write('%d ' % self.kpts[i])
                    else:
                        fh.write('0 ')
                fh.write('%.1f\n' % (((self.kpts[i] + 1) % 2) * 0.5))
            fh.write('%endblock kgrid_Monkhorst_Pack\n')
        
        fh.close()
        
    def read(self):
        """Read results from SIESTA's text-output file."""
        text = open(self.label + '.txt', 'r').read().lower()
        assert 'error' not in text
        lines = iter(text.split('\n'))

        # Get the number of grid points used:
        for line in lines:
            if line.startswith('initmesh: mesh ='):
                self.grid = [int(word) for word in line.split()[3:8:2]]
                break

        # Stress:
        for line in lines:
            if line.startswith('siesta: stress tensor (total) (ev/ang**3):'):
                self.stress = np.empty((3, 3))
                for i in range(3):
                    self.stress[i] = [float(word)
                                      for word in lines.next().split()]
                break
        else:
            raise RuntimeError

        # Energy:
        for line in lines:
            if line.startswith('siesta: etot    ='):
                self.etotal = float(line.split()[-1])
                self.efree = float(lines.next().split()[-1])
                break
        else:
            raise RuntimeError

        # Forces:
        lines = open(self.label + '.FA', 'r').readlines()
        assert int(lines[0]) == len(self.numbers)
        assert len(lines) == len(self.numbers) + 1
        self.forces = np.array([[float(word)
                                 for word in line.split()[1:4]]
                                for line in lines[1:]])

    def read_hs(self, filename, is_gamma_only=False, magnus=False):
        """Read the Hamiltonian and overlap matrix from a Siesta 
           calculation in sparse format. 

        Parameters
        ==========
        filename: str
            The filename should be on the form jobname.HS  
        is_gamma_only: {False, True), optional
            Is it a gamma point calculation?
        magnus: bool
            The fileformat was changed by Magnus in Siesta at some
            point around version 2.xxx. 
            Use mangus=False, to use the old file format.

        Note
        ====
        Data read in is put in self._dat.

        Examples
        ========
            >>> calc = Siesta()
            >>> calc.read_hs('jobname.HS')
            >>> print calc._dat.fermi_level
            >>> print 'Number of orbitals: %i' % calc._dat.nuotot 
        """
        assert not magnus, 'Not implemented; changes by Magnus to file io' 
        assert not is_gamma_only, 'Not implemented. Only works for k-points.'
        class Dummy: pass
        self._dat = dat = Dummy()
        # Try to read supercell and atom data from a jobname.XV file
        filename_xv = filename[:-2] + 'XV'
        #assert isfile(filename_xv), 'Missing jobname.XV file'
        if isfile(filename_xv):
            print 'Reading supercell and atom data from ' + filename_xv
            fd = open(filename_xv, 'r')
            dat.cell = np.zeros((3, 3)) # Supercell
            for a_vec in dat.cell:
                a_vec[:] = np.array(fd.readline().split()[:3], float)
            dat.rcell = 2 * np.pi * np.linalg.inv(dat.cell.T)
            dat.natoms = int(fd.readline().split()[0])
            dat.symbols = []
            dat.pos_ac = np.zeros((dat.natoms, 3))
            for a in range(dat.natoms):
                line = fd.readline().split()
                dat.symbols.append(chemical_symbols[int(line[1])])
                dat.pos_ac[a, :] = [float(line[i]) for i in range(2, 2 + 3)]
        # Read in the jobname.HS file
        fileobj = file(filename, 'rb')
        fileobj.seek(0)
        dat.fermi_level = float(open(filename[:-3] + '.EIG', 'r').readline())
        dat.is_gammay_only = is_gamma_only 
        dat.nuotot, dat.ns, dat.mnh = getrecord(fileobj, 'l')
        nuotot, ns, mnh = dat.nuotot, dat.ns, dat.mnh
        print 'Number of orbitals found: %i' % nuotot
        dat.numh = numh = np.array([getrecord(fileobj, 'l')
                                    for i in range(nuotot)], 'l')
        dat.maxval = max(numh)
        dat.listhptr = listhptr = np.zeros(nuotot, 'l')
        listhptr[0] = 0
        for oi in xrange(1, nuotot):
            listhptr[oi] = listhptr[oi - 1] + numh[oi - 1]
        dat.listh = listh = np.zeros(mnh, 'l')
        
        print 'Reading sparse info'
        for oi in xrange(nuotot):
            for mi in xrange(numh[oi]):
                listh[listhptr[oi] + mi] = getrecord(fileobj, 'l')

        dat.nuotot_sc = nuotot_sc = max(listh)
        dat.h_sparse = h_sparse = np.zeros((mnh, ns), float)
        dat.s_sparse = s_sparse = np.zeros(mnh, float)
        print 'Reading H'
        for si in xrange(ns):
            for oi in xrange(nuotot):
                for mi in xrange(numh[oi]):
                    h_sparse[listhptr[oi] + mi, si] = getrecord(fileobj, 'd')
        print 'Reading S'
        for oi in xrange(nuotot):
            for mi in xrange(numh[oi]):
                s_sparse[listhptr[oi] + mi] = getrecord(fileobj, 'd')

        dat.qtot, dat.temperature = getrecord(fileobj, 'd')
        if not is_gamma_only:
            print 'Reading X'
            dat.xij_sparse = xij_sparse = np.zeros([3, mnh], float)
            for oi in xrange(nuotot):
                for mi in xrange(numh[oi]):
                    xij_sparse[:, listhptr[oi] + mi] = getrecord(fileobj, 'd')
        fileobj.close()

    def get_hs(self, kpt=(0, 0, 0), spin=0, remove_pbc=None, kpt_scaled=True):
        """Hamiltonian and overlap matrices for an arbitrary k-point.
       
        The default values corresponds to the Gamma point for 
        spin 0 and periodic boundary conditions.

        Parameters
        ==========
        kpt : {(0, 0, 0), (3,) array_like}, optional
            k-point in scaled or absolute coordinates.
            For the latter the units should be Bohr^-1.
        spin : {0, 1}, optional
            Spin index 
        remove_pbc : {None, ({'x', 'y', 'z'}, basis)}, optional
            Use remove_pbc to truncate h and s along a cartesian
            axis. 
        basis: {str, dict}
            The basis specification as either a string or a dictionary.
        kpt_scaled : {True, bool}, optional
            Use kpt_scaled=False if `kpt` is in absolute units (Bohr^-1).

        Note
        ====
        read_hs should be called before get_hs gets called.

        Examples
        ========
        >>> calc = Siesta()
        >>> calc.read_hs('jobname.HS')
        >>> h, s = calc.get_hs((0.0, 0.375, 0.375))
        >>> h -= s * calc._dat.fermi_level # fermi level is now at 0.0
        >>> basis = 'szp'
        >>> h, s = calc.get_hs((0.0, 0.375, 0.375), remove_pbc=('x', basis))
        >>> basis = {'Au:'sz}', 'C':'dzp', None:'szp'}
        >>> h, s = calc.get_hs((0.0, 0.375, 0.375), remove_pbc=('x', basis))

        """
        if not hasattr(self, '_dat'):# XXX Crude check if data is avail.
            print 'Please read in data first by calling the method read_hs.'
            return None, None
        dot = np.dot
        dat = self._dat            
        kpt_c = np.array(kpt, float)
        if kpt_scaled:
            kpt_c = dot(kpt_c, dat.rcell)

        h_MM = np.zeros((dat.nuotot, dat.nuotot), complex)
        s_MM = np.zeros((dat.nuotot, dat.nuotot), complex)
        h_sparse, s_sparse = dat.h_sparse, dat.s_sparse
        x_sparse = dat.xij_sparse
        numh, listhptr, listh = dat.numh, dat.listhptr, dat.listh
        indxuo = np.mod(np.arange(dat.nuotot_sc), dat.nuotot)
        
        for iuo in xrange(dat.nuotot):
            for j in range(numh[iuo]):
                ind =  listhptr[iuo] + j 
                jo = listh[ind] - 1
                juo = indxuo[jo]
                kx = dot(kpt_c, x_sparse[:, ind])
                phasef = exp(1.0j * kx)
                h_MM[iuo, juo] += phasef * h_sparse[ind, spin] 
                s_MM[iuo, juo] += phasef * s_sparse[ind]
        
        if remove_pbc is not None:
            direction, basis = remove_pbc
            centers_ic = get_bf_centers(dat.symbols, dat.pos_ac, basis)
            d = 'xyz'.index(direction)
            cutoff = dat.cell[d, d] * 0.5
            truncate_along_axis(h_MM, s_MM, direction, centers_ic, cutoff)
        
        h_MM *= complex(Rydberg)
        return h_MM, s_MM


def getrecord(fileobj, dtype):
    """Used to read in binary files. 
    """
    typetosize = {'l':4, 'f':4, 'd':8}# XXX np.int, np.float32, np.float64
    assert dtype in typetosize # XXX
    size = typetosize[dtype]
    record = array.array('l')
    trunk = array.array(dtype)
    record.fromfile(fileobj, 1)
    nofelements = int(record[-1]) / size
    trunk.fromfile(fileobj, nofelements)
    record.fromfile(fileobj, 1)
    data = np.array(trunk, dtype=dtype)
    if len(data)==1:
        data = data[0]
    return data

def truncate_along_axis(h, s, direction, centers_ic, cutoff):
    """Truncate h and s such along a cartesian axis.

    Parameters:

    h: (N, N) ndarray
        Hamiltonian matrix.
    s: (N, N) ndarray
        Overlap matrix.
    direction: {'x', 'y', 'z'} 
        Truncate allong a cartesian axis.
    centers_ic: (N, 3) ndarray
        Centers of the basis functions.
    cutoff: float
        The (direction-axis projected) cutoff distance.
    """
    dtype = h.dtype
    ni = len(centers_ic)
    d = 'xyz'.index(direction)
    pos_i = centers_ic[:, d]
    for i in range(ni):
        dpos_i = abs(pos_i - pos_i[i])
        mask_i = (dpos_i < cutoff).astype(dtype)
        h[i, :] *= mask_i
        h[:, i] *= mask_i
        s[i, :] *= mask_i
        s[:, i] *= mask_i

def get_nao(symbol, basis):
    """Number of basis functions. 

    Parameters
    ==========
    symbol: str
        The chemical symbol.
    basis: str
        Basis function type.
    """
    ls = valence_config[symbol]
    nao = 0
    zeta = {'s':1, 'd':2, 't':3, 'q':4}
    nzeta = zeta[basis[0]]
    is_pol = 'p' in basis
    for l in ls: 
        nao += (2 * l + 1) * nzeta
    if is_pol:
        l_pol = None
        l = -1 
        while l_pol is None:
            l += 1
            if not l in ls:
                l_pol = l
        nao += 2 * l_pol + 1
    return nao        

def get_bf_centers(symbols, positions, basis):
    """Centers of basis functions.

    Parameters
    ==========
    symbols: str, (N, ) array_like 
        chemical symbol for each atom. 
    positions: float, (N, 3) array_like
        Positions of the atoms.
    basis: {str,  dict}
        Basis set specification as either a string or a dictionary

    Examples
    ========
    >>> symbols = ['O', 'H']
    >>> positions = [(0, 0, 0), (0, 0, 1)]
    >>> basis = 'sz'
    >>> print get_bf_centers(symbols, positions, basis)
    [[0 0 0]
     [0 0 0]
     [0 0 0]
     [0 0 0]
     [0 0 1]]
    >>> basis = {'H':'dz', None:'sz'}
    >>> print get_bf_centers(symbols, positions, basis)
    [[0 0 0]
     [0 0 0]
     [0 0 0]
     [0 0 0]
     [0 0 1]
     [0 0 1]]

    """
    centers_ic = []
    dict_basis = False
    if type(basis)==dict:
        dict_basis = True
    for symbol, pos in zip(symbols, positions):
        if dict_basis:
            if symbol not in basis:
                bas = basis[None]
            else:
                bas = basis[symbol]
        else:
            bas = basis
        for i in range(get_nao(symbol, bas)):
            centers_ic.append(pos)
    return np.asarray(centers_ic)

def fdfify(key):
    return key.lower().replace('_', '').replace('.', '').replace('-', '')

valence_config = {
    'H': (0,),
    'C': (0, 1),
    'N': (0, 1),
    'O': (0, 1),
    'S': (0, 1), 
    'Li': (0,),
    'Na': (0,),
    'Ni': (0, 2),
    'Cu': (0, 2), 
    'Pd': (0, 2), 
    'Ag': (0, 2), 
    'Pt': (0, 2), 
    'Au': (0, 2)}

keys_with_units = {
    'paoenergyshift': 'eV',
    'zmunitslength': 'Bohr',
    'zmunitsangle': 'rad',
    'zmforcetollength': 'eV/Ang',
    'zmforcetolangle': 'eV/rad',
    'zmmaxdispllength': 'Ang',
    'zmmaxdisplangle': 'rad',
    'meshcutoff': 'eV',
    'dmenergytolerance': 'eV',
    'electronictemperature': 'eV',
    'oneta': 'eV',
    'onetaalpha': 'eV',
    'onetabeta': 'eV',
    'onrclwf': 'Ang',
    'onchemicalpotentialrc': 'Ang',
    'onchemicalpotentialtemperature': 'eV',
    'mdmaxcgdispl': 'Ang',
    'mdmaxforcetol': 'eV/Ang',
    'mdmaxstresstol': 'eV/Ang**3',
    'mdlengthtimestep': 'fs',
    'mdinitialtemperature': 'eV',
    'mdtargettemperature': 'eV',
    'mdtargetpressure': 'eV/Ang**3',
    'mdnosemass': 'eV*fs**2',
    'mdparrinellorahmanmass': 'eV*fs**2',
    'mdtaurelax': 'fs',
    'mdbulkmodulus': 'eV/Ang**3',
    'mdfcdispl': 'Ang',
    'warningminimumatomicdistance': 'Ang',
    'rcspatial': 'Ang',
    'kgridcutoff': 'Ang',
    'latticeconstant': 'Ang'}

    

