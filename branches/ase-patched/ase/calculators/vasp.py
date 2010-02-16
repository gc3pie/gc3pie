# Copyright (C) 2008 CSC - Scientific Computing Ltd.
"""This module defines an ASE interface to VASP.

Developed on the basis of modules by Jussi Enkovaara and John
Kitchin.  The path of the directory containing the pseudopotential 
directories (potpaw,potpaw_GGA, potpaw_PBE, ...) should be set 
by the environmental flag $VASP_PP_PATH. 

The user should also set the environmental flag $VASP_SCRIPT pointing
to a python script looking something like::

   import os
   exitcode = os.system('vasp')

Alternatively, user can set the environmental flag $VASP_COMMAND pointing
to the command use the launch vasp e.g. 'vasp' or 'mpirun -n 16 vasp'

http://cms.mpi.univie.ac.at/vasp/

-Jonas Bjork j.bjork@liverpool.ac.uk
"""

import os
import sys
from os.path import join, isfile, islink

import numpy as np

import ase

# Parameters that can be set in INCAR. The values which are None
# are not written and default parameters of VASP are used for them.
keys = [
    'prec',       # Precission of calculation (Low, Normal, Accurate)
    'nbands',     # Number of bands
    'encut',      # Planewave cutoff
    'enaug',      # Density cutoff
    'ferwe',      # Fixed band occupation
    'ngx',        # FFT mesh for wavefunctions, x
    'ngy',        # FFT mesh for wavefunctions, y
    'ngz',        # FFT mesh for wavefunctions, z
    'ngxf',       # FFT mesh for charges x
    'ngyf',       # FFT mesh for charges y
    'ngzf',       # FFT mesh for charges z
    'nblk',       # blocking for some BLAS calls (Sec. 6.5)
    'system',     # name of System
    'nwrite',     # verbosity write-flag (how much is written)
    'istart',     # startjob: 0-new 1-cont 2-samecut
    'icharg',     # charge: 1-file 2-atom 10-const
    'iniwav',     # initial electr wf. : 0-lowe 1-rand
    'nelm',       #
    'nelmin',
    'nbands',     #
    'nelmdl',     # nr. of electronic steps
    'ediff',      # stopping-criterion for electronic upd.
    'ediffg',     # stopping-criterion for ionic upd.
    'nsw',        # number of steps for ionic upd.
    'nfree',      # number of steps per DOF when calculting Hessian using finitite differences
    'ibrion',     # ionic relaxation: 0-MD 1-quasi-New 2-CG
    'isif',       # calculate stress and what to relax
    'iwavpr',     # prediction of wf.: 0-non 1-charg 2-wave 3-comb
    'isym',       # symmetry: 0-nonsym 1-usesym
    'symprec',    # precession in symmetry routines
    'lcorr',      # Harris-correction to forces
    'potim',      # time-step for ion-motion (fs)
    'tebeg',      #
    'teend',      # temperature during run
    'smass',      # Nose mass-parameter (am)
    'pomass',     # mass of ions in am
    'zval',       # ionic valence
    'rwigs',      # Wigner-Seitz radii
    'nelect',     # total number of electrons
    'nupdown',    # fix spin moment to specified value
    'emin',       #
    'emax',       # energy-range for DOSCAR file
    'ismear',     # part. occupancies: -5 Blochl -4-tet -1-fermi 0-gaus >0 MP
    'sigma',      # broadening in eV -4-tet -1-fermi 0-gaus
    'algo',       # algorithm: Normal (Davidson) | Fast | Very_Fast (RMM-DIIS)
    'ialgo',      # algorithm: use only 8 (CG) or 48 (RMM-DIIS)
    'lreal',      # non-local projectors in real space
    'ropt',       # number of grid points for non-local proj in real space
    'gga',        # xc-type: PW PB LM or 91
    'voskown',    # use Vosko, Wilk, Nusair interpolation
    'dipol',      # center of cell for dipol
    'idipol',     # monopol/dipol and quadropole corrections
    'ldipol',     # potential correction mode
    'amix',       #
    'bmix',       # tags for mixing
    'lmaxmix',    # 
    'time',       # special control tag
    'lwave',      #
    'lcharg',     #
    'lvtot',      # create WAVECAR/CHGCAR/LOCPOT
    'lelf',       # create ELFCAR
    'lorbit',     # create PROOUT
    'npar',       # parallelization over bands
    'nsim',       # evaluate NSIM bands simultaneously if using RMM-DIIS
    'lscalapack', # switch off scaLAPACK
    'lscalu',     # switch of LU decomposition
    'lasync',     # overlap communcation with calculations
    'addgrid',    # finer grid for augmentation charge density
    'lplane',     # parallelisation over the FFT grid
    'lpard',      # evaluate partial (band and/or k-point) decomposed charge density
    'iband',      # bands to calculate partial charge for
    'eint',       # energy range to calculate partial charge for
    'nbmod',      # specifies mode for partial charge calculation
    'kpuse',      # k-point to calculate partial charge for
    'lsepb',      # write out partial charge of each band seperately?
    'lsepk',      # write out partial charge of each k-point seperately?
    'ispin',      # spin-polarized calculation
    'magmom',     # initial magnetic moments
    'ispin',      # spin-polarized calculation
    'lhfcalc',    # switch to turn on Hartree Fock calculations
    'hfscreen',   # attribute to change from PBE0 to HSE
    'aexx',       # Amount of exact/DFT exchange
    'encutfock',  # FFT grid in the HF related routines 
    'nkred',      # define sub grid of q-points for HF with nkredx=nkredy=nkredz 
    'nkredx',      # define sub grid of q-points in x direction for HF 
    'nkredy',      # define sub grid of q-points in y direction for HF 
    'nkredz',      # define sub grid of q-points in z direction for HF 
    # 'NBLOCK' and KBLOCK       inner block; outer block
    # 'NPACO' and APACO         distance and nr. of slots for P.C.
    # 'WEIMIN, EBREAK, DEPER    special control tags
]

class Vasp:
    def __init__(self, restart=None, output_template='vasp', track_output=False, 
                 **kwargs):
        self.name = 'Vasp'
        self.incar_parameters = {}
        for key in keys:
            self.incar_parameters[key] = None
        self.incar_parameters['prec'] = 'Normal'

        self.input_parameters = {
            'xc':     'PW91',  # exchange correlation potential 
            'setups': None,    # Special setups (e.g pv, sv, ...)
            'txt':    '-',     # Where to send information
            'kpts':   (1,1,1), # k-points
            'gamma':  False,   # Option to use gamma-sampling instead
                               # of Monkhorst-Pack
            }

        self.old_incar_parameters = self.incar_parameters.copy()
        self.old_input_parameters = self.input_parameters.copy()

        self.restart = restart
        if restart:
            self.restart_load()
            return

        if self.input_parameters['xc'] not in ['PW91','LDA','PBE']:
            raise ValueError(
                '%s not supported for xc! use one of: PW91, LDA or PBE.' %
                kwargs['xc'])
        self.nbands = self.incar_parameters['nbands']
        self.atoms = None
        self.run_counts = 0
        self.set(**kwargs)
        self.output_template = output_template
        self.track_output = track_output

    def set(self, **kwargs):
        for key in kwargs:
            if self.input_parameters.has_key(key):
                self.input_parameters[key] = kwargs[key]
            elif self.incar_parameters.has_key(key):
                self.incar_parameters[key] = kwargs[key]
            else:
                raise TypeError('Parameter not defined: ' + key)

    def update(self, atoms):
        if self.calculation_required(atoms, ['energy']):
            if (self.atoms is None or
                self.atoms.positions.shape != atoms.positions.shape):
                # Completely new calculation just reusing the same
                # calculator, so delete any old VASP files found.
                self.clean()
            self.calculate(atoms)

    def initialize(self, atoms):
        """Initialize a VASP calculation

        Constructs the POTCAR file. User should specify the PATH
        to the pseudopotentials in VASP_PP_PATH environment variable"""

        p = self.input_parameters

        self.all_symbols = atoms.get_chemical_symbols()
        self.natoms = len(atoms)
        self.spinpol = atoms.get_initial_magnetic_moments().any()
        atomtypes = atoms.get_chemical_symbols()

        # Determine the number of atoms of each atomic species
        # sorted after atomic species
        special_setups = []
        symbols = {}
        if self.input_parameters['setups']:
            for m in self.input_parameters['setups']:
                try : 
                    #special_setup[self.input_parameters['setups'][m]] = int(m)
                    special_setups.append(int(m))
                except:
                    #print 'setup ' + m + ' is a groups setup'
                    continue
            #print 'special_setups' , special_setups
        
        for m,atom in enumerate(atoms):
            symbol = atom.get_symbol()
            if m in special_setups:
                pass
            else:
                if not symbols.has_key(symbol):
                    symbols[symbol] = 1
                else:
                    symbols[symbol] += 1
        
        # Build the sorting list
        self.sort = []
        self.sort.extend(special_setups)

        for symbol in symbols:
            for m,atom in enumerate(atoms):
                if m in special_setups: 
                    pass
                else:
                    if atom.get_symbol() == symbol:
                        self.sort.append(m)
        self.resort = range(len(self.sort))
        for n in range(len(self.resort)):
            self.resort[self.sort[n]] = n
        self.atoms_sorted = atoms[self.sort]

        # Check if the necessary POTCAR files exists and
        # create a list of their paths.
        self.symbol_count = []
        for m in special_setups:
            self.symbol_count.append([atomtypes[m],1])
        for m in symbols:
            self.symbol_count.append([m,symbols[m]])
        #print 'self.symbol_count',self.symbol_count 
        sys.stdout.flush()
        xc = '/'
        #print 'p[xc]',p['xc']
        if p['xc'] == 'PW91':
            xc = '_gga/'
        elif p['xc'] == 'PBE':
            xc = '_pbe/'
        if 'VASP_PP_PATH' in os.environ:
            pppaths = os.environ['VASP_PP_PATH'].split(':')
        else:
            pppaths = []
        self.ppp_list = []
        #Setting the pseudopotentials, first special setups and 
        # then according to symbols
        for m in special_setups:
            name = 'potpaw'+xc.upper() + p['setups'][str(m)] + '/POTCAR'
            found = False
            for path in pppaths:
                filename = join(path, name)
                #print 'filename', filename
                if isfile(filename) or islink(filename):
                    found = True
                    self.ppp_list.append(filename)
                    break
                elif isfile(filename + '.Z') or islink(filename + '.Z'):
                    found = True
                    self.ppp_list.append(filename+'.Z')
                    break
            if not found:
                raise RuntimeError('No pseudopotential for %s!' % symbol)    
        #print 'symbols', symbols 
        for symbol in symbols:
            try:
                name = 'potpaw'+xc.upper()+symbol + p['setups'][symbol]
            except (TypeError, KeyError):
                name = 'potpaw' + xc.upper() + symbol
            name += '/POTCAR'
            found = False
            for path in pppaths:
                filename = join(path, name)
                #print 'filename', filename
                if isfile(filename) or islink(filename):
                    found = True
                    self.ppp_list.append(filename)
                    break
                elif isfile(filename + '.Z') or islink(filename + '.Z'):
                    found = True
                    self.ppp_list.append(filename+'.Z')
                    break
            if not found:
                raise RuntimeError('No pseudopotential for %s!' % symbol)
        self.converged = None
        self.setups_changed = None

    def calculate(self, atoms):
        """Generate necessary files in the working directory and run VASP.

        The method first write VASP input files, then calls the method
        which executes VASP. When the VASP run is finished energy, forces, 
        etc. are read from the VASP output.
        """
        
        # Write input
        from ase.io.vasp import write_vasp
        self.initialize(atoms)
        write_vasp('POSCAR', self.atoms_sorted, symbol_count = self.symbol_count)
        self.write_incar(atoms)
        self.write_potcar()
        self.write_kpoints()
        self.write_sort_file()
        
        # Execute VASP
        self.run()
        
        # Read output
        atoms_sorted = ase.io.read('CONTCAR', format='vasp')
        p=self.incar_parameters
        if p['ibrion']>-1 and p['nsw']>0:
            atoms.set_positions(atoms_sorted.get_positions()[self.resort])
        self.energy_free, self.energy_zero = self.read_energy()
        self.forces = self.read_forces(atoms)
        self.dipole = self.read_dipole()
        self.fermi = self.read_fermi()
        self.atoms = atoms.copy()
        self.nbands = self.read_nbands()
        if self.spinpol:
            self.magnetic_moment = self.read_magnetic_moment()
            if p['lorbit']>=10 or (p['lorbit']!=None and p['rwigs']):
                self.magnetic_moments = self.read_magnetic_moments(atoms)
        self.old_incar_parameters = self.incar_parameters.copy()
        self.old_input_parameters = self.input_parameters.copy()
        self.converged = self.read_convergence()
        self.stress = self.read_stress()
        
    def run(self):
        """Method which explicitely runs VASP."""

        if self.track_output:
            self.out = self.output_template+str(self.run_counts)+'.out'
            self.run_counts += 1
        else:
            self.out = self.output_template+'.out'
        stderr = sys.stderr
        p=self.input_parameters
        if p['txt'] is None:
            sys.stderr = devnull
        elif p['txt'] == '-':
            pass
        elif isinstance(p['txt'], str):
            sys.stderr = open(p['txt'], 'w')
        if os.environ.has_key('VASP_COMMAND'):
            vasp = os.environ['VASP_COMMAND']
            exitcode = os.system('%s > %s' % (vasp, self.out))
        elif os.environ.has_key('VASP_SCRIPT'):
            vasp = os.environ['VASP_SCRIPT']
            locals={}
            execfile(vasp, {}, locals)
            exitcode = locals['exitcode']
        else:
            raise RuntimeError('Please set either VASP_COMMAND or VASP_SCRIPT environment variable')
        sys.stderr = stderr
        if exitcode != 0:
            raise RuntimeError('Vasp exited with exit code: %d.  ' % exitcode)
        
    def restart_load(self):
        """Method which is called upon restart."""
        
        # Try to read sorting file
        if os.path.isfile('ase-sort.dat'):
            self.sort = []
            self.resort = []
            file = open('ase-sort.dat', 'r')
            lines = file.readlines()
            file.close()
            for line in lines:
                data = line.split()
                self.sort.append(int(data[0]))
                self.resort.append(int(data[1]))
            self.atoms = ase.io.read('CONTCAR', format='vasp')[self.resort]
        else:
            self.atoms = ase.io.read('CONTCAR', format='vasp')
            self.sort = range(len(self.atoms))
            self.resort = range(len(self.atoms))
        self.read_incar()
        self.read_outcar()
        self.read_kpoints()
        self.old_incar_parameters = self.incar_parameters.copy()
        self.old_input_parameters = self.input_parameters.copy()
        self.converged = self.read_convergence()

    def clean(self):
        """Method which cleans up after a calculation.
        
        The default files generated by Vasp will be deleted IF this
        method is called.

        """
        files = ['CHG', 'CHGCAR', 'POSCAR', 'INCAR', 'CONTCAR', 'DOSCAR',
                 'EIGENVAL', 'IBZKPT', 'KPOINTS', 'OSZICAR', 'OUTCAR', 'PCDAT',
                 'POTCAR', 'vasprun.xml', 'WAVECAR', 'XDATCAR',
                 'PROCAR', 'ase-sort.dat']
        for f in files:
            try:
                os.remove(f)
            except OSError:
                pass

    def set_atoms(self, atoms):
        if (atoms != self.atoms):
            self.converged = None
        self.atoms = atoms.copy()

    def get_atoms(self):
        atoms = self.atoms.copy()
        atoms.set_calculator(self)
        return atoms

    def get_potential_energy(self, atoms, force_consistent=False):
        self.update(atoms)
        if force_consistent:
            return self.energy_free
        else:
            return self.energy_zero

    def get_forces(self, atoms):
        self.update(atoms)
        return self.forces

    def get_stress(self, atoms):
        self.update(atoms)
        return self.stress

    def read_stress(self):
        for line in open('OUTCAR'):
            if line.find(' Total  ') != -1:
                stress = np.array([float(a) for a in line.split()[1:]])[[0, 1, 2, 4, 5, 3]]
        return stress

    def calculation_required(self, atoms, quantities):
        if (self.atoms != atoms
            or (self.incar_parameters != self.old_incar_parameters)
            or (self.input_parameters != self.old_input_parameters)
            or not self.converged):
            return True
        if 'magmom' in quantities:
            return not hasattr(self, 'magnetic_moment')
        return False

    def get_number_of_bands(self):
        return self.nbands

    def get_k_point_weights(self):
        self.update(self.atoms)
        return self.read_k_point_weights()

    def get_number_of_spins(self):
        return 1 + int(self.spinpol)

    def get_eigenvalues(self, kpt=0, spin=0):
        self.update(self.atoms)
        return self.read_eigenvalues(kpt, spin)

    def get_fermi_level(self):
        return self.fermi

    def get_number_of_grid_points(self):
        raise NotImplementedError

    def get_pseudo_density(self):
        raise NotImplementedError

    def get_pseudo_wavefunction(self, n=0, k=0, s=0, pad=True):
        raise NotImplementedError

    def get_bz_k_points(self):
        raise NotImplementedError

    def get_ibz_kpoints(self):
        self.update(self.atoms)
        return self.read_ibz_kpoints()

    def get_spin_polarized(self):
        if not hasattr(self, 'spinpol'):
            self.spinpol = self.atoms.get_initial_magnetic_moments().any()
        return self.spinpol            

    def get_magnetic_moment(self, atoms):
        self.update(atoms)
        return self.magnetic_moment
        
    def get_magnetic_moments(self, atoms):
        p=self.incar_parameters
        if p['lorbit']>=10 or p['rwigs']:
            self.update(atoms)
            return self.magnetic_moments
        else:
            raise RuntimeError(
                "The combination %s for lorbit with %s for rwigs not supported to calculate magnetic moments" % (p['lorbit'], p['rwigs']))

    def get_dipole_moment(self, atoms):
        """Returns total dipole moment of the system."""
        self.update(atoms)
        return self.dipole

    def get_number_of_bands(self):
        return self.nbands

    def get_xc_functional(self):
        return self.input_parameters['xc']

    def write_incar(self, atoms, **kwargs):
        """Writes the INCAR file."""
        p = self.incar_parameters
        incar = open('INCAR', 'w')
        incar.write('INCAR created by Atomic Simulation Environment\n')
        for key, val in p.items():
            if val is not None:
                incar.write(' '+key.upper()+' = ')
                # special cases:
                if key in ('dipol', 'eint'):
                    [incar.write('%.4f ' % x) for x in val]
                elif key in ('iband', 'kpuse'):
                    [incar.write('%i ' % x) for x in val]
                elif key == 'rwigs':
                    [incar.write('%.4f ' % rwigs) for rwigs in val]
                    if len(val) != len(self.symbol_count):
                        raise RuntimeError('Incorrect number of magnetic moments')
                else:
                    if type(val)==type(bool()):
                        if val:
                            incar.write('.TRUE.')
                        else:
                            incar.write('.FALSE.')
                    else:
                        incar.write('%s' % p[key])
                incar.write('\n')
        if self.spinpol and not p['ispin']:
            incar.write(' ispin = 2\n'.upper())
            # Write out initial magnetic moments
            magmom = atoms.get_initial_magnetic_moments()[self.sort]
            list = [[1, magmom[0]]]
            for n in range(1, len(magmom)):
                if magmom[n] == magmom[n-1]:
                    list[-1][0] += 1
                else:
                    list.append([1, magmom[n]])
            incar.write(' magmom = '.upper())
            [incar.write('%i*%.4f ' % (mom[0], mom[1])) for mom in list]
            incar.write('\n')
        incar.close()

    def write_kpoints(self, **kwargs):
        """Writes the KPOINTS file."""
        p = self.input_parameters
        kpoints = open('KPOINTS', 'w')
        kpoints.write('KPOINTS created by Atomic Simulation Environemnt\n')
        shape=np.array(p['kpts']).shape
        if len(shape)==1:
            kpoints.write('0\n')
            if p['gamma']:
                kpoints.write('Gamma\n')
            else:
                kpoints.write('Monkhorst-Pack\n')
            [kpoints.write('%i ' % kpt) for kpt in p['kpts']]
            kpoints.write('\n0 0 0')
        elif len(shape)==2:
            kpoints.write('%i \n' % (len(p['kpts'])))
            kpoints.write('Cartesian\n')
            for n in range(len(p['kpts'])):
                [kpoints.write('%f ' % kpt) for kpt in p['kpts'][n]]
                if shape[1]==4:
                    kpoints.write('\n')
                elif shape[1]==3:
                    kpoints.write('1.0 \n')
        kpoints.close()

    def write_potcar(self):
        """Writes the POTCAR file."""
        import tempfile
        potfile = open('POTCAR','w')
        for filename in self.ppp_list:
            if filename.endswith('R'):
                for line in open(filename, 'r'):
                    potfile.write(line)
            elif filename.endswith('.Z'):
                file_tmp = tempfile.NamedTemporaryFile()
                os.system('gunzip -c %s > %s' % (filename, file_tmp.name))
                for line in file_tmp.readlines():
                    potfile.write(line)
                file_tmp.close()
        potfile.close()

    def write_sort_file(self):
        """Writes a sortings file.

        This file contains information about how the atoms are sorted in
        the first column and how they should be resorted in the second
        column. It is used for restart purposes to get sorting right
        when reading in an old calculation to ASE."""

        file = open('ase-sort.dat', 'w')
        for n in range(len(self.sort)):
            file.write('%5i %5i \n' % (self.sort[n], self.resort[n]))

    # Methods for reading information from OUTCAR files:
    def read_energy(self, all=None):
        [energy_free, energy_zero]=[0, 0]
        if all:
            energy_free = []
            energy_zero = []
        for line in open('OUTCAR', 'r'):
            # Free energy
            if line.startswith('  free  energy   toten'):
                if all:
                    energy_free.append(float(line.split()[-2]))
                else:
                    energy_free = float(line.split()[-2])
            # Extrapolated zero point energy
            if line.startswith('  energy  without entropy'):
                if all:
                    energy_zero.append(float(line.split()[-1]))
                else:
                    energy_zero = float(line.split()[-1])
        return [energy_free, energy_zero]

    def read_forces(self, atoms, all=False):
        """Method that reads forces from OUTCAR file.

        If 'all' is switched on, the forces for all ionic steps
        in the OUTCAR file be returned, in other case only the
        forces for the last ionic configuration is returned."""

        file = open('OUTCAR','r')
        lines = file.readlines()
        file.close()
        n=0
        if all:
            all_forces = []
        for line in lines:
            if line.rfind('TOTAL-FORCE') > -1:
                forces=[]
                for i in range(len(atoms)):
                    forces.append(np.array([float(f) for f in lines[n+2+i].split()[3:6]]))
                if all:
                    all_forces.append(np.array(forces)[self.resort])
            n+=1
        if all:
            return np.array(all_forces)
        else:
            return np.array(forces)[self.resort]

    def read_fermi(self):
        """Method that reads Fermi energy from OUTCAR file"""
        E_f=None
        for line in open('OUTCAR', 'r'):
            if line.rfind('E-fermi') > -1:
                E_f=float(line.split()[2])
        return E_f

    def read_dipole(self):
        dipolemoment=np.zeros([1,3])
        for line in open('OUTCAR', 'r'):
            if line.rfind('dipolmoment') > -1:
                dipolemoment=np.array([float(f) for f in line.split()[1:4]])
        return dipolemoment

    def read_magnetic_moments(self, atoms):
        magnetic_moments = np.zeros(len(atoms))
        n = 0
        lines = open('OUTCAR', 'r').readlines()
        for line in lines:
            if line.rfind('magnetization (x)') > -1:
                for m in range(len(atoms)):
                    magnetic_moments[m] = float(lines[n + m + 4].split()[4])
            n += 1
        return np.array(magnetic_moments)[self.resort]

    def read_magnetic_moment(self):
        n=0
        for line in open('OUTCAR','r'):
            if line.rfind('number of electron  ') > -1:
                magnetic_moment=float(line.split()[-1])
            n+=1
        return magnetic_moment

    def read_nbands(self):
        for line in open('OUTCAR', 'r'):
            if line.rfind('NBANDS') > -1:
                return int(line.split()[-1])

    def read_convergence(self):
        """Method that checks whether a calculation has converged."""
        converged = None
        # First check electronic convergence
        for line in open('OUTCAR', 'r'):
            if line.rfind('EDIFF  ') > -1:
                ediff = float(line.split()[2])
            if line.rfind('total energy-change')>-1:
                split = line.split(':')
                a = float(split[1].split('(')[0])
                b = float(split[1].split('(')[1][0:-2])
                if [abs(a), abs(b)] < [ediff, ediff]:
                    converged = True
                else:
                    converged = False
                    continue
        # Then if ibrion > 0, check whether ionic relaxation condition been fulfilled
        if self.incar_parameters['ibrion'] > 0:
            ediffg = self.incar_parameters['ediffg']
            if ediffg < 0:
                for force in self.forces:
                    if np.linalg.norm(force)>=abs(ediffg):
                        converged = False
                        continue
                    else:
                        converged = True
            elif self.incar_parameters['ediffg'] > 0:
                raise NotImplementedError('Method not implemented for ediffg>0')
        return converged

    def read_ibz_kpoints(self):
        lines = open('OUTCAR', 'r').readlines()
        ibz_kpts = []
        n = 0
        i = 0
        for line in lines:
            if line.rfind('Following cartesian coordinates')>-1:
                m = n+2
                while i==0:
                    ibz_kpts.append([float(lines[m].split()[p]) for p in range(3)])
                    m += 1
                    if lines[m]==' \n':
                        i = 1
            if i == 1:
                continue
            n += 1
        ibz_kpts = np.array(ibz_kpts)
        return np.array(ibz_kpts)

    def read_k_point_weights(self):
        file = open('IBZKPT')
        lines = file.readlines()
        file.close()
        kpt_weights = []
        for n in range(3, len(lines)):
            kpt_weights.append(float(lines[n].split()[3]))
        kpt_weights = np.array(kpt_weights)
        kpt_weights /= np.sum(kpt_weights)
        return kpt_weights

    def read_eigenvalues(self, kpt=0, spin=0):
        file = open('EIGENVAL', 'r')
        lines = file.readlines()
        file.close()
        eigs = []
        for n in range(8+kpt*(self.nbands+2), 8+kpt*(self.nbands+2)+self.nbands):
            eigs.append(float(lines[n].split()[spin+1]))
        return np.array(eigs)

# The below functions are used to restart a calculation and are under early constructions

    def read_incar(self, filename='INCAR'):
        file=open(filename, 'r')
        file.readline()
        lines=file.readlines()
        for line in lines:
            try:
                key = line.split()[0].lower()
                if key in ['ispin', 'magmom']:
                    continue
                self.incar_parameters[key]
                if key=='dipol':
                    dipol=[]
                    for n in range(3):
                        dipol.append(float(line.split()[n+2]))
                    self.incar_parameters[key] = dipol
                else:
                    try:
                        self.incar_parameters[key] = int(line.split()[2])
                    except ValueError:
                        try:
                            self.incar_parameters[key] = float(line.split()[2])
                        except ValueError:
                            self.incar_parameters[key] = line.split()[2]
            except KeyError:
                continue
            except IndexError:
                continue

    def read_outcar(self):
        # Spin polarized calculation?
        file = open('OUTCAR', 'r')
        lines = file.readlines()
        file.close()
        for line in lines:
            if line.rfind('ISPIN') > -1:
                if int(line.split()[2])==2:
                    self.spinpol = True
                else:
                    self.spinpol = None
        self.energy_free, self.energy_zero = self.read_energy()
        self.forces = self.read_forces(self.atoms)
        self.dipole = self.read_dipole()
        self.fermi = self.read_fermi()
        self.nbands = self.read_nbands()
        p=self.incar_parameters
        if self.spinpol:
            self.magnetic_moment = self.read_magnetic_moment()
            if p['lorbit']>=10 or (p['lorbit']!=None and p['rwigs']):
                self.magnetic_moments = self.read_magnetic_moments(self.atoms)
        self.set(nbands=self.nbands)

    def read_kpoints(self, filename='KPOINTS'):
        file = open(filename, 'r')
        lines = file.readlines()
        file.close()
        type = lines[2].split()[0].lower()[0]
        if type in ['g', 'm']:
            if type=='g':
                self.set(gamma=True)
            kpts = np.array([int(lines[3].split()[i]) for i in range(3)])
            self.set(kpts=kpts)
        elif type in ['c', 'k']:
            raise NotImplementedError('Only Monkhorst-Pack and gamma centered grid supported for restart.')
        else:
            raise NotImplementedError('Only Monkhorst-Pack and gamma centered grid supported for restart.')


class VaspChargeDensity(object):
    """Class for representing VASP charge density"""

    def __init__(self, filename='CHG'):
        # Instance variables
        self.atoms = []   # List of Atoms objects
        self.chg = []     # Charge density
        self.chgdiff = [] # Charge density difference, if spin polarized
        self.aug = ''     # Augmentation charges, not parsed just a big string
        self.augdiff = '' # Augmentation charge differece, is spin polarized
        
        # Note that the augmentation charge is not a list, since they
        # are needed only for CHGCAR files which store only a single
        # image.
        if filename != None:
            self.read(filename)

    def is_spin_polarized(self):
        if len(self.chgdiff) > 0:
            return True
        return False

    def _read_chg(self, fobj, chg, volume):
        """Read charge from file object

        Utility method for reading the actual charge density (or
        charge density difference) from a file object. On input, the
        file object must be at the beginning of the charge block, on
        output the file position will be left at the end of the
        block. The chg array must be of the correct dimensions.

        """
        # VASP writes charge density as
        # WRITE(IU,FORM) (((C(NX,NY,NZ),NX=1,NGXC),NY=1,NGYZ),NZ=1,NGZC)
        # Fortran nested implied do loops; innermost index fastest
        # First, just read it in
        for zz in range(chg.shape[2]):
            for yy in range(chg.shape[1]):
                chg[:, yy, zz] = np.fromfile(fobj, count = chg.shape[0],
                                             sep=' ')
        chg /= volume

    def read(self, filename='CHG'):
        """Read CHG or CHGCAR file.

        If CHG contains charge density from multiple steps all the
        steps are read and stored in the object. By default VASP
        writes out the charge density every 10 steps.

        chgdiff is the difference between the spin up charge density
        and the spin down charge density and is thus only read for a
        spin-polarized calculation.

        aug is the PAW augmentation charges found in CHGCAR. These are
        not parsed, they are just stored as a string so that they can
        be written again to a CHGCAR format file.

        """
        import ase.io.vasp as aiv
        f = open(filename)
        self.atoms = []
        self.chg = []
        self.chgdiff = []
        self.aug = ''
        self.augdiff = ''
        while True:
            try:
                atoms = aiv.read_vasp(f)
            except ValueError, e:
                # Probably an empty line, or we tried to read the 
                # augmentation occupancies in CHGCAR
                break 
            f.readline()
            ngr = f.readline().split()
            ng = (int(ngr[0]), int(ngr[1]), int(ngr[2]))
            chg = np.empty(ng)
            self._read_chg(f, chg, atoms.get_volume())
            self.chg.append(chg)
            self.atoms.append(atoms)
            # Check if the file has a spin-polarized charge density part, and
            # if so, read it in.
            fl = f.tell()
            # First check if the file has an augmentation charge part (CHGCAR file.)
            line1 = f.readline()
            if line1=='':
                break
            elif line1.find('augmentation') != -1:
                augs = [line1]
                while True:
                    line2 = f.readline()
                    if line2.split() == ngr:
                        self.aug = ''.join(augs)
                        augs = []
                        chgdiff = np.empty(ng)
                        self._read_chg(f, chgdiff, atoms.get_volume())
                        self.chgdiff.append(chgdiff)
                    elif line2 == '':
                        break
                    else:
                        augs.append(line2)
                if len(self.aug) == 0:
                    self.aug = ''.join(augs)
                    augs = []
                else:
                    self.augdiff = ''.join(augs)
                    augs = []
            elif line1.split() == ngr:
                chgdiff = np.empty(ng)
                self._read_chg(f, chgdiff, atoms.get_volume())
                self.chgdiff.append(chgdiff)
            else:
                f.seek(fl)
        f.close()

    def _write_chg(self, fobj, chg, volume, format='chg'):
        """Write charge density

        Utility function similar to _read_chg but for writing.

        """
        # Make a 1D copy of chg, must take transpose to get ordering right
        chgtmp=chg.T.ravel()
        # Multiply by volume
        chgtmp=chgtmp*volume
        # Must be a tuple to pass to string conversion
        chgtmp=tuple(chgtmp)
        # CHG format - 10 columns
        if format.lower() == 'chg':
            # Write all but the last row
            for ii in range((len(chgtmp)-1)/10):
                fobj.write(' %#11.5G %#11.5G %#11.5G %#11.5G %#11.5G\
 %#11.5G %#11.5G %#11.5G %#11.5G %#11.5G\n' % chgtmp[ii*10:(ii+1)*10]
                           )
            # If the last row contains 10 values then write them without a newline
            if len(chgtmp)%10==0:
                fobj.write(' %#11.5G %#11.5G %#11.5G %#11.5G %#11.5G\
 %#11.5G %#11.5G %#11.5G %#11.5G %#11.5G' % chgtmp[len(chgtmp)-10:len(chgtmp)])
            # Otherwise write fewer columns without a newline
            else:
                for ii in range(len(chgtmp)%10):
                    fobj.write((' %#11.5G') % chgtmp[len(chgtmp)-len(chgtmp)%10+ii])
        # Other formats - 5 columns
        else:
            # Write all but the last row
            for ii in range((len(chgtmp)-1)/5):
                fobj.write(' %17.10E %17.10E %17.10E %17.10E %17.10E\n' % chgtmp[ii*5:(ii+1)*5])
            # If the last row contains 5 values then write them without a newline
            if len(chgtmp)%5==0:
                fobj.write(' %17.10E %17.10E %17.10E %17.10E %17.10E' % chgtmp[len(chgtmp)-5:len(chgtmp)])
            # Otherwise write fewer columns without a newline
            else:
                for ii in range(len(chgtmp)%5):
                    fobj.write((' %17.10E') % chgtmp[len(chgtmp)-len(chgtmp)%5+ii])
        # Write a newline whatever format it is
        fobj.write('\n')
        # Clean up
        del chgtmp

    def write(self, filename='CHG', format=None):
        """Write VASP charge density in CHG format.

        filename: str
            Name of file to write to.
        format: str
            String specifying whether to write in CHGCAR or CHG
            format.

        """
        import ase.io.vasp as aiv
        if format == None:
            if filename.lower().find('chgcar') != -1:
                format = 'chgcar'
            elif filename.lower().find('chg') != -1:
                format = 'chg'
            elif len(self.chg) == 1:
                format = 'chgcar'
            else:
                format = 'chg'
        f = open(filename, 'w')
        for ii, chg in enumerate(self.chg):
            if format == 'chgcar' and ii != len(self.chg) - 1:
                continue # Write only the last image for CHGCAR
            aiv.write_vasp(f, self.atoms[ii], direct=True)
            f.write('\n')
            for dim in chg.shape:
                f.write(' %4i' % dim)
            f.write('\n')
            vol = self.atoms[ii].get_volume()
            self._write_chg(f, chg, vol, format)
            if format == 'chgcar':
                f.write(self.aug)
            if self.is_spin_polarized():
                if format == 'chg':
                    f.write('\n')
                for dim in chg.shape:
                    f.write(' %4i' % dim)
                self._write_chg(f, self.chgdiff[ii], vol, format)
                if format == 'chgcar':
                    f.write('\n')
                    f.write(self.augdiff)
            if format == 'chg' and len(self.chg) > 1:
                f.write('\n')
        f.close()


class VaspDos(object):
    """Class for representing density-of-states produced by VASP

    The energies are in property self.energy

    Site-projected DOS is accesible via the self.site_dos method.

    Total and integrated DOS is accessible as numpy.ndarray's in the
    properties self.dos and self.integrated_dos. If the calculation is
    spin polarized, the arrays will be of shape (2, NDOS), else (1,
    NDOS).

    The self.efermi property contains the currently set Fermi
    level. Changing this value shifts the energies.
    
    """

    def __init__(self, doscar='DOSCAR', efermi=0.0):
        """Initialize"""
        self._efermi = 0.0
        self.read_doscar(doscar)
        self.efermi = efermi

    def _set_efermi(self, efermi):
        """Set the Fermi level."""
        ef = efermi - self._efermi
        self._efermi = efermi
        self._total_dos[0, :] = self._total_dos[0, :] - ef
        try:
            self._site_dos[:, 0, :] = self._site_dos[:, 0, :] - ef
        except IndexError:
            pass

    def _get_efermi(self):
        return self._efermi

    efermi = property(_get_efermi, _set_efermi, None, "Fermi energy.")

    def _get_energy(self):
        """Return the array with the energies."""
        return self._total_dos[0, :]
    energy = property(_get_energy, None, None, "Array of energies")

    def site_dos(self, atom, orbital):
        """Return an NDOSx1 array with dos for the chosen atom and orbital.

        atom: int
            Atom index
        orbital: int or str
            Which orbital to plot

        If the orbital is given as an integer:
        If spin-unpolarized calculation, no phase factors:
        s = 0, p = 1, d = 2
        Spin-polarized, no phase factors:
        s-up = 0, s-down = 1, p-up = 2, p-down = 3, d-up = 4, d-down = 5
        If phase factors have been calculated, orbitals are
        s, py, pz, px, dxy, dyz, dz2, dxz, dx2
        double in the above fashion if spin polarized.

        """
        # Integer indexing for orbitals starts from 1 in the _site_dos array
        # since the 0th column contains the energies
        if isinstance(orbital, int):
            return self._site_dos[atom, orbital + 1, :]
        n = self._site_dos.shape[1]
        if n == 4:
            norb = {'s':1, 'p':2, 'd':3}
        elif n == 7:
            norb = {'s+':1, 's-up':1, 's-':2, 's-down':2,
                    'p+':3, 'p-up':3, 'p-':4, 'p-down':4,
                    'd+':5, 'd-up':5, 'd-':6, 'd-down':6}
        elif n == 10:
            norb = {'s':1, 'py':2, 'pz':3, 'px':4,
                    'dxy':5, 'dyz':6, 'dz2':7, 'dxz':8,
                    'dx2':9}
        elif n == 19:
            norb = {'s+':1, 's-up':1, 's-':2, 's-down':2,
                    'py+':3, 'py-up':3, 'py-':4, 'py-down':4,
                    'pz+':5, 'pz-up':5, 'pz-':6, 'pz-down':6,
                    'px+':7, 'px-up':7, 'px-':8, 'px-down':8,
                    'dxy+':9, 'dxy-up':9, 'dxy-':10, 'dxy-down':10,
                    'dyz+':11, 'dyz-up':11, 'dyz-':12, 'dyz-down':12,
                    'dz2+':13, 'dz2-up':13, 'dz2-':14, 'dz2-down':14,
                    'dxz+':15, 'dxz-up':15, 'dxz-':16, 'dxz-down':16,
                    'dx2+':17, 'dx2-up':17, 'dx2-':18, 'dx2-down':18}
        return self._site_dos[atom, norb[orbital.lower()], :]

    def _get_dos(self):
        if self._total_dos.shape[0] == 3:
            return self._total_dos[1, :]
        elif self._total_dos.shape[0] == 5:
            return self._total_dos[1:3, :]
    dos = property(_get_dos, None, None, 'Average DOS in cell')

    def _get_integrated_dos(self):
        if self._total_dos.shape[0] == 3:
            return self._total_dos[2, :]
        elif self._total_dos.shape[0] == 5:
            return self._total_dos[3:5, :]
    integrated_dos = property(_get_integrated_dos, None, None,
                              'Integrated average DOS in cell')

    def read_doscar(self, fname="DOSCAR"):
        """Read a VASP DOSCAR file"""
        f = open(fname)
        natoms = int(f.readline().split()[0])
        [f.readline() for nn in range(4)]  # Skip next 4 lines.
        # First we have a block with total and total integrated DOS
        ndos = int(f.readline().split()[2])
        dos = []
        for nd in xrange(ndos):
            dos.append(np.array([float(x) for x in f.readline().split()]))
        self._total_dos = np.array(dos).T
        # Next we have one block per atom, if INCAR contains the stuff
        # necessary for generating site-projected DOS
        dos = []
        for na in xrange(natoms):
            line = f.readline()
            if line == '':
                # No site-projected DOS
                break
            ndos = int(line.split()[2])
            line = f.readline().split()
            cdos = np.empty((ndos, len(line)))
            cdos[0] = np.array(line)
            for nd in xrange(1, ndos):
                line = f.readline().split()
                cdos[nd] = np.array([float(x) for x in line])
            dos.append(cdos.T)
        self._site_dos = np.array(dos)


import pickle

class xdat2traj:
    def __init__(self, trajectory=None, atoms=None, poscar=None, 
                 xdatcar=None, sort=None, calc=None):
        if not poscar:
            self.poscar = 'POSCAR'
        else:
            self.poscar = poscar
        if not atoms:
            self.atoms = ase.io.read(self.poscar, format='vasp')
        else:
            self.atoms = atoms
        if not xdatcar:
            self.xdatcar = 'XDATCAR'
        else:
            self.xdatcar = xdatcar
        if not trajectory:
            self.trajectory = 'out.traj'
        else:
            self.trajectory = trajectory
        if not calc:
            self.calc = Vasp()
        else:
            self.calc = calc
        if not sort: 
            if not hasattr(self.calc, 'sort'):
                self.calc.sort = range(len(self.atoms))
        else:
            self.calc.sort = sort
        self.calc.resort = range(len(self.calc.sort))
        for n in range(len(self.calc.resort)):
            self.calc.resort[self.calc.sort[n]] = n
        self.out = ase.io.trajectory.PickleTrajectory(self.trajectory, mode='w')
        self.energies = self.calc.read_energy(all=True)[1]
        self.forces = self.calc.read_forces(self.atoms, all=True)

    def convert(self):
        lines = open(self.xdatcar).readlines()

        del(lines[0:6])
        step = 0
        iatom = 0
        scaled_pos = []
        for line in lines:
            if iatom == len(self.atoms):
                if step == 0:
                    self.out.write_header(self.atoms[self.calc.resort])
                scaled_pos = np.array(scaled_pos)
                self.atoms.set_scaled_positions(scaled_pos)
                d = {'positions': self.atoms.get_positions()[self.calc.resort],
                     'cell': self.atoms.get_cell(),
                     'momenta': None,
                     'energy': self.energies[step],
                     'forces': self.forces[step],
                     'stress': None}
                pickle.dump(d, self.out.fd, protocol=-1)
                scaled_pos = []
                iatom = 0
                step += 1
            else:
                
                iatom += 1
                scaled_pos.append([float(line.split()[n]) for n in range(3)])

        # Write also the last image
        # I'm sure there is also more clever fix...
        scaled_pos = np.array(scaled_pos)
        self.atoms.set_scaled_positions(scaled_pos)
        d = {'positions': self.atoms.get_positions()[self.calc.resort],
             'cell': self.atoms.get_cell(),
             'momenta': None,
             'energy': self.energies[step],
             'forces': self.forces[step],
             'stress': None}
        pickle.dump(d, self.out.fd, protocol=-1)

        self.out.fd.close()
