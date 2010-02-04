'''
python module for ASE2 and Numeric free dacapo

U{John Kitchin<mailto:jkitchin@andrew.cmu.edu>} December 25, 2008

This module supports numpy directly.

* ScientificPython2.8 is required

 - this is the first version to use numpy by default.

see https://wiki.fysik.dtu.dk/stuff/nc/ for dacapo netcdf variable
documentation
'''

__docformat__ = 'restructuredtext'

import exceptions, glob, os, pickle, string, time
from Scientific.IO.NetCDF import NetCDFFile as netCDF
import numpy as np
import subprocess as sp

class DacapoRunning(exceptions.Exception):
    def __init__(self,value):
        self.parameter = value

class DacapoAborted(exceptions.Exception):
    def __init__(self,value):
        self.parameter = value

def read(ncfile):
    '''return atoms and calculator from ncfile

    >>> atoms, calc = read('co.nc')
    '''
    calc = Jacapo(ncfile)
    atoms = calc.get_atoms() #this returns a copy
    return (atoms,calc)

class Jacapo:
    
    def __init__(self,
                 nc='out.nc',
                 outnc=None,
                 atoms=None,
                 pw=None,
                 dw=None,
                 xc=None,
                 nbands=None,
                 ft=None,
                 kpts=None,
                 spinpol=None,
                 fixmagmom=None,
                 symmetry=None,
                 stress=None,
                 dipole=None,
                 stay_alive=None,
                 debug=0):
        '''
        Initialize the Jacapo calculator

        do not set default parameters here, as they will cause the calculator
        to reset.

        :Parameters:

          nc : string
           output netcdf file, or input file if nc already exists

          outnc : string
           output file. by default equal to nc 

          atoms : ASE.Atoms instance
            atoms is an ase.Atoms object that will be attached
            to this calculator.
        
          pw : integer
            sets planewave cutoff

          dw : integer
            sets density cutoff

          kpts : iterable
            set monkhorst-pack kpt grid, e.g. kpts = (2,2,1)

          spinpol : Boolean
            sets whether spin-polarization is used or not.

          fixmagmom : float
            set the magnetic moment of the unit cell. only used
            in spin polarize calculations

          ft : float
            set the Fermi temperature used in occupation smearing

          xc : string
            set the exchange-correlation functional.
            one of ['PZ','VWN','PW91','PBE','RPBE','revPBE'],

          dipole
            boolean
            turn the dipole correction on (True) or off (False)

          nbands : integer
            set the number of bands

          symmetry : Boolean
            Turn symmetry reduction on (True) or off (False)

          stress : Boolean
            Turn stress calculation on (True) or off (False)

          debug : integer
            set debug level (0 = off, 10 = extreme)
        
        Modification of the nc file is immediate on setting the
        parameter, and in most cases will require a new calculation to
        get data.

        >>> calc = Jacapo('CO.nc')
        
        reads the calculator from CO.nc if it exists or
        minimally initializes CO.nc with dimensions if it does not exist. 

        >>> calc = Jacapo('CO.nc', pw=300)

        reads the calculator from CO.nc or initializes it if
        it does not exist and changes the planewave cutoff energy to
        300eV

        >>> calc = Jacapo('CO.nc',outnc='new.nc', pw=300)

        reads the calculator from CO.nc, copies it to
        new.nc, and then changes the planewave cutoff in new.nc to
        300eV. CO.nc should be unchanged. if new.nc exists it will be
        clobbered by CO.nc

        >>> co2 = Atoms(...)
        >>> calc = Jacapo('CO.nc',atoms=co2)

        will use the calculator from 'CO.nc' to get properties of the
        atoms co2. equivalent to: co2.set_calculator(Jacapo('CO.nc'))

        >>> calc = Jacapo('CO.nc', pw=300)
        >>> calc.set_nc('new.nc')
        
        reads the atoms and calculator from CO.nc and changes the
        planewave cutoff energy to 300eV. Then copies the calculator
        and atoms into new.nc. This probably is not what you would
        normally want to do.

        >>> calc = Jacapo('CO.nc', outnc='new.nc', pw=300)

        copies the calculator and atoms from CO.nc to new.nc then changes teh pw
        cutoff in new.nc to 300.
        
        >>> atoms = Jacapo.read_atoms('CO.nc')

        returns the atoms in the netcdffile CO.nc, with the calculator
        attached to it.

        >>> atoms, calc = read('CO.nc')
        
        '''

        if (atoms is None and
            pw is None and
            dw is None and
            xc is None and
            nbands is None and
            ft is None and
            kpts is None and
            spinpol is None and
            fixmagmom is None and
            symmetry is None and
            stress is None
            and dipole is None):
            
            if not os.path.exists(nc):
                raise Exception('you asked for %s, and did not define any parameters, but %s does not exist.' % (nc,nc))
        
        self.set_nc(nc)
        
        self.debug = debug

        #assume not ready at init, rely on code later to change this 
        self.ready = False  

        # we have to set the psp database first before any chance
        # atoms are written. Eventually this has to be revisited when
        # there are multiple xc databases to choose from.
        self.set_psp_database()

        # need to set a default value for stay_alive
        self.stay_alive = False
        
        # Jacapo('out.nc') should return a calculator with atoms in
        # out.nc attached or initialize out.nc
        if os.path.exists(nc):

            # for correct updating, we need to set the correct frame number
            # before setting atoms or calculator

            #it is possible to get an error here from an incomplete
            #ncfile if it exists. the only solution so far is to
            #delete the bad file

            self._set_frame_number()

            #we are reading atoms in, so there is no reason to write them back out.
            #we do not use self.set_atoms() to avoid writing the atoms back out.
            self.atoms = self.read_only_atoms(nc)
            #self.set_atoms(self.read_only_atoms(nc))

            #assume here if you read atoms in, it is ready.
            #later when calculation required is checked
            # a calculation will be run if required.
            self.ready = True
        else:
            self.initnc(nc)

        #change output file if needed
        if outnc:
            self.set_nc(outnc)

        #force atoms onto the calculator definition in nc
        #there is something buggy here if the atoms exists in the
        #netcdf file already
        if atoms:
            #self.set_atoms(atoms) #this needs self.psp to work!
            #print 'setting second atoms calculator'
            atoms.set_calculator(self)   # atoms.set_calculator() automatically calls calc.set_atoms()

        #all these methods require the ncfile to exist.
        if pw: self.set_pw(pw)
        if dw: self.set_dw(dw)
        if nbands: self.set_nbands(nbands)
        if kpts: self.set_kpts(kpts)
        if spinpol: self.set_spinpol(spinpol)
        if fixmagmom: self.set_fixmagmom(fixmagmom)
        if ft: self.set_ft(ft)
        if xc: self.set_xc(xc)
        if dipole: self.set_dipole()
        if symmetry: self.set_symmetry(symmetry)
        if stress: self.set_stress(stress)
        if stay_alive: self.set_stay_alive(stay_alive)
        
        
    def initnc(self,ncfile):
        'create an ncfile with minimal dimensions in it'
        if self.debug > 0: print 'initializing ',ncfile
        ncf = netCDF(ncfile,'w')
        #first, we define some dimensions we always need
        #unlimited
        dionsteps = ncf.createDimension('number_ionic_steps',None)
        d1 = ncf.createDimension('dim1',1)
        d2 = ncf.createDimension('dim2',2)
        d3 = ncf.createDimension('dim3',3)
        d4 = ncf.createDimension('dim4',4)
        d5 = ncf.createDimension('dim5',5)
        d6 = ncf.createDimension('dim6',6)
        d7 = ncf.createDimension('dim7',7)
        d20 = ncf.createDimension('dim20',20) #for longer strings
        ncf.status  = 'new'
        ncf.history = 'Dacapo'
        ncf.close()
        
        # Setting some default values to maintain consistency with ASE2 
        self.set_convergence()	# automatically sets the ASE2 default values
        self.set_ft(0.1) 	# default value from ASE2
        self.set_xc('PW91')	# default xc functional
        self.ready = False
        self._frame = 0
        self.set_nc(ncfile)

    def __del__(self):
        '''If calculator is deleted try to stop dacapo program
        '''
        if hasattr(self,'_dacapo'):
            if self._dacapo.poll()==None:
                self.execute_external_dynamics(stopprogram=True)
        #and clean up after Dacapo
        if os.path.exists('stop'): os.remove('stop')
        #remove slave files
        txt = self.get_txt()
        slv = txt + '.slave*'
        for slvf in glob.glob(slv):
            os.remove(slvf)
        
    def __str__(self):
        '''
        pretty-print the calculator and atoms.

        we read everything directly from the ncfile to prevent
        triggering any calculations
        '''
        import string
        s = []
        if self.nc is None:
            return 'No netcdf file attached to this calculator'
        nc = netCDF(self.nc,'r')
        s.append('  ---------------------------------')
        s.append('  Dacapo calculation from %s' % self.nc)
        if hasattr(nc,'status'):
            s.append('  status = %s' % nc.status)
        if hasattr(nc,'version'):
            s.append('  version = %s' % nc.version)
        
        energy = nc.variables.get('TotalEnergy',None)
        
        if energy and energy[:][-1] < 1E36:   # missing values get returned at 9.3E36
            s.append('  Energy = %1.6f eV' % energy[:][-1])
        else:
            s.append('  Energy = None')
            
        s.append('')
        
        atoms = self.get_atoms()
        
        if atoms is None:
            s.append('  no atoms defined')
        else:
            uc = atoms.get_cell()
            a,b,c = uc
            s.append("  Unit Cell vectors (angstroms)")
            s.append("         x        y       z   length")

            for i,v in enumerate(uc):
                L = (np.sum(v**2))**0.5 #vector length
                s.append("  a%i [% 1.4f % 1.4f % 1.4f] %1.2f" % (i,
                                                                 v[0],
                                                                 v[1],
                                                                 v[2],
                                                                 L))
                                                                 
            stress = nc.variables.get('TotalStress',None)
            if stress is not None:
                stress = np.take(stress[:].ravel(),[0,4,8,5,2,1])
                s.append('  Stress: xx,   yy,    zz,    yz,    xz,    xy')
                s1 = '       % 1.3f % 1.3f % 1.3f % 1.3f % 1.3f % 1.3f'
                s.append(s1 % tuple(stress))
            else:
                s.append('  No stress calculated.')
            s.append('  Volume = %1.2f A^3' % atoms.get_volume())
            s.append('')

            s1 = "  Atom,  sym, position (in x,y,z),     tag, rmsForce and psp"
            s.append(s1)

            #this is just the ncvariable
            forces = nc.variables.get('DynamicAtomForces',None)
           
            for i,atom in enumerate(atoms):
                sym = atom.get_symbol()
                pos = atom.get_position()
                tag = atom.get_tag()
                if forces is not None and (forces[:][-1][i] < 1E36).all():
                    f = forces[:][-1][i]
                    # Lars Grabow: this seems to work right for some reason,
                    # but I would expect this to be the right index order f=forces[-1][i][:]
                    # frame,atom,direction
                    rmsforce = (np.sum(f**2))**0.5
                else:
                    rmsforce = None
                    
                st = "  %2i   %3.12s  " % (i,sym)
                st += "[% 7.3f%7.3f% 7.3f] " % tuple(pos)
                st += " %2s  " % tag
                if rmsforce is not None:
                    st += " %4.3f " % rmsforce
                else:
                    st += ' None '
                st += " %s"  % (self.get_psp(sym))        
                s.append(st)
                
        s.append('')
        s.append('  Details:')
        xc = self.get_xc()
        if xc is not None:
            s.append('  XCfunctional        = %s' % self.get_xc())
        else:
            s.append('  XCfunctional        = Not defined')
        s.append('  Planewavecutoff     = %i eV' % self.get_pw())
        s.append('  Densitywavecutoff   = %i eV' % self.get_dw())
        ft = self.get_ft()
        if ft is not None:
            s.append('  FermiTemperature    = %f kT' % ft)
        else:
            s.append('  FermiTemperature    = not defined')
        nelectrons = self.get_valence()
        if nelectrons is not None:
            s.append('  Number of electrons = %1.1f'  % nelectrons)
        else:
            s.append('  Number of electrons = None')
        s.append('  Number of bands     = %s'  % self.get_nbands())
        s.append('  Kpoint grid         = %s' % str(self.get_kpts()))
        s.append('  Spin-polarized      = %s' % self.get_spin_polarized())
        s.append('  Dipole correction   = %s' % self.get_dipole())
        s.append('  Symmetry            = %s' % self.get_symmetry())
        s.append('  Constraints         = %s' % str(atoms._get_constraints()))
        s.append('  ---------------------------------')
        nc.close()
        return string.join(s,'\n')            

    def set_psp_database(self,xc=None):
        '''
        get the xc-dependent psp database

        :Parameters:

         xc : string
           one of 'PW91', 'PBE', 'revPBE', 'RPBE', 'PZ'

        
        not all the databases are complete, and that means
        some psp do not exist.

        note: this function is not supported fully. only pw91 is
        imported now. Changing the xc at this point results in loading
        a nearly empty database, and I have not thought about how to
        resolve that
        '''

        from pw91_psp import defaultpseudopotentials
        
        ##if xc is None:
##            if self.nc is None:
##                xc = 'PW91'
##            else:
##                xc = self.get_xc()
##                if xc is None: #in case it is not defined
##                    xc = 'PW91'
        
##        if xc == 'PW91':
##            from pw91_psp import defaultpseudopotentials
##        elif xc == 'PBE':
##            from pbe_psp import defaultpseudopotentials
##        elif xc == 'RPBE':
##            from pw91_psp import defaultpseudopotentials
##        elif xc == 'revPBE':
##            from pw91_psp import defaultpseudopotentials
##        elif xc == 'PZ':
##            from lda_psp import defaultpseudopotentials
##        else:
##            #default settings
##            from pw91_psp import defaultpseudopotentials

        self.psp = defaultpseudopotentials

        #update teh pspdatabase from the ncfile if it exists
        if os.path.exists(self.nc):
            nc = netCDF(self.nc,'r')
            sym = nc.variables['DynamicAtomSpecies'][:]
            symbols = [x.tostring().strip() for x in sym]
            for sym in symbols:
                vn = 'AtomProperty_%s' % sym
                if vn in nc.variables:
                    var = nc.variables[vn]
                    pspfile = var.PspotFile

                    self.psp[sym] = pspfile
            nc.close()

    def _set_frame_number(self,frame=None):
        if frame is None:
            nc = netCDF(self.nc,'r')
            if 'TotalEnergy' in nc.variables:
                frame = nc.variables['TotalEnergy'].shape[0]
                # make sure the last energy is reasonable. Sometime the field is empty if the
                # calculation ran out of walltime for example. Empty values get returned as 9.6E36.
                # Dacapos energies should always be negative, so if the energy is > 1E36, there is
                # definitely something wrong and a restart is required.
                if nc.variables.get('TotalEnergy',None)[-1] > 1E36:
                    if self.debug > 1:
                        print "NC file is incomplete. Restart required"
                    self.restart()
            else:
                frame = 1
            nc.close()
            if self.debug > 1:
                print "Current frame number is: ",frame-1
        self._frame = frame-1  #netCDF starts counting with 1

    def _increment_frame(self):
        self._frame += 1

    def set_pw(self,pw):
        '''set the planewave cutoff.

        :Parameters:

         pw : integer
           the planewave cutoff in eV
           
        this function checks to make sure the density wave cutoff is
        greater than or equal to the planewave cutoff.'''
        
        nc = netCDF(self.nc,'a')
        if 'PlaneWaveCutoff' in nc.variables:
            vpw = nc.variables['PlaneWaveCutoff']
            vpw.assignValue(pw)
        else:
            vpw = nc.createVariable('PlaneWaveCutoff','f',('dim1',))
            vpw.assignValue(pw)

        if 'Density_WaveCutoff' in nc.variables:
            vdw = nc.variables['Density_WaveCutoff']
            dw = vdw.getValue()
            if pw > dw:
                vdw.assignValue(pw) #make them equal
        else:
            vdw = nc.createVariable('Density_WaveCutoff','f',('dim1',))
            vdw.assignValue(pw) 
        nc.close()
        self.restart() #nc dimension change for number_plane_Wave dimension
        self.set_status('new')
        self.ready = False

    def set_dw(self,dw):
        '''set the density wave cutoff energy.

        :Parameters:

          dw : integer
            the density wave cutoff

        The function checks to make sure it is not less than the
        planewave cutoff.

        Density_WaveCutoff describes the kinetic energy neccesary to
        represent a wavefunction associated with the total density,
        i.e. G-vectors for which $\vert G\vert^2$ $<$
        4*Density_WaveCutoff will be used to describe the total
        density (including augmentation charge and partial core
        density). If Density_WaveCutoff is equal to PlaneWaveCutoff
        this implies that the total density is as soft as the
        wavefunctions described by the kinetic energy cutoff
        PlaneWaveCutoff. If a value of Density_WaveCutoff is specified
        (must be larger than or equal to PlaneWaveCutoff) the program
        will run using two grids, one for representing the
        wavefunction density (softgrid_dim) and one representing the
        total density (hardgrid_dim). If the density can be
        reprensented on the same grid as the wavefunction density
        Density_WaveCutoff can be chosen equal to PlaneWaveCutoff
        (default).
        '''

        pw = self.get_pw()
        if pw > dw:
            raise Exception('Planewave cutoff %i is greater than density cutoff %i' % (pw,dw))
        
        ncf = netCDF(self.nc,'a')
        if 'Density_WaveCutoff' in ncf.variables:
            vdw = ncf.variables['Density_WaveCutoff']
            vdw.assignValue(dw)
        else:
            vdw = ncf.createVariable('Density_WaveCutoff','i',('dim1',))
            vdw.assignValue(dw)
        ncf.close()
        self.restart() #nc dimension change
        self.set_status('new')
        self.ready = False

    def set_xc(self,xc):
        '''Set the self-consistent exchange-correlation functional

        :Parameters:

         xc : string
           Must be one of 'PZ', 'VWN', 'PW91', 'PBE', 'revPBE', 'RPBE'

        Selects which density functional to use for
        exchange-correlation when performing electronic minimization
        (the electronic energy is minimized with respect to this
        selected functional) Notice that the electronic energy is also
        evaluated non-selfconsistently by DACAPO for other
        exchange-correlation functionals Recognized options :

        * "PZ" (Perdew Zunger LDA-parametrization)
        * "VWN" (Vosko Wilk Nusair LDA-parametrization)
        * "PW91" (Perdew Wang 91 GGA-parametrization)
        * "PBE" (Perdew Burke Ernzerhof GGA-parametrization)
        * "revPBE" (revised PBE/1 GGA-parametrization)
        * "RPBE" (revised PBE/2 GGA-parametrization)

        option "PZ" is not allowed for spin polarized
        calculation; use "VWN" instead.
        '''
        nc = netCDF(self.nc,'a')
        v = 'ExcFunctional'
        if v in nc.variables:
            nc.variables[v][:] = np.array('%7s' % xc,'c') 
        else:
            vxc = nc.createVariable('ExcFunctional','c',('dim7',))
            vxc[:] = np.array('%7s' % xc,'c')
        nc.close()
        self.set_status('new')
        self.ready = False    
    
    def set_nbands(self,nbands=None):
        '''Set the number of bands. a few unoccupied bands are
        recommended.

        :Parameters:

          nbands : integer
            the number of bands.
            
        if nbands = None the number of bands is calculated as
        $nbands=nvalence*0.65 + 4$
        '''
        nc = netCDF(self.nc,'a')
        v = 'ElectronicBands'
        if v in nc.variables:
            vnb = nc.variables[v]
        else:
            vnb = nc.createVariable('ElectronicBands','c',('dim1',))

        if nbands is None:
            nbands = int(self.get_valence()*0.65 + 4)
            
        vnb.NumberOfBands = nbands
        nc.sync()
        nc.close()
        self.set_status('new')
        self.ready = False
        
    def set_kpts(self,kpts):
        '''
        set the monkhorst pack kpt grid.

        :Parameters:

         kpts : (n1,n2,n3) or [k1,k2,k3,...] 
           (n1,n2,n3) creates an n1 x n2 x n3 monkhorst-pack grid, [k1,k2,k3,...] creates a kpt-grid based on the kpoints in k1,k2,k3,... 
        
        eventually I would like to support::

          kpts=(3,3,3) #MonkhorstPack grid
          kpts=[[0,0,0],
                [0.5,0.5,0.5],
                ...
                ]  #explicit list of kpts
          kpts='cc-6-1x1' #ChadiCohen 6 kpts in 1x1 symmetry
          
        this code exists in the old ASE2 Dacapo directory: Kpoints.py
        '''

        if np.array(kpts).shape == (3,):
            N1,N2,N3 = kpts

            listofkpts = []
            for n1 in range(N1):
                kp1 = float(1+2*n1-N1)/(2*N1)
                for n2 in range(N2):
                    kp2=float(1+2*n2-N2)/(2*N2)
                    for n3 in range(N3):
                        kp3=float(1+2*n3-N3)/(2*N3)
                        fk = np.array([kp1,kp2,kp3])
                        listofkpts.append(fk)
            grid = kpts
        else:
            #this probably means a user-defined list is provided
            listofkpts = np.array(kpts,dtype=np.float32)
            grid = len(kpts)
                    
        nbzkpts = len(listofkpts)

        nc2 = netCDF(self.nc,'r')
        ncdims = nc2.dimensions
        ncvars = nc2.variables
        nc2.close()

        if 'number_BZ_kpoints' in ncdims:
            self.delete_ncattdimvar(self.nc,
                                    ncdims=['number_plane_waves',
                                            'number_BZ_kpoints',
                                            'number_IBZ_kpoints'])

        # now define dim and var
        nc = netCDF(self.nc,'a')
        d = nc.createDimension('number_BZ_kpoints',nbzkpts)
        bv = nc.createVariable('BZKpoints','f',('number_BZ_kpoints',
                                                 'dim3'))
        bv[:] = np.array(listofkpts,np.float32)
        bv.grid = grid
        nc.sync()
        nc.close()

        if self.debug > 0: print 'kpts = ',self.get_kpts()

        self.set_status('new')
        self.ready = False

    def set_atoms(self,atoms):
        '''attach an atoms to the calculator and update the ncfile

        :Parameters:

          atoms
            ASE.Atoms instance
          
        '''
        if hasattr(self,'atoms') and self.atoms is not None:
            #return if the atoms are the same. no change needs to be made
            #i am not sure if new constraints would make atoms unequal
            if atoms == self.atoms:
                return
            # some atoms already exist. Test if new atoms are different from old atoms.
            if atoms != self.atoms:
                # the new atoms are different from the old ones. Start a new frame.
                self._increment_frame()
        self.atoms = atoms.copy()
        self.write_nc(atoms=atoms) #this makes sure teh ncfile is up to date

        #store constraints if they exist
        constraints = atoms._get_constraints()
        if constraints != []:
            nc = netCDF(self.get_nc(),'a')
            if 'constraints' not in nc.variables:
                if 'dim1' not in nc.dimensions:
                    nc.createDimension('dim1',1)
                c = nc.createVariable('constraints','c',('dim1',))
            else:
                c = nc.variables['constraints']
            #we store the pickle string as an attribute of a netcdf variable
            #because that way we do not have to know how long the string is.
            #with a character variable you have to specify the dimension of the
            #string ahead of time.
            c.data = pickle.dumps(constraints)
            nc.close()
        else:
            # getting here means there where no constraints on the atoms just written
            #we should check if there are any old constraints left in the ncfile
            #from a previous atoms, and delete them if so
            delete_constraints = False
            nc = netCDF(self.get_nc())
            if 'constraints' in nc.variables:
                delete_constraints = True
            nc.close()

            if delete_constraints:
                print 'deleting old constraints'
                self.delete_ncattdimvar(self.nc,
                                        ncvars=['constraints'])
        
    def set_ft(self,ft):
        '''set the Fermi temperature for occupation smearing

        :Parameters:

          ft : float
            Fermi temperature in kT (eV)

        Electronic temperature, corresponding to gaussian occupation
        statistics. Device used to stabilize the convergence towards
        the electronic ground state. Higher values stabilizes the
        convergence. Values in the range 0.1-1.0 eV are recommended,
        depending on the complexity of the Fermi surface (low values
        for d-metals and narrow gap semiconducters, higher for free
        electron-like metals).
        '''
        nc = netCDF(self.nc,'a')
        v = 'ElectronicBands'
        if v in nc.variables:
            vnb = nc.variables[v]
        else:
            vnb = nc.createVariable('ElectronicBands','c',('dim1',))

        vnb.OccupationStatistics_FermiTemperature=ft
        nc.sync()
        nc.close()
        self.set_status('new')
        self.ready = False

    def set_status(self,status):
        '''set the status flag in the netcdf file

        :Parameters:

          status : string
            status flag, e.g. 'new', 'finished'
        '''
        nc = netCDF(self.nc,'a')
        nc.status = status
        nc.sync()
        nc.close()
        
    def set_spinpol(self,spinpol=False):
        '''set Spin polarization.

        :Parameters:

         spinpol : Boolean
           set_spinpol(True)  spin-polarized.
           set_spinpol(False) no spin polarization, default

        Specify whether to perform a spin polarized or unpolarized
        calculation.
        '''
        nc = netCDF(self.nc,'a')
        v = 'ElectronicBands'
        if v in nc.variables:
            vnb = nc.variables[v]
        else:
            vnb = nc.createVariable('ElectronicBands','c',('dim1',))

        if spinpol is True:
            vnb.SpinPolarization = 2
        else:
            vnb.SpinPolarization = 1

        nc.sync()
        nc.close()
        self.set_status('new')
        self.ready = False

    def set_fixmagmom(self,fixmagmom=None):
        '''set a fixed magnetic moment for a spin polarized calculation

        :Parameters:

          fixmagmom : float
            the magnetic moment of the cell in Bohr magnetons
        '''
        nc = netCDF(self.nc,'a')
        v = 'ElectronicBands'
        if v in nc.variables:
            vnb = nc.variables[v]
        else:
            vnb = ncf.createVariable('ElectronicBands','c',('dim1',))

        vnb.SpinPolarizaton = 2 #You must want spin-polarized
        vnb.FixedMagneticMoment = fixmagmom
        nc.sync()
        nc.close()
        self.set_status('new')
        self.ready = False

    def set_stress(self,stress=True):
        '''Turn on stress calculation

        :Parameters:

          stress : boolean
            set_stress(True) calculates stress
            set_stress(False) do not calculate stress
        '''
        nc = netCDF(self.get_nc(),'a')
        vs = 'NetCDFOutputControl'
        if vs in nc.variables:
            v = nc.variables[vs]
        else:
            v = nc.createVariable('NetCDFOutputControl','c',('dim1',))

        if stress is True:
            v.PrintTotalStress = 'Yes'
        else:
            v.PrintTotalStress = 'No'
        nc.sync()
        nc.close()
        self.set_status('new')
        self.ready = False
        
    def set_nc(self,nc='out.nc'):
        '''
        set filename for the netcdf and text output for this calculation

        :Parameters:

          nc : string
            filename for netcdf file
                
        if the ncfile attached to the calculator is changing, the old
        file will be copied to the new file so that all the calculator
        details are preserved.

        if the ncfile does not exist, it will get initialized.

        the text file will have the same basename as the ncfile, but
        with a .txt extension.
        '''
        #the first time this is called, there may be no self.nc defined
        if not hasattr(self,'nc'):
            self.nc = nc
            
        #check if the name is changing and if so, copy
        #the old ncfile to the new one.  This is necessary
        #to ensure all the calculator details are copied
        #over. the new file gets clobbered
        if nc != self.nc:
            if self.debug > 0:
                print 'copying %s to %s' % (self.nc,nc)
            #import shutil
            #shutil.copy(self.nc,nc)
            os.system('cp %s %s' % (self.nc,nc))
            if self.debug > 0:
                print 'nc = ',self.nc
                print 'kpts = ',self.get_kpts()
            self.nc = nc

        if self.nc is not None:
            #I always want the text file set based on the ncfile
            #and I never want to set this myself.
            base,ext = os.path.splitext(self.nc)
            self.txt = base + '.txt'
        
    def set_psp(self,sym=None,z=None,psp=None):
        '''
        set the pseudopotential file for a species or an atomic number.

        :Parameters:

         sym : string
           chemical symbol of the species

          z : integer
            the atomic number of the species

          psp : string
            filename of the pseudopotential

        
        you can only set sym or z.

        examples::
        
          set_psp('N',psp='pspfile')
          set_psp(z=6,psp='pspfile')

        '''
        if (sym is None and z is not None):
            from ase.data import chemical_symbols
            sym = chemical_symbols[z]
        elif (sym is not None and z is None):
            pass
        else:
            raise Exception, 'You can only specify Z or sym!'

        if not hasattr(self,'psp'):
            self.set_psp_database()
            
        self.psp[sym] = psp
        self.ready = False
        self.set_status('new')
        
        #now we update the netcdf file
        ncf = netCDF(self.nc,'a')
        vn = 'AtomProperty_%s' % sym
        if vn not in ncf.variables:
            p = ncf.createVariable(vn,'c',('dim20',))
        else:
            p = ncf.variables[vn]

        ppath = self.get_psp(sym=sym)
        p.PspotFile = ppath
        ncf.close()

    def set_symmetry(self,val=False):
        '''set how symmetry is used to reduce k-points

        :Parameters:

         val : Boolean
           set_sym(True) Maximum symmetry is used
           set_sym(False) No symmetry is used

        This variable controls the if and how DACAPO should attempt
        using symmetry in the calculation. Imposing symmetry generally
        speeds up the calculation and reduces numerical noise to some
        extent. Symmetry should always be applied to the maximum
        extent, when ions are not moved. When relaxing ions, however,
        the symmetry of the equilibrium state may be lower than the
        initial state. Such an equilibrium state with lower symmetry
        is missed, if symmetry is imposed. Molecular dynamics-like
        algorithms for ionic propagation will generally not break the
        symmetry of the initial state, but some algorithms, like the
        BFGS may break the symmetry of the initial state. Recognized
        options:

        "Off": No symmetry will be imposed, apart from time inversion
        symmetry in recipical space. This is utilized to reduce the
        k-point sampling set for Brillouin zone integration and has no
        influence on the ionic forces/motion.

        "Maximum": DACAPO will look for symmetry in the supplied
        atomic structure and extract the highest possible symmetry
        group. During the calculation, DACAPO will impose the found
        spatial symmetry on ionic forces and electronic structure,
        i.e. the symmetry will be conserved during the calculation.
        '''
        if val:
            symval = 'Maximum'
        else:
            symval = 'Off'
        
        ncf = netCDF(self.get_nc(),'a')
        if 'UseSymmetry' not in ncf.variables:
            sym = ncf.createVariable('UseSymmetry','c',('dim7',))
        else:
            sym = ncf.variables['UseSymmetry']
            
        sym[:] = np.array('%7s' % symval,'c')
        ncf.sync()
        ncf.close()
        self.set_status('new')
        self.ready = False

    def set_extracharge(self,val):
        '''add extra charge to unit cell

        :Parameters:

          val : float
            extra electrons to add or subtract from the unit cell

        Fixed extra charge in the unit cell (i.e. deviation from
        charge neutrality). This assumes a compensating, positive
        constant backgound charge (jellium) to forge overall charge
        neutrality.
        '''
        nc = netCDF(self.get_nc(),'a')
        if 'ExtraCharge' in nc.variables:
            v = nc.variables['ExtraCharge']
        else:
            v = nc.createVariable('ExtraCharge','f',('dim1',))

        v.assignValue(val)
        nc.sync()
        nc.close()
            
    def set_extpot(self,potgrid):
        '''add external potential of value

        see this link before using this
        https://listserv.fysik.dtu.dk/pipermail/campos/2003-August/000657.html
        
        :Parameters:

          potgrid : np.array with shape (nx,ny,nz)
            the shape must be the same as the fft soft grid
            the value of the potential to add

        
        you have to know both of the fft grid dimensions ahead of time!
        if you know what you are doing, you can set the fft_grid you want
        before hand with:
        calc.set_fftgrid((n1,n2,n3))
        '''
        
        nc = netCDF(self.get_nc(),'a')
        if 'ExternalPotential' in nc.variables:
            v = nc.variables['ExternalPotential']
        else:
            # I assume here you have the dimensions of potgrid correct
            # and that the soft and hard grids are the same. 
            # if softgrid is defined, Dacapo requires hardgrid to be
            # defined too.
            s1,s2,s3 = potgrid.shape
            if 'softgrid_dim1' not in nc.dimensions:
                d1 = nc.createDimension('softgrid_dim1',s1)
                d2 = nc.createDimension('softgrid_dim2',s2)
                d3 = nc.createDimension('softgrid_dim3',s3)
                h1 = nc.createDimension('hardgrid_dim1',s1)
                h2 = nc.createDimension('hardgrid_dim2',s2)
                h3 = nc.createDimension('hardgrid_dim3',s3)
                
            v = nc.createVariable('ExternalPotential',
                                  'f',
                                  ('softgrid_dim1',
                                   'softgrid_dim2',
                                   'softgrid_dim3',))
        
        v[:] = np.array(potgrid,np.float32)
        nc.sync()
        nc.close()
        self.set_status('new')
        self.ready = False
        
    def set_fftgrid(self,soft,hard=None):
        '''
        sets the dimensions of the FFT grid to be used

        :Parameters:

          soft : (n1,n2,n3) integers
            make a n1 x n2 x n3 grid

          hard : (n1,n2,n3) integers
            make a n1 x n2 x n3 grid

        
        >>> calc.set_fftgrid(soft=[42,44,46])
        sets the soft and hard grid dimensions to 42,44,46

        >>> calc.set_fftgrid(soft=[42,44,46],hard=[80,84,88])
        sets the soft grid dimensions to 42,44,46 and the hard
        grid dimensions to 80,84,88
        
        These are the fast FFt grid numbers listed in fftdimensions.F
        
        data list_of_fft /2,  4,  6,  8, 10, 12, 14, 16, 18, 20, &
        22,24, 28, 30,32, 36, 40, 42, 44, 48, &
        56,60, 64, 66, 70, 72, 80, 84, 88, 90, &
        96,108,110,112,120,126,128,132,140,144,154, &
        160,168,176,180,192,198,200, &
        216,240,264,270,280,288,324,352,360,378,384,400,432, &
        450,480,540,576,640/
        
        otherwise you will get some errors from mis-dimensioned variables.
        
        this is usually automatically set by Dacapo.
        '''
        
        self.delete_ncattdimvar(self.nc,
                                ncdims=['softgrid_dim1',
                                        'softgrid_dim2',
                                        'softgrid_dim3',
                                        'hardgrid_dim1',
                                        'hardgrid_dim2',
                                        'hardgrid_dim3'])
        
        nc = netCDF(self.get_nc(),'a')

        nc.createDimension('softgrid_dim1',soft[0])
        nc.createDimension('softgrid_dim2',soft[1])
        nc.createDimension('softgrid_dim3',soft[2])

        if hard is None:
            hard = soft
        
        nc.createDimension('hardgrid_dim1',hard[0])
        nc.createDimension('hardgrid_dim2',hard[1])
        nc.createDimension('hardgrid_dim3',hard[2])               
        nc.sync()
        nc.close()
        self.set_status('new')
        self.ready = False

    def set_debug(self,level):
        '''set the debug level for Dacapo

        :Parameters:
        
          level : string
            one of 'Off', 'MediumLevel', 'HighLevel'
        '''
        
        nc = netCDF(self.get_nc(),'a')
        if 'PrintDebugInfo' in nc.variables:
            v = nc.variables['PrintDebugInfo']
        else:
            if 'dim20' not in nc.dimensions:
                d = nc.createDimension('dim20',20)
            v = nc.createVariable('PrintDebugInfo','c',('dim20',))

        v[:] = np.array('%20s' % level,dtype='c')
        nc.sync()
        nc.close()
        self.set_status('new')
        self.ready = False

    def set_ncoutput(self,
                     wf=None,
                     cd=None,
                     efp=None,
                     esp=None):
        '''set the output of large variables in the netcdf output file

        :Parameters:

          wf : string
            controls output of wavefunction. values can
            be 'Yes' or 'No'

          cd : string
            controls output of charge density. values can
            be 'Yes' or 'No'

          efp : string
            controls output of effective potential. values can
            be 'Yes' or 'No'

          esp : string
            controls output of electrostatic potential. values can
            be 'Yes' or 'No'
        '''
        nc = netCDF(self.get_nc(),'a')
        if 'NetCDFOutputControl' in nc.variables:
            v = nc.variables['NetCDFOutputControl']
        else:
            v = nc.createVariable('NetCDFOutputControl','c',())

        if wf is not None: v.PrintWaveFunction = wf
        if cd is not None: v.PrintChargeDensity = cd
        if efp is not None: v.PrintEffPotential = efp
        if esp is not None: v.PrintElsPotential = esp

        nc.sync()
        nc.close()
        self.set_status('new')
        self.ready = False
        
    def set_ados(self,
                 energywindow=(-15,5),
                 energywidth=0.2,
                 npoints=250,
                 cutoff=1.0):
        '''
        setup calculation of atom-projected density of states

        :Parameters:

          energywindow : (float, float)
            sets (emin,emax) in eV referenced to the Fermi level

          energywidth : float
            the gaussian used in smearing

          npoints : integer
            the number of points to sample the DOS at

          cutoff : float
            the cutoff radius in angstroms for the integration.
        '''
        nc = netCDF(self.get_nc(),'a')
        if 'PrintAtomProjectedDOS' in nc.variables:
            v = nc.variables['PrintAtomProjectedDOS']
        else:
            v = nc.createVariable('PrintAtomProjectedDOS','c',())

        v.EnergyWindow = energywindow
        v.EnergyWidth  = energywidth
        v.NumberEnergyPoints = npoints
        v.CutoffRadius = cutoff

        nc.sync()
        nc.close()
        self.set_status('new')
        self.ready = False

    def set_decoupling(self,
                       ngaussians=3,
                       ecutoff=100,
                       gausswidth=0.35):
        '''
        Decoupling activates the three dimensional electrostatic
        decoupling. Based on paper by Peter E. Bloechl: JCP 103
        page7422 (1995).

        :Parameters:

          ngaussians : int
            The number of gaussian functions per atom
            used for constructing the model charge of the system

          ecutoff : int
            The cut off energy (eV) of system charge density in
            g-space used when mapping constructing the model change of
            the system, i.e. only charge density components below
            ECutoff enters when constructing the model change.

          gausswidth : float
            The width of the Gaussians defined by
            $widthofgaussian*1.5^(n-1)$  $n$=(1 to numberofgaussians)
            
        '''

        nc = netCDF(self.get_nc(),'a')
        if 'Decoupling' in nc.variables:
            v = nc.variables['Decoupling']
        else:
            v = nc.createVariable('Decoupling','c',())

        v.NumberOfGaussians = ngaussians
        v.ECutoff = ecutoff
        v.WidthOfGaussian = gausswidth

        nc.sync()
        nc.close()
        self.set_status('new')
        self.ready = False

    def set_external_dipole(self,
                            value,
                            position=None):
        '''
        Externally imposed dipole potential. This option overwrites
        DipoleCorrection if set. 

        :Parameters:

          value : float
            units of volts

          position : float
            scaled coordinates along third unit cell direction.
            if None, the compensation dipole layer plane in the
            vacuum position farthest from any other atoms on both
            sides of the slab. Do not set to 0.0.
        '''
        var = 'ExternalDipolePotential'
        nc = netCDF(self.get_nc(),'a')
        if var in nc.variables:
            v = nc.variables[var]
        else:
            v = nc.createVariable('ExternalDipolePotential','f')

        v.setValue(value)
        if position is not None:
            v.DipoleLayerPosition = position

        nc.sync()
        nc.close()
        self.set_status('new')
        self.ready = False
                            
    def set_dipole(self, status=True,
                   mixpar=0.2,
                   initval=0.0,
                   adddipfield=0.0,
                   position=None):
        '''turn on and set dipole correction scheme

        :Parameters:

          status : Boolean
            True turns dipole on. False turns Dipole off

          mixpar : float
            Mixing Parameter for the the dipole correction field
            during the electronic minimization process. If instabilities
            occur during electronic minimization, this value may be
            decreased.

          initval : float
            initial value to start at

          adddipfield : float
            additional dipole field to add
            units : V/ang
            External additive, constant electrostatic field along
            third unit cell vector, corresponding to an external
            dipole layer. The field discontinuity follows the position
            of the dynamical dipole correction, i.e. if
            DipoleCorrection:DipoleLayerPosition is set, the field
            discontinuity is at this value, otherwise it is at the
            vacuum position farthest from any other atoms on both
            sides of the slab.

          position : float
            scaled coordinates along third unit cell direction.
            If this attribute is set, DACAPO will position the
            compensation dipole layer plane in at the provided value.
            If this attribute is not set, DACAPO will put the compensation
            dipole layer plane in the vacuum position farthest from any
            other atoms on both sides of the slab. Do not set this to
            0.0

        
        calling set_dipole() sets all default values.
            
        '''
        if status == False:
            self.delete_ncattdimvar(self.nc,ncvars=['DipoleCorrection'])
            return
        
        ncf = netCDF(self.get_nc(),'a')
        if 'DipoleCorrection' not in ncf.variables:
            dip = ncf.createVariable('DipoleCorrection','c',())
        else:
            dip = ncf.variables['DipoleCorrection']
        dip.MixingParameter = mixpar
        dip.InitialValue = initval
        dip.AdditiveDipoleField = adddipfield

        if position is not None:
            dip.DipoleLayerPosition = position
            
        ncf.sync()
        ncf.close()
        self.set_status('new')
        self.ready = False

    def set_stay_alive(self,value):
        self.delete_ncattdimvar(self.nc,
                                ncvars=['Dynamics'])

        if value in [True,False]:
            self.stay_alive = value
            #self._dacapo_is_running = False
        else:
            print "stay_alive must be boolean. Value was not changed."

    def get_stay_alive(self):
        return self.stay_alive

    def get_symmetry(self):
        '''return the type of symmetry used'''
        nc = netCDF(self.nc,'r')
        if 'UseSymmetry' in nc.variables:
            sym = string.join(nc.variables['UseSymmetry'][:],'')
        else:
            sym = None
            
        nc.close()
        return sym

    def get_fftgrid(self):
        'return soft and hard grids'
        nc = netCDF(self.nc,'r')

        soft = []
        hard = []
        for d in [1,2,3]:
            sd = 'softgrid_dim%i' % d
            hd = 'hardgrid_dim%i' % d
            if sd in nc.dimensions:
                soft.append(nc.dimensions[sd])
                hard.append(nc.dimensions[hd])

        nc.close()
        return (soft,hard)

    def get_kpts(self):
        'return the kpt grid, not the kpts'
        nc = netCDF(self.nc,'r')

        if 'BZKpoints' in nc.variables:
            bv = nc.variables['BZKpoints']
            if hasattr(bv,'grid'):
                kpts = (bv.grid)
            else:
                kpts = len(bv[:])
        else:
            kpts = (1,1,1) #default used in Dacapo

        nc.close()
        return kpts
        
    def get_nbands(self):
        'return the number of bands used in the calculation'
        nc = netCDF(self.nc,'r')

        if 'ElectronicBands' in nc.variables:
            v = nc.variables['ElectronicBands']
            if hasattr(v,'NumberOfBands'):
                nbands = v.NumberOfBands[0]
            else:
                nbands = None
        else:
            nbands = None
            
        nc.close()
        return nbands
    
    def get_ft(self):
        'return the FermiTemperature used in the calculation'
        nc = netCDF(self.nc,'r')

        if 'ElectronicBands' in nc.variables:
            v = nc.variables['ElectronicBands']
            if hasattr(v,'OccupationStatistics_FermiTemperature'):
                ft = v.OccupationStatistics_FermiTemperature
            else:
                ft = None
        else:
            ft = None
        nc.close()
        return ft
    
    def get_dipole(self):
        'return True if the DipoleCorrection was used'
        nc = netCDF(self.get_nc(),'r')
        if 'DipoleCorrection' in nc.variables:
            getdip = True
        else:
            getdip = False
        nc.close()
        return getdip
        
    def get_pw(self):
        'return the planewave cutoff used'
        ncf = netCDF(self.nc,'r')
        pw = ncf.variables['PlaneWaveCutoff'].getValue()
        ncf.close()
        return pw

    def get_dw(self):
        'return the density wave cutoff'
        ncf = netCDF(self.nc,'r')
        if 'Density_WaveCutoff' in ncf.variables:
            dw = ncf.variables['Density_WaveCutoff'].getValue()
        else:
            dw = self.get_pw()
        ncf.close()
        return dw
    
    def get_xc(self):
        '''return the self-consistent exchange-correlation functional used

        returns a string'''
        nc = netCDF(self.nc,'r')
        v = 'ExcFunctional'
        if v in nc.variables:
            xc = nc.variables[v][:].tostring().strip()
        else:
            xc = None

        nc.close()
        return xc

    def get_potential_energy(self,atoms=None,force_consistent=False):
        '''
        return the potential energy.
        '''
        if self.calculation_required(atoms):
            if self.debug > 0: print 'calculation required for energy'
            self.calculate()
                        
        nc = netCDF(self.get_nc(),'r')
        try:
            if force_consistent:
                e = nc.variables['TotalFreeEnergy'][-1]
            else:
                e = nc.variables['TotalEnergy'][-1]
            nc.close()
            return e 
        except (TypeError,KeyError):
            raise RuntimeError('Error in calculating the total energy\n' +
                               'Check ascii out file for error messages')

    def get_forces(self, atoms=None):
        """Calculate atomic forces"""
        if atoms is None:
            atoms = self.atoms
        if self.calculation_required(atoms):
            if self.debug > 0: print 'calculation required for forces'
            self.calculate()
        nc = netCDF(self.get_nc(),'r')
        forces = nc.variables['DynamicAtomForces'][-1]
        nc.close()
        return forces

    def get_atoms(self):
        'return the atoms attached to a calculator()'
        if hasattr(self,'atoms'):
            atoms = self.atoms.copy()
            atoms.set_calculator(self)
        else:
            atoms = None
        return atoms

    def get_nc(self):
        'return the ncfile used for output'
        return self.nc

    def get_txt(self):
        'return the txt file used for output'
        return self.txt
        
    def get_psp(self,sym=None,z=None):
        '''get the pseudopotential filename from the psp database

        :Parameters:

          sym : string
            the chemical symbol of the species

          z : integer
            the atomic number of the species

        
        you can only specify sym or z. Returns the pseudopotential
        filename, not the full path.
        '''            
        if (sym is None and z is not None):
            from ase.data import chemical_symbols
            sym = chemical_symbols[z]
        elif (sym is not None and z is None):
            pass
        else:
            raise Exception, 'You can only specify Z or sym!'
        psp = self.psp[sym]
        return psp
    
    def get_spin_polarized(self):
        'Return True if calculate is spin-polarized or False if not'
        #self.calculate() #causes recursion error with get_magnetic_moments
        nc = netCDF(self.nc,'r')
        if 'ElectronicBands' in nc.variables:
            v = nc.variables['ElectronicBands']
            if hasattr(v,'SpinPolarization'):
                if v.SpinPolarization==1:
                    spinpol = False
                elif v.SpinPolarization==2:
                    spinpol = True
            else:
                spinpol = False
        else:
            spinpol = 'Not defined'

        nc.close()
        return spinpol

    def get_magnetic_moments(self,atoms=None):
        '''return magnetic moments on each atom after the calculation is
        run'''

        if self.calculation_required(atoms):
            self.calculate()
        nc = netCDF(self.nc,'r')
        if 'InitialAtomicMagneticMoment' in nc.variables:
            mom = nc.variables['InitialAtomicMagneticMoment'][:]
        else:
            mom = [0.0]*len(self.atoms)

        nc.close()
        return mom

    def get_status(self):
        '''get status of calculation from ncfile. usually one of:
        'new',
        'aborted'
        'running'
        'finished'
        None
        '''
        nc = netCDF(self.nc,'r')
        if hasattr(nc,'status'):
            status = nc.status
        else:
            status = None
        nc.close()
        return status
    
    def get_stress(self,atoms=None):
        '''get stress on the atoms.

        you should have set up the calculation
        to calculate stress first.

        returns [sxx, syy, szz, syz, sxz, sxy]'''
        if self.calculation_required(atoms):
            self.calculate()

        nc = netCDF(self.get_nc(),'r')
        if 'TotalStress' in nc.variables:
            stress = nc.variables['TotalStress'][:]
            #ase expects the 6-element form
            stress = np.take(stress.ravel(),[0,4,8,5,2,1])
        else:
            #stress will not be here if you did not set it up by
            #calling set_stress() or in the __init__
            stress = None
        
        nc.close()
        
        return stress

    def get_valence(self,atoms=None):
        '''return the total number of valence electrons for the
        atoms. valence electrons are read directly from the
        pseudopotentials.

        the psp filenames are stored in the ncfile. They may be just
        the name of the file, in which case the psp may exist in the
        same directory as the ncfile, or in $DACAPOPATH, or the psp
        may be defined by an absolute or relative path. This function
        deals with all these possibilities.
        '''
        from struct import unpack
        
        #do not use get_atoms() or recursion occurs
        if atoms is None:
            if hasattr(self,'atoms'):
                atoms = self.atoms
            else:
                return None

        dacapopath = os.environ.get('DACAPOPATH')
        totval = 0.0
        for sym in atoms.get_chemical_symbols():
            psp = self.get_psp(sym)
            
            if os.path.exists(psp):
                #the pspfile may be in the current directory
                #or defined by an absolute path
                fullpsp = psp

            #let's also see if we can construct an absolute path to a
            #local or relative path psp.
            abs_path_to_nc = os.path.abspath(self.get_nc())
            base,ncfile = os.path.split(abs_path_to_nc)
            possible_path_to_psp = os.path.join(base,psp)
            if os.path.exists(possible_path_to_psp):
                fullpsp = possible_path_to_psp
                
            else:
                #or, it is in the default psp path
                fullpsp = os.path.join(dacapopath,psp)
            if os.path.exists(fullpsp.strip()):
                f = open(fullpsp)
                # read past version numbers and text information
                buf = f.read(64)
                # read number valence electrons
                buf = f.read(8)
                fmt = ">d"
                nvalence = unpack(fmt,buf)[0]
                f.close()
                totval += float(nvalence)
            else:
                raise Exception, "%s does not exist" % fullpsp
            
        return totval 

    def calculation_required(self, atoms=None, quantities=None):
        '''
        determines if a calculation is needed.

        return True if a calculation is needed to get up to date data.
        return False if no calculation is needed.

        quantities is here because of the ase interface.
        '''
        #provide a way to make no calculation get run
        if os.environ.get('DACAPO_DRYRUN',None) is not None:
            return False
        
        # first, compare if the atoms is the same as the stored atoms
        # if anything has changed, we need to run a calculation
        if self.debug > 0: print 'running calculation_required'

        if self.nc is None:
            raise Exception, 'No output ncfile specified!'
                
        if atoms is not None:
            if atoms != self.atoms:
                if self.debug > 0:
                    print 'found that atoms != self.atoms'
                tol = 1e-6 #tolerance that the unit cell is the same
                new = atoms.get_cell()
                old = self.atoms.get_cell()
                #float comparison of equality
                if not np.all(abs(old-new) < tol): 
                    #this often changes the number of planewaves
                    #which requires a complete restart
                    if self.debug > 0:
                        print 'restart required! because cell changed'
                    self.restart()
                else:
                    if self.debug > 0:
                        print 'Unitcells apparently the same'
                    
                self.set_atoms(atoms) #we have to update the atoms in any case
                if self.debug > 0:
                    print 'returning true for calculation required'
                return True
            
        #if we make it past the atoms check, we look in the
        #nc file. if parameters have been changed the status
        #will tell us if a calculation is needed

        #past this point, atoms was None or equal, so there is nothing to
        #update in the calculator

        flag = True
        if self.debug > 0: print 'atoms tested equal'
        if os.path.exists(self.nc):
            if self.debug > 0: print 'ncfile = ',self.nc
            nc = netCDF(self.nc,'r')
            if hasattr(nc,'status'):
                if nc.status == 'finished' and self.ready:
                    nc.close()
                    return False
                elif nc.status == 'running':
                    nc.close()
                    raise DacapoRunning('Dacapo is Running')
                elif nc.status == 'aborted':
                    nc.close()
                    raise DacapoAborted('Dacapo aborted. try to fix the problem!')
                else:
                    if self.debug > 0:
                        print 'ncfile exists, but is not ready'
                        print 'self.ready = ',self.ready
                    nc.close()
                    return True
            else:
                #legacy calculations do not have a status flag in them.
                #let us guess that if the TotalEnergy is there
                #no calculation needs to be run?
                if 'TotalEnergy' in nc.variables:
                    runflag = False
                else:
                    runflag = True
                nc.close()
                return runflag #if no status run calculation
            nc.close()
            
        #default, a calculation is required
        if self.debug > 0: print 'default calculation required'
        return True

    def get_scratch(self):
        '''finds an appropriate scratch directory for the calculation'''
        import getpass
        username=getpass.getuser()

        scratch_dirs = []
        if os.environ.has_key('SCRATCH'):
            scratch_dirs.append(os.environ['SCRATCH'])
        if os.environ.has_key('SCR'):
            scratch_dirs.append(os.environ['SCR'])
        scratch_dirs.append('/scratch/'+username)
        scratch_dirs.append('/scratch/')
        scratch_dirs.append(os.curdir)
        for scratch_dir in scratch_dirs:
            if os.access(scratch_dir,os.W_OK):
                return scratch_dir
        raise IOError,"No suitable scratch directory and no write access to current dir"

    def calculate(self):
        '''run a calculation.

        you have to be a little careful with code in here. Use the
        calculation_required function to tell if a calculation is
        required. It is assumed here that if you call this, you mean
        it.'''
        
        if self.debug > 0: print 'running a calculation'

        nc = self.get_nc()
        txt= self.get_txt()
        scratch = self.get_scratch()

        #check that the bands get set
        if self.get_nbands() is None: self.set_nbands() 

        if self.stay_alive:
            self.execute_external_dynamics(nc,txt)
            self.ready = True
            self.set_status('finished')

        else:
            cmd = 'dacapo.run  %(innc)s -out %(txt)s -scratch %(scratch)s' % {'innc':nc,'txt':txt, 'scratch':scratch}

            if self.debug > 0: print cmd
            # using subprocess instead of commands
            # subprocess is more flexible and works better for stay_alive
            self._dacapo = sp.Popen(cmd,stdout=sp.PIPE,stderr=sp.PIPE,shell=True)
            status = self._dacapo.wait()
            [stdout,stderr] = self._dacapo.communicate()
            output = stdout+stderr
            #status,output = commands.getstatusoutput(cmd)
            if status is 0: #that means it ended fine!
                self.ready = True
                self.set_status('finished')
            else:
                print 'Status was not 0'
                print output
                self.ready = False
            del self._dacapo
        # directory cleanup has been moved to self.__del__()

    def execute_external_dynamics(self,nc=None,txt=None,stoppfile='stop',stopprogram=None):
        '''
        Implementation of the stay alive functionality with socket communication between dacapo and python.
        Known limitations: It is not possible to start 2 independent Dacapo calculators from the same python process,
        since the python PID is used as identifier for the script[PID].py file.
        '''
        from socket import socket,AF_INET,SOCK_STREAM,timeout
        import tempfile
        import os

        if self.debug > 0:
            if hasattr(self,"_dacapo"):
                print "Starting External Dynamics while Dacapo is runnning ",self._dacapo.poll()
            else:
                print "No dacapo instance has been started yet"
            print "Stopprogram",stopprogram

        if not nc: nc = self.get_nc()
        if not txt: txt = self.get_txt()
        tempfile.tempdir=os.curdir

        if stopprogram:
                # write stop file
                stfile = open(stoppfile,'w')
                stfile.write('1 \n')
                stfile.close()

                # signal to dacapo that positions are ready
                # let dacapo continue, it is up to the python mainloop 
                # to allow dacapo enough time to finish properly.
                self._client.send('ok too proceed')

                # Wait for dacapo to acknowledge that netcdf file has been updated, and analysis part of the code
                # has been terminated. Dacapo sends a signal at the end of call clexit().
                print "waiting for dacapo to exit..."
                self.s.settimeout(1200.0)  # if dacapo exits with an error, self.s.accept() should time out,
                                          # but we need to give it enough time to write the wave function to the nc file.
                try:
                    self._client,self._addr = self.s.accept() # Last mumble before Dacapo dies.
                    os.system("sleep 5")                     # 5 seconds of silence mourning dacapo.
                except timeout:
                    print "Socket connection timed out. This usually means Dacapo crashed."
                    pass

                # close the socket s
                self.s.close()
                self._client.close()

                # remove the script???? file
                ncfile = netCDF(nc,'r')
                vdyn = ncfile.variables['Dynamics']
                os.system('rm -f '+vdyn.ExternalIonMotion_script)
                ncfile.close()
                os.system('rm -f '+stoppfile)

                if self._dacapo.poll()==None:  # dacapo is still not dead!
                    # but this should do it!
                    sp.Popen("kill -9 "+str(self._dacapo.pid),shell=True)
                    #print "Dacapo was forced to quit. Check results."
                    #if Dacapo dies for example because of too few bands, subprocess never returns an exitcode.
                    #very strange, but at least the program is terminated.
                    #print self._dacapo.returncode
                del self._dacapo
                #print "dacapo is terminated"
                return

        if hasattr(self,'_dacapo') and self._dacapo.poll()==None: # returns None if dacapo is running  self._dacapo_is_running:

            # calculation_required already updated the positions in the nc file
            self._client.send('ok too proceed')

        else:

            # get process pid that will be used as communication channel 
            pid = os.getpid()

            # setup communication channel to dacapo
            from sys    import version
            from string import split
            effpid = (pid)%(2**16-1025)+1025   # This translate pid [0;99999] to a number in [1025;65535] (the allowed socket numbers)

            self.s = socket(AF_INET,SOCK_STREAM)
            foundafreesocket = 0
            while not foundafreesocket:
                try:
                        if split(version)[0] > "2":     # new interface
                                self.s.bind(("",effpid))
                        else:                           # old interface
                                self.s.bind("",effpid)
                        foundafreesocket = 1
                except:
                        effpid = effpid + 1

            # write script file that will be used by dacapo
            scriptname = 'script'+`pid`+'.py'
            scriptfile = open(scriptname,'w')
            scriptfile.write(
"""#!/usr/bin/env python
from socket import *
from sys    import version
from string import split  
s = socket(AF_INET,SOCK_STREAM)
# tell python that dacapo has finished
if split(version)[0] > "2":     # new interface 
     s.connect(("",""" + `effpid` + """))
else:                           # old interface
     s.connect("","""  + `effpid` + """)
# wait for python main loop
s.recv(14)
""")
            scriptfile.close()
            os.system('chmod +x ' + scriptname)

            # setup dynamics as external and set the script name
            ncfile = netCDF(nc,'a')
            vdyn = ncfile.createVariable('Dynamics','c',())
            vdyn.Type = "ExternalIonMotion" 
            vdyn.ExternalIonMotion_script = './'+ scriptname
            ncfile.close()

            # dacapo is not running start dacapo non blocking
            scratch_in_nc = tempfile.mktemp()
            os.system('mv '+nc+' '+scratch_in_nc)
            os.system('rm -f '+stoppfile)
            scratch = self.get_scratch()
            cmd = 'dacapo.run  %(innc)s %(outnc)s -out %(txt)s -scratch %(scratch)s' % {'innc':scratch_in_nc,'outnc':nc,'txt':txt, 'scratch':scratch}

            if self.debug > 0: print cmd
            self._dacapo = sp.Popen(cmd,stdout=sp.PIPE,stderr=sp.PIPE,shell=True)

            self.s.listen(1)

        # wait for dacapo  
        self._client,self._addr = self.s.accept()

    def write_nc(self,nc=None,atoms=None):
        '''
        write out a netcdffile. This does not change the ncfile
        attached to the calculator!

        :Parameters:

          nc : string
            ncfilename to write to. this file will get clobbered
            if it already exists.

          atoms : ASE.Atoms
            atoms to write. if None use the attached atoms
            if no atoms are attached only the calculator is
            written out. 

        the ncfile is always opened in 'a' mode.

        note: it is good practice to use the atoms argument to make
        sure that the geometry you mean gets written! Otherwise, the
        atoms in the calculator is used, which may be different than
        the external copy of the atoms.

        '''
        #no filename was provided to function, use the current ncfile
        if nc is None: 
            nc = self.get_nc()

        if nc != self.nc:
            #this means we are writing a new file, and we should copy
            #the old file to it first.  this makes sure the old
            #calculator settings are preserved
            new = nc
            old = self.nc
            os.system('cp %s %s' % (old, new))
              
        if atoms is None:
            atoms = self.get_atoms()

        if atoms is not None: #there may still be no atoms attached
        
            ncf = netCDF(nc,'a')

            if 'number_of_dynamic_atoms' not in ncf.dimensions:
                dnatoms = ncf.createDimension('number_of_dynamic_atoms',
                                              len(atoms))
            else:  # number of atoms is already a dimension, but we might be setting new atoms here
                   # check for same atom symbols (implicitly includes a length check)
                symbols = np.array(['%2s' % s for s in atoms.get_chemical_symbols()],dtype='c')
                ncsym = ncf.variables['DynamicAtomSpecies'][:]
                if (symbols.size != ncsym.size) or (np.any(ncsym != symbols)):
                    # the number of atoms or their order has changed.
                    # Treat this as a new calculation and reset number_of_ionic_steps and number_of_dynamic_atoms.
                    ncf.close()  #nc file must be closed for delete_ncattdimvar to work correctly
                    self.delete_ncattdimvar(nc,ncattrs=[],ncdims=['number_of_dynamic_atoms','number_ionic_steps'],ncvars=[])
                    ncf = netCDF(nc,'a')
                    dnatoms = ncf.createDimension('number_of_dynamic_atoms',
                                                  len(atoms)) 
                    dionsteps = ncf.createDimension('number_ionic_steps',None)
                    self._set_frame_number(0)
                    ncf.close() #nc file must be closed for restart to work correctly
                    self.restart()
                    ncf = netCDF(nc,'a')

            #now, create variables
            if 'DynamicAtomSpecies' not in ncf.variables:
                sym = ncf.createVariable('DynamicAtomSpecies',
                                         'c',
                                         ('number_of_dynamic_atoms',
                                          'dim2',))
            else:
                sym = ncf.variables['DynamicAtomSpecies']

            #note explicit array casting was required here
            symbols = atoms.get_chemical_symbols()
            sym[:] = np.array(['%2s' % s for s in symbols],dtype='c')

            if 'DynamicAtomPositions' not in ncf.variables:
                pos = ncf.createVariable('DynamicAtomPositions',
                                         'f',
                                         ('number_ionic_steps',
                                          'number_of_dynamic_atoms',
                                          'dim3'))
            else:
                pos = ncf.variables['DynamicAtomPositions']

            pos[self._frame,:] = np.array(atoms.get_scaled_positions(),np.float32)

            if 'UnitCell' not in ncf.variables:
                uc = ncf.createVariable('UnitCell','f',
                                        ('number_ionic_steps','dim3','dim3'))
            else:
                uc = ncf.variables['UnitCell']

            uc[self._frame,:] = np.array(atoms.get_cell(),np.float32)

            if 'AtomTags' not in ncf.variables:
                tags = ncf.createVariable('AtomTags','i',
                                          ('number_of_dynamic_atoms',))
            else:
                tags = ncf.variables['AtomTags']

            tags[:] = np.array(atoms.get_tags(),np.int32)

            if 'InitialAtomicMagneticMoment' not in ncf.variables:
                mom = ncf.createVariable('InitialAtomicMagneticMoment',
                                         'f',
                                         ('number_of_dynamic_atoms',))
            else:
                mom = ncf.variables['InitialAtomicMagneticMoment']

            #explain why we have to use get_initial_magnetic_moments()
            mom[:] = np.array(atoms.get_initial_magnetic_moments(),np.float32)
            
            #finally the atom pseudopotentials
            for sym in atoms.get_chemical_symbols():
                vn = 'AtomProperty_%s' % sym
                if vn not in ncf.variables:
                    p = ncf.createVariable(vn,'c',('dim20',))
                else:
                    p = ncf.variables[vn]

                ppath = self.get_psp(sym=sym)
                p.PspotFile = ppath

            ncf.sync()
            ncf.close()
        
    def read_atoms(filename):
        '''read atoms and calculator from an existing netcdf file.

        :Parameters:

          filename : string
            name of file to read from.

        static method

        example::
        
          >>> atoms = Jacapo.read_atoms(ncfile)
          >>> calc = atoms.get_calculator()

        this method is here for legacy purposes. I used to use it alot.
        '''
        
        calc = Jacapo(filename)
        atoms = calc.get_atoms()
        return atoms
    
    read_atoms = staticmethod(read_atoms)

    def read_only_atoms(self,ncfile):
        '''read only the atoms from an existing netcdf file. Used to
        initialize a calculator from a ncfilename.

        :Parameters:

          ncfile : string
            name of file to read from.

        return ASE.Atoms with no calculator attached or None if no atoms found
        '''
        from ase import Atoms
        
        nc = netCDF(ncfile,'r')
        #some ncfiles do not have atoms in them
        if 'UnitCell' not in nc.variables:
            print nc.variables
            print 'UnitCell not found, there are probably no atoms in ',ncfile
            nc.close()
            return None
        
        cell = nc.variables['UnitCell'][:][-1]
        sym = nc.variables['DynamicAtomSpecies'][:]
        symbols = [x.tostring().strip() for x in sym]
        spos = nc.variables['DynamicAtomPositions'][:][-1]

        pos = [p[0]*cell[0]+p[1]*cell[1]+p[2]*cell[2] for p in spos]
        
        atoms = Atoms(symbols=symbols,
                      positions=pos,
                      cell=cell)

        if 'AtomTags' in nc.variables:
            tags = nc.variables['AtomTags'][:]
            atoms.set_tags(tags)

        if 'InitialAtomicMagneticMoment' in nc.variables:
            mom = nc.variables['InitialAtomicMagneticMoment'][:]
            atoms.set_initial_magnetic_moments(mom)

        #get constraints if they exist
        c = nc.variables.get('constraints',None)
        if c is not None:
            constraints = pickle.loads(c.data)
            atoms.set_constraint(constraints)
                    
        nc.close()
        
        return atoms
        
    def delete_ncattdimvar(self,ncf,ncattrs=[],ncdims=[],ncvars=[]):
        '''
        helper function to delete attributes,
        dimensions and variables in a netcdffile

        this functionality is not implemented for some reason in
        netcdf, so the only way to do this is to copy all the
        attributes, dimensions, and variables to a new file, excluding
        the ones you want to delete and then rename the new file.

        if you delete a dimension, all variables with that dimension
        are also deleted.
        '''
        if self.debug > 0:
            print 'beginning: going to delete dims: ',ncdims
            print 'beginning: going to delete vars: ',ncvars
            
        oldnc = netCDF(ncf,'r')

        #h,tempnc = tempfile.mkstemp(dir='.',suffix='.nc')
        tempnc = ncf+'.temp'
        
        newnc= netCDF(tempnc,'w')

        for attr in dir(oldnc):
            if attr in ['close','createDimension',
                        'createVariable','flush','sync']:
                continue
            if attr in ncattrs:
                continue #do not copy this attribute
            setattr(newnc,attr,getattr(oldnc,attr))
           
        #copy dimensions
        for dim in oldnc.dimensions:
            if dim in ncdims:
                if self.debug > 0: print 'deleting %s of %s' % (dim,str(ncdims))
                continue #do not copy this dimension
            size=oldnc.dimensions[dim]
            #print 'created ',dim
            newnc.createDimension(dim,size)

        # we need to delete all variables that depended on a deleted dimension
        for v in oldnc.variables:
            dims1 = oldnc.variables[v].dimensions
            for dim in ncdims:
                if dim in dims1:
                    if self.debug > 0: print 'deleting "%s" because it depends on dim "%s"' %(v,dim)
                    ncvars.append(v)

        #copy variables, except the ones to delete
        for v in oldnc.variables:
            if v in ncvars:
                if self.debug>0:
                    print 'vars to delete: ',ncvars
                    print 'deleting ncvar: ',v
                continue #we do not copy this v over

            ncvar = oldnc.variables[v]
            tcode = ncvar.typecode()
            #char typecodes do not come out right apparently
            if tcode == " ":
                tcode = 'c'
            
            ncvar2 = newnc.createVariable(v,tcode,ncvar.dimensions)
            try:
                ncvar2[:] = ncvar[:]
            except TypeError:
                #this exception occurs for scalar variables
                #use getValue and assignValue instead
                if self.debug > 0: print 'EXCEPTION HAPPENED in delete_NCATTR'
                ncvar2.assignValue(ncvar.getValue())

            #and variable attributes
            #print dir(ncvar)
            for att in dir(ncvar):
                if att in ['assignValue', 'getValue', 'typecode']:
                    continue
                setattr(ncvar2,att,getattr(ncvar,att))

        oldnc.close()
        newnc.close()

        if self.debug > 0: print 'deletencatt--2 (.nfs): ', glob.glob('.nfs*')
        #ack!!! this makes .nfsxxx files!!!
        #os.close(h) #this avoids the stupid .nfsxxx file
        #import shutil
        #shutil.move(tempnc,ncf)

        #this seems to avoid making the .nfs files 
        os.system('cp %s %s' % (tempnc,ncf))
        os.system('rm %s' % tempnc)

        if self.debug > 0: print 'deletencatt-end (.nfs): ', glob.glob('.nfs*')
        
    def restart(self):
        '''
        Restart the calculator by deleting nc dimensions that will
        be rewritten on the next calculation. This is sometimes required
        when certain dimensions change related to unitcell size changes
        planewave/densitywave cutoffs and kpt changes. These can cause
        fortran netcdf errors if the data does not match the pre-defined
        dimension sizes.
        '''
        if self.debug > 0: print 'restarting!'
        
        ncdims = ['number_plane_waves',
                  'number_IBZ_kpoints',
                  'softgrid_dim1',
                  'softgrid_dim2',
                  'softgrid_dim3',
                  'hardgrid_dim1',
                  'hardgrid_dim2',
                  'hardgrid_dim3']

        #very strange error if I don't set ncvars to []
        #for some reason, BZKpoints gets deleted
        self.delete_ncattdimvar(self.nc,
                                ncattrs=[],
                                ncdims=ncdims,
                                ncvars=[])

        self.set_status('new')
        self.ready = False

    #############################
    # misc set methods for Dacapo
    #############################
    def set_convergence(self,
                        energy=0.00001,
                        density=0.0001,
                        occupation=0.001,
                        maxsteps=None,
                        maxtime=None
                        ):
        '''set convergence criteria for stopping the dacapo calculator.

        :Parameters:

          energy : float
            set total energy change (eV) required for stopping

          density : float
            set density change required for stopping

          occupation : float
            set occupation change required for stopping

          maxsteps : integer
            specify maximum number of steps to take

          maxtime : integer
            specify maximum number of hours to run.

        Autopilot not supported here.
        '''
        nc = netCDF(self.get_nc(),'a')
        vname = 'ConvergenceControl'
        if vname in nc.variables:
            v = nc.variables[vname]
        else:
            v = nc.createVariable(vname,'c',('dim1',))

        if energy is not None:
            v.AbsoluteEnergyConvergence = energy
        if density is not None:
            v.DensityConvergence = density
        if occupation is not None:
            v.OccupationConvergence = occupation
        if maxsteps is not None:
            v.MaxNumberOfSteps = maxsteps
        if maxtime is not None:
            v.CPUTimeLimit = maxtime

        nc.sync()
        nc.close()

    def set_charge_mixing(self,
                          method='Pulay',
                          mixinghistory=10,
                          mixingcoeff=0.1,
                          precondition='No',
                          updatecharge='Yes'):
        '''set density mixing method and parameters

        :Parameters:

          method : string
            'Pulay' for Pulay mixing. only one supported now

          mixinghistory : integer
            number of iterations to mix
            Number of charge residual vectors stored for generating
            the Pulay estimate on the self-consistent charge density,
            see Sec. 4.2 in Kresse/Furthmuller:
            Comp. Mat. Sci. 6 (1996) p34ff

          mixingcoeff : float
            Mixing coefficient for Pulay charge mixing, corresponding
            to A in G$^1$ in Sec. 4.2 in Kresse/Furthmuller:
            Comp. Mat. Sci. 6 (1996) p34ff
                        
          precondition : string
            'Yes' or 'No'
            
            * "Yes" : Kerker preconditiong is used,
               i.e. q$_0$ is different from zero, see eq. 82
               in Kresse/Furthmuller: Comp. Mat. Sci. 6 (1996).
               The value of q$_0$ is fix to give a damping of 20
               of the lowest q vector.
            
            * "No" : q$_0$ is zero and mixing is linear (default).

          updatecharge : string
            'Yes' or 'No'
            
            * "Yes" : Perform charge mixing according to
               ChargeMixing:Method setting
              
            * "No" : Freeze charge to initial value.
               This setting is useful when evaluating the Harris-Foulkes
               density functional
              
        '''
        
        if method == 'Pulay':
            nc = netCDF(self.get_nc(),'a')
            vname = 'ChargeMixing'
            if vname in nc.variables:
                v = nc.variables[vname]
            else:
                v = nc.createVariable(vname,'c',('dim1',))

            v.Method = 'Pulay'
            v.UpdateCharge = updatecharge
            v.Pulay_MixingHistory = mixinghistory
            v.Pulay_DensityMixingCoeff = mixingcoeff
            v.Pulay_KerkerPrecondition = precondition

            nc.sync()
            nc.close()

        self.ready = False

    def set_electronic_minimization(self,
                                    method,
                                    diagsperband=None):
        '''set the eigensolver method

        Selector for which subroutine to use for electronic
        minimization

        Recognized options : "resmin", "eigsolve" and "rmm-diis".

        * "resmin" : Power method (Lennart Bengtson), can only handle
           k-point parallization.

        * "eigsolve : Block Davidson algorithm
           (Claus Bendtsen et al).

        * "rmm-diis : Residual minimization
           method (RMM), using DIIS (direct inversion in the iterate
           subspace) The implementaion follows closely the algorithm
           outlined in Kresse and Furthmuller, Comp. Mat. Sci, III.G/III.H
        
        :Parameters:

          method : string
            should be 'resmin', 'eigsolve' or 'rmm-diis'

          diagsperband : int
            The number of diagonalizations per band for
            electronic minimization algorithms (maps onto internal
            variable ndiapb). Applies for both
            ElectronicMinimization:Method = "resmin" and "eigsolve".
            default value = 2
        '''
        
        nc = netCDF(self.get_nc(),'a')
        vname = 'ElectronicMinimization'
        if vname in nc.variables:
            v = nc.variables[vname]
        else:
            v = nc.createVariable(vname,'c',('dim1',))
        v.Method = method

        if diagsperband is not None:
            v.DiagonalizationsPerBand = diagsperband

        nc.sync()
        nc.close()
        
    def set_occupationstatistics(self,method):
        '''
        set the method used for smearing the occupations.

        :Parameters:

          method : string
            one of 'FermiDirac' or 'MethfesselPaxton'
            Currently, the Methfessel-Paxton scheme (PRB 40, 3616 (1989).)
            is implemented to 1th order (which is recommemded by most authors).
            'FermiDirac' is the default
        '''
        nc = netCDF(self.get_nc(),'a')
        if 'ElectronicBands' in nc.variables:
            v = nc.variables['ElectronicBands']
            v.OccupationStatistics = method
        
        nc.sync()
        nc.close()

    #########################
    # dacapo get data methods
    #########################

    def get_fermi_level(self):
        'return Fermi level'
        if self.calculation_required():
            self.calculate()
        nc = netCDF(self.get_nc(),'r')
        ef = nc.variables['FermiLevel'][-1]
        nc.close()
        return ef

    def get_occupation_numbers(self,kpt=0,spin=0):
        '''return occupancies of eigenstates for a kpt and spin

        :Parameters:

          kpt : integer
            index of the IBZ kpoint you want the occupation of

          spin : integer
            0 or 1
        '''
        if self.calculation_required():
            self.calculate()
        nc = netCDF(self.get_nc(),'r')
        occ = nc.variables['OccupationNumbers'][:][-1][kpt,spin]
        nc.close()
        return occ

    def get_xc_energies(self,*functional):
        """
        Get energies for different functionals self-consistent and
        non-self-consistent.

        :Parameters:

          functional : strings
            some set of 'PZ','VWN','PW91','PBE','revPBE', 'RPBE'

        This function returns the self-consistent energy and/or
	energies associated with various functionals. 
        The functionals are currently PZ,VWN,PW91,PBE,revPBE, RPBE.
        The different energies may be useful for calculating improved
	adsorption energies as in B. Hammer, L.B. Hansen and
	J.K. Norskov, Phys. Rev. B 59,7413. 
        Examples: 
        get_xcenergies() #returns all the energies
        get_xcenergies('PBE') # returns the PBE total energy
        get_xcenergies('PW91','PBE','revPBE') # returns a
	# list of energies in the order asked for
        """
        if self.calculation_required():
            self.calculate()

        nc = netCDF(self.get_nc(),'r')

        funcenergies = nc.variables['EvaluateTotalEnergy'][:][-1]
        xcfuncs = nc.variables['EvalFunctionalOfDensity_XC'][:]

        nc.close()
        
        xcfuncs = [xc.tostring().strip() for xc in xcfuncs]
        edict = dict(zip(xcfuncs,funcenergies))

        if len(functional) == 0:
            #get all energies by default
            functional = xcfuncs

        return [edict[xc] for xc in functional]

    def get_ados(self,
                 atoms,
                 orbitals,
                 cutoff,
                 spin):
        '''get atom projected data

        :Parameters:

          atoms  
              list of atom indices (integers)

          orbitals
              list of strings
              ['s','p','d'],
              ['px','py','pz']
              ['d_zz', 'dxx-yy', 'd_xy', 'd_xz', 'd_yz']

          cutoff : string
            cutoff radius you want the results for 'short' or 'infinite'

          spin
            : list of integers
            spin you want the results for
            [0] or [1] or [0,1] for both

        returns (egrid, ados)
        egrid has the fermi level at 0 eV
        '''
        if self.calculation_required():
            self.calculate()
        nc = netCDF(self.get_nc(),'r')
        omapvar = nc.variables['AtomProjectedDOS_OrdinalMap']
        omap = omapvar[:] #indices
        c = omapvar.AngularChannels
        channels = [x.strip() for x in c.split(',')] #channel names
        #this has dimensions(nprojections, nspins, npoints)
        ados = nc.variables['AtomProjectedDOS_EnergyResolvedDOS'][:]
        #this is the energy grid for all the atoms
        egrid = nc.variables['AtomProjectedDOS_EnergyGrid'][:]
        nc.close()

        #it is apparently not necessary to normalize the egrid to
        #the Fermi level. the data is already for ef = 0.

        #get list of orbitals, replace 'p' and 'd' in needed
        orbs = []
        for o in orbitals:
            if o == 'p':
                orbs += ['p_x','p_y','p_z']
            elif o == 'd':
                orbs += ['d_zz', 'dxx-yy', 'd_xy', 'd_xz', 'd_yz']
            else:
                orbs += [o]

        orbinds = [channels.index(x) for x in orbs]

        cutdict = {'infinite':0,
                   'short':1}

        icut = cutdict[cutoff]

        ydata = np.zeros(len(egrid),np.float)
        
        for atomind in atoms:
            for oi in orbinds:
                ind = omap[atomind,icut,oi]

                for si in spin:
                    ydata += ados[ind,si]

        return (egrid,ydata)

    def get_all_eigenvalues(self,spin=0):
        '''return all the eigenvalues at all the kpoints for a spin.

        :Parameters:

          spin : integer
            which spin the eigenvalues are for'''
        if self.calculation_required():
            self.calculate()
        nc = netCDF(self.get_nc(),'r')
        ev = nc.variables['EigenValues'][:][-1][:,spin]
        nc.close()
        return ev
    
    def get_eigenvalues(self,kpt=0,spin=0):
        '''return the eigenvalues for a kpt and spin

        :Parameters:

          kpt : integer
            index of the IBZ kpoint

          spin : integer
            which spin the eigenvalues are for'''
        if self.calculation_required():
            self.calculate()
        nc = netCDF(self.get_nc(),'r')
        ev = nc.variables['EigenValues'][:][-1][kpt,spin]
        nc.close()
        return ev
    
    def get_k_point_weights(self):
        'return the weights on the IBZ kpoints'
        if self.calculation_required():
            self.calculate()
        nc = netCDF(self.get_nc(),'r')
        kw = nc.variables['KpointWeight'][:]
        nc.close()
        return kw

    def get_magnetic_moment(self):
        'calculates the magnetic moment (Bohr-magnetons) of the supercell'

        if not self.get_spin_polarized():
            return None
        
        if self.calculation_required():
            self.calculate()

        nibzk = len(self.get_ibz_kpoints())
        ibzkw = self.get_k_point_weights()
        spinup, spindn = 0.0, 0.0

        for k in range(nibzk):

            spinup += self.get_occupation_numbers(k,0).sum()*ibzkw[k]
            spindn += self.get_occupation_numbers(k,1).sum()*ibzkw[k]

        return (spinup - spindn)

    def get_number_of_spins(self):
        'if spin-polarized returns 2, if not returns 1'
        if self.calculation_required():
            self.calculate()
        nc = netCDF(self.get_nc(),'r')
        sp = nc.variables['ElectronicBands']
        nc.close()

        if hasattr(sp,'SpinPolarization'):
            return sp.SpinPolarization
        else:
            return 1

    def get_ibz_kpoints(self):
        'return list of kpoints in the irreducible brillouin zone'
        if self.calculation_required():
            self.calculate()
        nc = netCDF(self.get_nc(),'r')
        ibz = nc.variables['IBZKpoints'][:]
        nc.close()
        return ibz

    get_ibz_k_points = get_ibz_kpoints

    def get_bz_k_points(self):
        'return list of kpoints in the Brillouin zone'
        nc = netCDF(self.get_nc(),'r')
        if 'BZKpoints' in nc.variables:
            bz = nc.variables['BZKpoints'][:]
        else:
            bz = None
        nc.close()
        return bz
    
    
    def get_effective_potential(self,spin=1):
        '''
        returns the realspace local effective potential for the spin.
        the units of the potential are eV

        :Parameters:

          spin : integer
             specify which spin you want, 0 or 1
          
        '''
        if self.calculation_required():
            self.calculate()
            
        nc = netCDF(self.get_nc(),'r')
        efp = np.transpose(nc.variables['EffectivePotential'][:][spin])
        nc.close()
        (softgrid,hardgrid) = self.get_fftgrid()

        x,y,z = self.get_ucgrid(hardgrid)
        return (x,y,z,efp)
        
    def get_electrostatic_potential(self,spin=0):
        '''get electrostatic potential

        Netcdf documentation::
        
          double ElectrostaticPotential(number_of_spin,
                                        hardgrid_dim3,
                                        hardgrid_dim2,
                                        hardgrid_dim1) ;
                 ElectrostaticPotential:
                     Description = "realspace local effective potential" ;
                     unit = "eV" ;
                     
                '''
        if self.calculation_required():
            self.calculate()
            
        nc = netCDF(self.get_nc(),'r')
        esp = np.transpose(nc.variables['ElectrostaticPotential'][:][spin])
        nc.close()
        (softgrid,hardgrid) = self.get_fftgrid()

        x,y,z = self.get_ucgrid(hardgrid)
        
        return (x,y,z,esp)
    
    def get_charge_density(self,spin=0):
        '''
        return x,y,z,charge density data
        
        x,y,z are grids sampling the unit cell
        cd is the charge density data

        netcdf documentation::
        
          ChargeDensity(number_of_spin,
                        hardgrid_dim3,
                        hardgrid_dim2,
                        hardgrid_dim1)
          ChargeDensity:Description = "realspace charge density" ;
                  ChargeDensity:unit = "-e/A^3" ;

        '''
        if self.calculation_required():
            self.calculate()
            
        nc = netCDF(self.get_nc(),'r')
        cd = np.transpose(nc.variables['ChargeDensity'][:][spin])

        #I am not completely sure why this has to be done
        #it does give units of electrons/ang**3
        vol = self.get_atoms().get_volume()
        cd /= vol
        nc.close()
        (softgrid,hardgrid) = self.get_fftgrid()

        x,y,z = self.get_ucgrid(hardgrid)
        return x,y,z,cd

    def get_ucgrid(self,dims):
        '''Return X,Y,Z grids for uniform sampling of the unit cell

        dims = (n0,n1,n2)

        n0 points along unitcell vector 0
        n1 points along unitcell vector 1
        n2 points along unitcell vector 2
        '''
        n0,n1,n2 = dims
        
        s0 = 1.0/n0
        s1 = 1.0/n1
        s2 = 1.0/n2

        X,Y,Z = np.mgrid[0.0:1.0:s0,
                         0.0:1.0:s1,
                         0.0:1.0:s2]
        
        C = np.column_stack([X.ravel(),
                             Y.ravel(),
                             Z.ravel()])
        
        atoms = self.get_atoms()
        uc = atoms.get_cell()
        real = np.dot(C,uc)

        #now convert arrays back to unitcell shape
        RX = np.reshape(real[:,0],(n0,n1,n2))
        RY = np.reshape(real[:,1],(n0,n1,n2))
        RZ = np.reshape(real[:,2],(n0,n1,n2))
        return (RX, RY, RZ)

    def get_number_of_grid_points(self):
        # needed by ase.dft.wannier
        (softgrid,hardgrid) = self.get_fftgrid()
        return np.array(softgrid)
    
    def get_wannier_localization_matrix(self, nbands, dirG, kpoint,
                                        nextkpoint, G_I, spin):

        if self.calculation_required():
            self.calculate()

        if not hasattr(self,'wannier'):
            from utils.wannier import Wannier
            self.wannier = Wannier(self)
            self.wannier.set_bands(nbands)
            self.wannier.set_spin(spin)
        locmat = self.wannier.get_zi_bloch_matrix(dirG,kpoint,nextkpoint,G_I)
        return locmat

    def initial_wannier(self,
                        initialwannier,
                        kpointgrid,
                        fixedstates,
                        edf,
                        spin):

        if self.calculation_required():
            self.calculate()

        if not hasattr(self,'wannier'):
            from utils.wannier import Wannier
            self.wannier = Wannier(self)

        self.wannier.set_data(initialwannier)
        self.wannier.set_k_point_grid(kpointgrid)
        self.wannier.set_spin(spin)

        waves = [[self.get_reciprocal_bloch_function(band=band,kpt=kpt,spin=spin)
                  for band in range(self.get_nbands())]
                  for kpt in range(len(self.get_ibz_k_points()))]

        self.wannier.setup_m_matrix(waves,self.get_bz_k_points())
        c, U = self.wannier.get_list_of_coefficients_and_rotation_matrices((self.get_nbands(), fixedstates, edf))

        U = np.array(U)
        for k in range(len(c)):
            c[k] = np.array(c[k])
        return c, U

    def get_psp_nuclear_charge(self,psp):
        '''
        get the nuclear charge of the atom from teh psp-file.

        This is not the same as the atomic number, nor is it
        necessarily the negative of the number of valence electrons,
        since a psp may be an ion. this function is needed to compute
        centers of ion charge for the dipole moment calculation.

        We read in the valence ion configuration from the psp file and
        add up the charges in each shell.
        '''
        from struct import unpack
        dacapopath = os.environ.get('DACAPOPATH')

        if os.path.exists(psp):
            #the pspfile may be in the current directory
            #or defined by an absolute path
            fullpsp = psp

        else:
            #or, it is in the default psp path
            fullpsp = os.path.join(dacapopath,psp)

        if os.path.exists(fullpsp.strip()):
            f = open(fullpsp)
            unpack('>i',f.read(4))[0]
            for i in range(3):
                f.read(4)
            for i in range(3):
                f.read(4)
            f.read(8)
            f.read(20)
            f.read(8)
            f.read(8)
            f.read(8)
            nvalps = unpack('>i',f.read(4))[0]
            f.read(4)
            f.read(8)
            f.read(8)
            wwnlps = []
            for i in range(nvalps):
                f.read(4)
                wwnlps.append(unpack('>d',f.read(8))[0])
                f.read(8)
            f.close()

        else:
            raise Exception, "%s does not exist" % fullpsp

        return np.array(wwnlps).sum()

    def get_dipole_moment(self):
        '''
        return dipole moment of unit cell

        Defined by the vector connecting the center of electron charge
        density to the center of nuclear charge density.

        Units = eV*angstrom

        1 Debye = 0.208194 eV*angstrom

        '''
        if self.calculation_required():
            self.calculate()

        atoms = self.get_atoms()
        
        #center of electron charge density
        x,y,z,cd = self.get_charge_density()

        n1,n2,n3 = cd.shape
        nelements = n1*n2*n3
        voxel_volume = atoms.get_volume()/nelements
        total_electron_charge = -cd.sum()*voxel_volume

        
        electron_density_center = np.array([(cd*x).sum(),
                                            (cd*y).sum(),
                                            (cd*z).sum()])
        electron_density_center *= voxel_volume
        electron_density_center /= total_electron_charge
        #we need the - here so the two negatives don't cancel
        electron_dipole_moment = -electron_density_center*total_electron_charge

        # now the ion charge center
        psps = self.get_pseudopotentials()
        ion_charge_center = np.array([0.0, 0.0, 0.0])
        total_ion_charge = 0.0
        for atom in atoms:
            Z = self.get_psp_nuclear_charge(psps[atom.symbol])
            total_ion_charge += Z
            pos = atom.get_position()
            ion_charge_center += Z*pos

        ion_charge_center /= total_ion_charge
        ion_dipole_moment = ion_charge_center*total_ion_charge

        dipole_vector = (ion_dipole_moment + electron_dipole_moment)
        return dipole_vector
 
    def get_reciprocal_bloch_function(self,band=0,kpt=0,spin=0):
        '''
        return the reciprocal bloch function.  Need for Jacapo Wannier
        class.
        '''
        if self.calculation_required():
            self.calculate()

        nc = netCDF(self.get_nc(),'r')

        # read reciprocal bloch function
        npw = nc.variables['NumberPlaneWavesKpoint'][:]
        bf = nc.variables['WaveFunction'][kpt,spin,band]
        wflist = np.zeros(npw[kpt],np.complex)
        wflist.real = bf[0:npw[kpt],1]
        wflist.imag = bf[0:npw[kpt],0]

        nc.close()

        return wflist

    def get_reciprocal_fft_index(self,kpt=0):
        '''return the Wave Function FFT Index'''
        nc = netCDF(self.get_nc(),'r')
        recind = nc.variables['WaveFunctionFFTindex'][kpt,:,:]
        nc.close()
        return recind

    def get_pseudo_wave_function(self,band=0,kpt=0,spin=0,pad=True):

        '''return the pseudo wavefunction'''
        # pad=True does nothing here.
        if self.calculation_required():
            self.calculate()

        ibz = self.get_ibz_kpoints()

        #get the reciprocal bloch function
        wflist = self.get_reciprocal_bloch_function(band=band,kpt=kpt,spin=spin)
        # wflist == Reciprocal Bloch Function
 
        recind = self. get_reciprocal_fft_index(kpt)
        (softgrid,hardgrid) = self.get_fftgrid() 

        # GetReciprocalBlochFunctionGrid
        wfrec = np.zeros((softgrid),np.complex) 

        for i in xrange(len(wflist)):
            wfrec[recind[0,i]-1,
                  recind[1,i]-1,
                  recind[2,i]-1] = wflist[i]

        # calculate Bloch Function
        wf = wfrec.copy() 
        dim=wf.shape
        for i in range(len(dim)):
            wf=np.fft.fft(wf,dim[i],axis=i)

        #now the phase function to get the bloch phase
        basis = self.get_atoms().get_cell()
        kpoint = np.dot(ibz[kpt],basis) #coordinates of relevant kpoint in cartesian coordinates
        def phasefunction(coor):
            pf = np.exp(1.0j*np.dot(kpoint,coor))
            return pf

        # Calculating the Bloch phase at the origin (0,0,0) of the grid
        origin = np.array([0.,0.,0.])
        blochphase=phasefunction(origin)
        spatialshape = wf.shape[-len(basis):]
        gridunitvectors=np.array(map(lambda unitvector,shape:unitvector/shape,basis,spatialshape))

        for dim in range(len(spatialshape)):
            # Multiplying with the phase at the origin
            deltaphase=phasefunction(gridunitvectors[dim])
            # and calculating phase difference between each point
            newphase=np.fromfunction(lambda i,phase=deltaphase:phase**i,(spatialshape[dim],))
            blochphase=np.multiply.outer(blochphase,newphase)

        return blochphase*wf

    def get_wave_function(self,band=0,kpt=0,spin=0):
        '''return the wave function. This is the pseudo wave function divided by volume.'''
        
        pwf = self.get_pseudo_wave_function(band=band,kpt=kpt,spin=spin,pad=True)
        vol = self.get_atoms().get_volume()
        (softgrid,hardgrid) = self.get_fftgrid()
        x,y,z = self.get_ucgrid((softgrid))

        return x,y,z,pwf/np.sqrt(vol)

    def strip(self):
        '''remove all large memory nc variables not needed for
        anything I use very often. 
        '''
        self.delete_ncattdimvar(self.nc,
                                ncdims=['max_projectors_per_atom'],
                                ncvars=['WaveFunction',
                                        'WaveFunctionFFTindex',
                                        'NumberOfNLProjectors',
                                        'NLProjectorPsi',
                                        'TypeNLProjector1',
                                        'NumberofNLProjectors',
                                        'PartialCoreDensity',
                                        'ChargeDensity',
                                        'ElectrostaticPotential',
                                        'StructureFactor'])

# shortcut function names
Jacapo.get_cd = Jacapo.get_charge_density
Jacapo.get_wf = Jacapo.get_wave_function
Jacapo.get_esp = Jacapo.get_electrostatic_potential
Jacapo.get_occ = Jacapo.get_occupation_numbers
Jacapo.get_ef = Jacapo.get_fermi_level
Jacapo.get_number_of_bands = Jacapo.get_nbands

if __name__ == '__main__':

    pass
