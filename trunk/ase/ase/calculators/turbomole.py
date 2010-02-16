"""This module defines an ASE interface to Turbomole

http://www.turbomole.com/
"""


import os, sys, string

from ase.data import chemical_symbols
from ase.units import Hartree, Bohr
from ase.io.turbomole import write_turbomole

import numpy as np

class Turbomole:
    """Class for doing Turbomole calculations.
    """
    def __init__(self, label='turbomole'):
        """Construct TURBOMOLE-calculator object.

        Parameters
        ==========
        label: str
            Prefix to use for filenames (label.txt, ...).
            Default is 'turbomole'.

        Examples
        ========
        This is poor man's version of ASE-Turbomole:

        First you do a normal turbomole preparation using turbomole's
        define-program for the initial and final states.

        (for instance in subdirectories Initial and Final)

        Then relax the initial and final coordinates with desired constraints
        using standard turbomole.

        Copy the relaxed initial and final coordinates 
        (initial.coord and final.coord)
        and the turbomole related files (from the subdirectory Initial) 
        control, coord, alpha, beta, mos, basis, auxbasis etc to the directory
        you do the diffusion run.
        
        For instance:
        cd $My_Turbomole_diffusion_directory
        cd Initial; cp control alpha beta mos basis auxbasis ../;
        cp coord ../initial.coord;
        cd ../;
        cp Final/coord ./final.coord;
        mkdir Gz ; cp * Gz ; gzip -r Gz
        
        from ase import *
        a = read('coord', index=-1, format='turbomole')
        calc = Turbomole()
        a.set_calculator(calc)
        e = a.get_potential_energy()
        
        """
        
        # get names of executables for turbomole energy and forces
        # get the path

        os.system('rm -f sysname.file; sysname > sysname.file')
        f = open('sysname.file')
        architechture = f.readline()[:-1]
        f.close()
        tmpath = os.environ['TURBODIR']
        pre = tmpath + '/bin/' + architechture + '/'


        if os.path.isfile('control'):
            f = open('control')
        else:
            print 'File control is missing'
            raise RuntimeError, \
                'Please run Turbomole define and come thereafter back'
        lines = f.readlines()
        f.close()
        self.tm_program_energy=pre+'dscf'
        self.tm_program_forces=pre+'grad'        
        for line in lines:
            if line.startswith('$ricore'):
                self.tm_program_energy=pre+'ridft'
                self.tm_program_forces=pre+'rdgrad'

        self.label = label
        self.converged = False
        #clean up turbomole energy file
        os.system('rm -f energy; touch energy')

        #turbomole has no stress
        self.stress = np.empty((3, 3))
        

    def update(self, atoms):
        """Energy and forces are calculated when atoms have moved
        by calling self.calculate
        """
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
            self.species.append(Z)
        self.converged = False
        
    def get_potential_energy(self, atoms):
        self.update(atoms)
        return self.etotal

    def get_forces(self, atoms):
        self.update(atoms)
        return self.forces.copy()
    
    def get_stress(self, atoms):
        self.update(atoms)
        return self.stress.copy()

    def calculate(self, atoms):
        """Total Turbomole energy is calculated (to file 'energy'
        also forces are calculated (to file 'gradient')
        """
        self.positions = atoms.get_positions().copy()
        self.cell = atoms.get_cell().copy()
        self.pbc = atoms.get_pbc().copy()

        #write current coordinates to file 'coord' for Turbomole
        write_turbomole('coord', atoms)


        #Turbomole energy calculation
        os.system('rm -f output.energy.dummy; \
                      '+ self.tm_program_energy +'> output.energy.dummy')

        #check that the energy run converged
        if os.path.isfile('dscf_problem'):
            print 'Turbomole scf energy calculation did not converge'
            print 'issue command t2x -c > last.xyz'
            print 'and check geometry last.xyz and job.xxx or statistics'
            raise RuntimeError, \
                'Please run Turbomole define and come thereafter back'


        self.read_energy()

        #Turbomole atomic forces calculation
        #killing the previous gradient file because 
        #turbomole gradients are affected by the previous values
        os.system('rm -f gradient; rm -f output.forces.dummy; \
                      '+ self.tm_program_forces +'> output.forces.dummy')

        self.read_forces(atoms)

        self.converged = True

        
    def read_energy(self):
        """Read Energy from Turbomole energy file."""
        text = open('energy', 'r').read().lower()
        lines = iter(text.split('\n'))

        # Energy:
        for line in lines:
            if line.startswith('$end'):
                break
            elif line.startswith('$'):
                pass
            else:
                #print 'LINE',line
                energy_tmp = float(line.split()[1])
        #print 'energy_tmp',energy_tmp
        self.etotal = energy_tmp * Hartree


    def read_forces(self,atoms):
        """Read Forces from Turbomole gradient file."""

        file = open('gradient','r')
        line=file.readline()
        line=file.readline()
        tmpforces = np.array([[0, 0, 0]])
        while line:
            if 'cycle' in line:
                for i, dummy in enumerate(atoms):
                            line=file.readline()
                forces_tmp=[]
                for i, dummy in enumerate(atoms):
                            line=file.readline()
                            line2=string.replace(line,'D','E')
                            #tmp=np.append(forces_tmp,np.array\
                            #      ([[float(f) for f in line2.split()[0:3]]]))
                            tmp=np.array\
                                ([[float(f) for f in line2.split()[0:3]]])
                            tmpforces=np.concatenate((tmpforces,tmp))  
            line=file.readline()
            

        #note the '-' sign for turbomole, to get forces
        self.forces = (-np.delete(tmpforces, np.s_[0:1], axis=0))*Hartree/Bohr

        #print 'forces', self.forces

    def read(self):
        """Dummy stress for turbomole"""
        self.stress = np.empty((3, 3))
