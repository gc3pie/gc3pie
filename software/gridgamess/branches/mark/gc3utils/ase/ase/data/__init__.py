import numpy as np


chemical_symbols = ['X',  'H',  'He', 'Li', 'Be',
                    'B',  'C',  'N',  'O',  'F',
                    'Ne', 'Na', 'Mg', 'Al', 'Si',
                    'P',  'S',  'Cl', 'Ar', 'K',
                    'Ca', 'Sc', 'Ti', 'V',  'Cr',
                    'Mn', 'Fe', 'Co', 'Ni', 'Cu',
                    'Zn', 'Ga', 'Ge', 'As', 'Se',
                    'Br', 'Kr', 'Rb', 'Sr', 'Y',
                    'Zr', 'Nb', 'Mo', 'Tc', 'Ru',
                    'Rh', 'Pd', 'Ag', 'Cd', 'In',
                    'Sn', 'Sb', 'Te', 'I',  'Xe',
                    'Cs', 'Ba', 'La', 'Ce', 'Pr',
                    'Nd', 'Pm', 'Sm', 'Eu', 'Gd',
                    'Tb', 'Dy', 'Ho', 'Er', 'Tm',
                    'Yb', 'Lu', 'Hf', 'Ta', 'W',
                    'Re', 'Os', 'Ir', 'Pt', 'Au',
                    'Hg', 'Tl', 'Pb', 'Bi', 'Po',
                    'At', 'Rn', 'Fr', 'Ra', 'Ac',
                    'Th', 'Pa', 'U',  'Np', 'Pu',
                    'Am', 'Cm', 'Bk', 'Cf', 'Es',
                    'Fm', 'Md', 'No', 'Lw']

atomic_numbers = {}
for Z, symbol in enumerate(chemical_symbols):
    atomic_numbers[symbol] = Z

atomic_names = [
    '', 'Hydrogen', 'Helium', 'Lithium', 'Beryllium', 'Boron',
    'Carbon', 'Nitrogen', 'Oxygen', 'Fluorine', 'Neon', 'Sodium',
    'Magnesium', 'Aluminium', 'Silicon', 'Phosphorus', 'Sulfur',
    'Chlorine', 'Argon', 'Potassium', 'Calcium', 'Scandium',
    'Titanium', 'Vanadium', 'Chromium', 'Manganese', 'Iron',
    'Cobalt', 'Nickel', 'Copper', 'Zinc', 'Gallium', 'Germanium',
    'Arsenic', 'Selenium', 'Bromine', 'Krypton', 'Rubidium',
    'Strontium', 'Yttrium', 'Zirconium', 'Niobium', 'Molybdenum',
    'Technetium', 'Ruthenium', 'Rhodium', 'Palladium', 'Silver',
    'Cadmium', 'Indium', 'Tin', 'Antimony', 'Tellurium',
    'Iodine', 'Xenon', 'Caesium', 'Barium', 'Lanthanum',
    'Cerium', 'Praseodymium', 'Neodymium', 'Promethium',
    'Samarium', 'Europium', 'Gadolinium', 'Terbium',
    'Dysprosium', 'Holmium', 'Erbium', 'Thulium', 'Ytterbium',
    'Lutetium', 'Hafnium', 'Tantalum', 'Tungsten', 'Rhenium',
    'Osmium', 'Iridium', 'Platinum', 'Gold', 'Mercury',
    'Thallium', 'Lead', 'Bismuth', 'Polonium', 'Astatine',
    'Radon', 'Francium', 'Radium', 'Actinium', 'Thorium',
    'Protactinium', 'Uranium', 'Neptunium', 'Plutonium',
    'Americium', 'Curium', 'Berkelium', 'Californium',
    'Einsteinium', 'Fermium', 'Mendelevium', 'Nobelium',
    'Lawrencium', 'Unnilquadium', 'Unnilpentium', 'Unnilhexium']

atomic_masses = np.array([
   0.00000, # X
   1.00794, # H
   4.00260, # He
   6.94100, # Li
   9.01218, # Be
  10.81100, # B
  12.01100, # C
  14.00670, # N
  15.99940, # O
  18.99840, # F
  20.17970, # Ne
  22.98977, # Na
  24.30500, # Mg
  26.98154, # Al
  28.08550, # Si
  30.97376, # P
  32.06600, # S
  35.45270, # Cl
  39.94800, # Ar
  39.09830, # K
  40.07800, # Ca
  44.95590, # Sc
  47.88000, # Ti
  50.94150, # V
  51.99600, # Cr
  54.93800, # Mn
  55.84700, # Fe
  58.93320, # Co
  58.69340, # Ni
  63.54600, # Cu
  65.39000, # Zn
  69.72300, # Ga
  72.61000, # Ge
  74.92160, # As
  78.96000, # Se
  79.90400, # Br
  83.80000, # Kr
  85.46780, # Rb
  87.62000, # Sr
  88.90590, # Y
  91.22400, # Zr
  92.90640, # Nb
  95.94000, # Mo
    np.nan, # Tc
 101.07000, # Ru
 102.90550, # Rh
 106.42000, # Pd
 107.86800, # Ag
 112.41000, # Cd
 114.82000, # In
 118.71000, # Sn
 121.75700, # Sb
 127.60000, # Te
 126.90450, # I
 131.29000, # Xe
 132.90540, # Cs
 137.33000, # Ba
 138.90550, # La
 140.12000, # Ce
 140.90770, # Pr
 144.24000, # Nd
    np.nan, # Pm
 150.36000, # Sm
 151.96500, # Eu
 157.25000, # Gd
 158.92530, # Tb
 162.50000, # Dy
 164.93030, # Ho
 167.26000, # Er
 168.93420, # Tm
 173.04000, # Yb
 174.96700, # Lu
 178.49000, # Hf
 180.94790, # Ta
 183.85000, # W
 186.20700, # Re
 190.20000, # Os
 192.22000, # Ir
 195.08000, # Pt
 196.96650, # Au
 200.59000, # Hg
 204.38300, # Tl
 207.20000, # Pb
 208.98040, # Bi
    np.nan, # Po
    np.nan, # At
    np.nan, # Rn
    np.nan, # Fr
 226.02540, # Ra
    np.nan, # Ac
 232.03810, # Th
 231.03590, # Pa
 238.02900, # U
 237.04820, # Np
    np.nan, # Pu
    np.nan, # Am
    np.nan, # Cm
    np.nan, # Bk
    np.nan, # Cf
    np.nan, # Es
    np.nan, # Fm
    np.nan, # Md
    np.nan, # No
    np.nan])# Lw

covalent_radii = np.array([
 0.20, # X
 0.32, # H
 0.93, # He
 1.23, # Li
 0.90, # Be
 0.82, # B
 0.77, # C
 0.75, # N
 0.73, # O
 0.72, # F
 0.71, # Ne
 1.54, # Na
 1.36, # Mg
 1.18, # Al
 1.11, # Si
 1.06, # P
 1.02, # S
 0.99, # Cl
 0.98, # Ar
 2.03, # K
 1.74, # Ca
 1.44, # Sc
 1.32, # Ti
 1.22, # V
 1.18, # Cr
 1.17, # Mn
 1.17, # Fe
 1.16, # Co
 1.15, # Ni
 1.17, # Cu
 1.25, # Zn
 1.26, # Ga
 1.22, # Ge
 1.20, # As
 1.16, # Se
 1.14, # Br
 1.89, # Kr
 2.16, # Rb
 1.91, # Sr
 1.62, # Y
 1.45, # Zr
 1.34, # Nb
 1.30, # Mo
 1.27, # Tc
 1.25, # Ru
 1.25, # Rh
 1.28, # Pd
 1.34, # Ag
 1.41, # Cd
 1.44, # In
 1.41, # Sn
 1.40, # Sb
 1.36, # Te
 1.33, # I
 1.31, # Xe
 2.35, # Cs
 1.98, # Ba
 1.25, # La
 1.65, # Ce
 1.65, # Pr
 1.64, # Nd
 1.63, # Pm
 1.62, # Sm
 1.85, # Eu
 1.61, # Gd
 1.59, # Tb
 1.59, # Dy
 1.58, # Ho
 1.57, # Er
 1.56, # Tm
 1.70, # Yb
 1.56, # Lu
 1.44, # Hf
 1.34, # Ta
 1.30, # W
 1.28, # Re
 1.26, # Os
 1.27, # Ir
 1.30, # Pt
 1.34, # Au
 1.49, # Hg
 1.48, # Tl
 1.47, # Pb
 1.46, # Bi
 1.53, # Po
 1.47, # At
 np.nan, # Rn
 np.nan, # Fr
 np.nan, # Ra
 np.nan, # Ac
 1.65, # Th
 np.nan, # Pa
 1.42, # U
 np.nan, # Np
 np.nan, # Pu
 np.nan, # Am
 np.nan, # Cm
 np.nan, # Bk
 np.nan, # Cf
 np.nan, # Es
 np.nan, # Fm
 np.nan, # Md
 np.nan, # No
 np.nan]) # Lw

# This data is from Ashcroft and Mermin.
reference_states = [\
    None, #X
    {'symmetry': 'diatom', 'd': 0.74}, #H
    {'symmetry': 'atom'}, #He
    {'symmetry': 'BCC', 'a': 3.49}, #Li
    {'symmetry': 'hcp', 'c/a': 1.567, 'a': 2.29}, #Be
    {'symmetry': 'Tetragonal', 'c/a': 0.576, 'a': 8.73}, #B
    {'symmetry': 'Diamond', 'a': 3.57},#C
    {'symmetry': 'diatom', 'd': 1.10},#N
    {'symmetry': 'diatom', 'd': 1.21},#O
    {'symmetry': 'diatom', 'd': 1.42},#F
    {'symmetry': 'fcc', 'a': 4.43},#Ne
    {'symmetry': 'BCC', 'a': 4.23},#Na
    {'symmetry': 'hcp', 'c/a': 1.624, 'a': 3.21},#Mg
    {'symmetry': 'fcc', 'a': 4.05},#Al
    {'symmetry': 'Diamond', 'a': 5.43},#Si
    {'symmetry': 'Cubic', 'a': 7.17},#P
    {'symmetry': 'Orthorhombic', 'c/a': 2.339, 'a': 10.47,'b/a': 1.229},#S
    {'symmetry': 'Orthorhombic', 'c/a': 1.324, 'a': 6.24, 'b/a': 0.718},#Cl
    {'symmetry': 'fcc', 'a': 5.26},#Ar
    {'symmetry': 'BCC', 'a': 5.23},#K
    {'symmetry': 'fcc', 'a': 5.58},#Ca
    {'symmetry': 'hcp', 'c/a': 1.594, 'a': 3.31},#Sc
    {'symmetry': 'hcp', 'c/a': 1.588, 'a': 2.95},#Ti
    {'symmetry': 'BCC', 'a': 3.02},#V
    {'symmetry': 'BCC', 'a': 2.88},#Cr
    {'symmetry': 'Cubic', 'a': 8.89},#Mn
    {'symmetry': 'BCC', 'a': 2.87},#Fe
    {'symmetry': 'hcp', 'c/a': 1.622, 'a': 2.51},#Co
    {'symmetry': 'fcc', 'a': 3.52},#Ni
    {'symmetry': 'fcc', 'a': 3.61},#Cu
    {'symmetry': 'hcp', 'c/a': 1.856, 'a': 2.66},#Zn
    {'symmetry': 'Orthorhombic', 'c/a': 1.695, 'a': 4.51, 'b/a': 1.001},#Ga
    {'symmetry': 'Diamond', 'a': 5.66},#Ge
    {'symmetry': 'Rhombohedral', 'a': 4.13, 'alpha': 54.10},#As
    {'symmetry': 'hcp', 'c/a': 1.136, 'a': 4.36},#Se
    {'symmetry': 'Orthorhombic', 'c/a': 1.307, 'a': 6.67, 'b/a': 0.672},#Br
    {'symmetry': 'fcc', 'a': 5.72},#Kr
    {'symmetry': 'BCC', 'a': 5.59},#Rb
    {'symmetry': 'fcc', 'a': 6.08},#Sr
    {'symmetry': 'hcp', 'c/a': 1.571, 'a': 3.65},#Y
    {'symmetry': 'hcp', 'c/a': 1.593, 'a': 3.23},#Zr
    {'symmetry': 'BCC', 'a': 3.30},#Nb
    {'symmetry': 'BCC', 'a': 3.15},#Mo
    {'symmetry': 'hcp', 'c/a': 1.604, 'a': 2.74},#Tc
    {'symmetry': 'hcp', 'c/a': 1.584, 'a': 2.70},#Ru
    {'symmetry': 'fcc', 'a': 3.80},#Rh
    {'symmetry': 'fcc', 'a': 3.89},#Pd
    {'symmetry': 'fcc', 'a': 4.09},#Ag
    {'symmetry': 'hcp', 'c/a': 1.886, 'a': 2.98},#Cd
    {'symmetry': 'Tetragonal', 'c/a': 1.076, 'a': 4.59},#In
    {'symmetry': 'Tetragonal', 'c/a': 0.546, 'a': 5.82},#Sn
    {'symmetry': 'Rhombohedral', 'a': 4.51, 'alpha': 57.60},#Sb
    {'symmetry': 'hcp', 'c/a': 1.330, 'a': 4.45},#Te
    {'symmetry': 'Orthorhombic', 'c/a': 1.347, 'a': 7.27, 'b/a': 0.659},#I
    {'symmetry': 'fcc', 'a': 6.20},#Xe
    {'symmetry': 'BCC', 'a': 6.05},#Cs
    {'symmetry': 'BCC', 'a': 5.02},#Ba
    {'symmetry': 'hcp', 'c/a': 1.619, 'a': 3.75},#La
    {'symmetry': 'fcc', 'a': 5.16},#Ce
    {'symmetry': 'hcp', 'c/a': 1.614, 'a': 3.67},#Pr
    {'symmetry': 'hcp', 'c/a': 1.614, 'a': 3.66},#Nd
    None,#Pm
    {'symmetry': 'Rhombohedral', 'a': 9.00, 'alpha': 23.13},#Sm
    {'symmetry': 'BCC', 'a': 4.61},#Eu
    {'symmetry': 'hcp', 'c/a': 1.588, 'a': 3.64},#Gd
    {'symmetry': 'hcp', 'c/a': 1.581, 'a': 3.60},#Th
    {'symmetry': 'hcp', 'c/a': 1.573, 'a': 3.59},#Dy
    {'symmetry': 'hcp', 'c/a': 1.570, 'a': 3.58},#Ho
    {'symmetry': 'hcp', 'c/a': 1.570, 'a': 3.56},#Er
    {'symmetry': 'hcp', 'c/a': 1.570, 'a': 3.54},#Tm
    {'symmetry': 'fcc', 'a': 5.49},#Yb
    {'symmetry': 'hcp', 'c/a': 1.585, 'a': 3.51},#Lu
    {'symmetry': 'hcp', 'c/a': 1.582, 'a': 3.20},#Hf
    {'symmetry': 'BCC', 'a': 3.31},#Ta
    {'symmetry': 'BCC', 'a': 3.16},#W
    {'symmetry': 'hcp', 'c/a': 1.615, 'a': 2.76},#Re
    {'symmetry': 'hcp', 'c/a': 1.579, 'a': 2.74},#Os
    {'symmetry': 'fcc', 'a': 3.84},#Ir
    {'symmetry': 'fcc', 'a': 3.92},#Pt
    {'symmetry': 'fcc', 'a': 4.08},#Au
    {'symmetry': 'Rhombohedral', 'a': 2.99, 'alpha': 70.45},#Hg
    {'symmetry': 'hcp', 'c/a': 1.599, 'a': 3.46},#Tl
    {'symmetry': 'fcc', 'a': 4.95},#Pb
    {'symmetry': 'Rhombohedral', 'a': 4.75, 'alpha': 57.14},#Bi
    {'symmetry': 'SC', 'a': 3.35},#Po
    None,#At
    None,#Rn
    None,#Fr
    None,#Ra
    {'symmetry': 'fcc', 'a': 5.31},#Ac
    {'symmetry': 'fcc', 'a': 5.08},#Th
    {'symmetry': 'Tetragonal', 'c/a': 0.825, 'a': 3.92},#Pa
    {'symmetry': 'Orthorhombic', 'c/a': 2.056, 'a': 2.85, 'b/a': 1.736},#U
    {'symmetry': 'Orthorhombic', 'c/a': 1.411, 'a': 4.72, 'b/a': 1.035},#Np
    {'symmetry': 'Monoclinic'},#Pu
    None,#Am
    None,#Cm
    None,#Bk
    None,#Cf
    None,#Es
    None,#Fm
    None,#Md
    None,#No
    None]#Lw
