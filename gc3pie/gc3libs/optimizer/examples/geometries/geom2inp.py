#! /usr/bin/env python

import os
import numpy as np

# SCRIPT TO CONVERT A 2-DIM. NUMPY ARRAY CONTAINING A SET OF MOLECULAR GEOMETRIES
# INTO A SET OF INPUT FILES FOR GAMESS STORED IN A COMMON FOLDER

# defining template for input files containing all job parameters to keep
# constant during the entire search
inptmpl = []
inptmpl.append("""
 $CONTRL SCFTYP=RHF RUNTYP=ENERGY $END
 $BASIS GBASIS=STO NGAUSS=3 $END
 $DATA 
Title
C1
""")
#inptmpl.append("""
 #$CONTRL SCFTYP=UHF DFTTYP=BP86 RUNTYP=ENERGY $END
 #$SYSTEM MEMDDI=1 PARALL=.TRUE. $END
 #$BASIS  GBASIS=N21 NGAUSS=3 NDFUNC=1 NPFUNC=1 $END
 #$DFT NRAD0=96 NLEB0=302 $END
 #$GUESS  GUESS=HUCKEL $END
 #$DATA
#Water
#C1
#""")
inptmpl.append('$END')

inpfl = 'H2CO3'
natm = 6
element = ('C', 'O', 'O', 'O', 'H', 'H')
nchrg = (6.0, 8.0, 8.0, 8.0, 1.0, 1.0)
ngeom = 1
geom = np.array([[ 1.,1.,1.,2.,2.,2.,3.,3.,3.,4.,4.,4.,5.,5.,5.,6.,6.,6.]])
dirname = 'blub'

# creating directory for input files for current set of geometries
if not os.path.exists(dirname):
	os.makedirs(dirname)

# GENERATING INPUT FILES FOR GAMESS

for i in xrange(ngeom):
	# taking ith geometry / molecule
	geomstr = ''
	for j in xrange(natm):
		# taking jth atom of current molecule
		geomstr = geomstr + element[j] + '  ' + str(nchrg[j]) + \
		'  ' + '%10.8f'%geom[i,3*j] + '  ' + '%10.8f'%geom[i,3*j+1] + \
		'  ' + '%11.8f'%geom[i,3*j+2] + '\n'
	file = open(os.path.join(dirname, inpfl+str(i)+'.inp'), 'w')
	file.write(inptmpl[0] + geomstr + inptmpl[1])
	file.close()
	
