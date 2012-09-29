#! /usr/bin/env python
#
"""
Recursive substitution into templates.
"""
__docformat__ = 'reStructuredText'


import os
import os.path
import string
import sys

from optparse import OptionParser

from gc3libs.template import Template, expansions

PROG = os.path.basename(sys.argv[0])

cmdline = OptionParser("%s [options] BASENAME" % PROG,
                       description="""
Create a group of input files by recursive substitution
into a template.  The template is hard-coded into the script.
Generated files follow the pattern specified by BASENAME,
with a sequential number appended.
""")
cmdline.add_option('-w', '--width', action='store', type=int, default=5, metavar='NUM',
                   help="How many digits to use for numbering output files (default: %default).")
(options, args) = cmdline.parse_args()


## Timm's GAMESS template


def match_ispher_with_basis(kw):
    """Ensure we have ISPHER=1 if (and only if) the basis set requires it."""
    def val(k): # small aux function
        return str(kw[k])
    if (kw.has_key('SIMPLEBASIS')
        and val('SIMPLEBASIS') in [ "CCD", "CCT", "CCQ", "CC5", "CC6" ]):
        # CCn requires ISPHER=1
        if val('ISPHER') == "1":
            return True
        else:
            return False
    else:
        # with simple bases, default ISPHER is OK
        if val('ISPHER') == "1":
            return False
        else:
            return True

def acceptable_gbasis_and_ngauss(kw):
    """Define which combination of GBASIS and NGAUSS are valid."""
    def val(k): # small aux function
        return str(kw[k])
    # a. N21 with all NGAUSS except 5
    if val('GBASIS') == 'N21' and val('NGAUSS') == '5':
        return False
    # b. N31 with all NGAUSS except 3
    if val('GBASIS') == 'N31' and val('NGAUSS') == '3':
        return False
    # c. N311 only with NGAUSS=6
    if val('GBASIS') == 'N311' and val('NGAUSS') != '6':
        return False
    # d. NGAUSS=2 only with GBASIS=STO
    if val('NGAUSS') == '2' and val('GBASIS') != 'STO':
        return False
    # e. NGAUSS=3 only with GBASIS=N21
    if val('NGAUSS') == '3' and val('GBASIS') != 'N21':
        return False
    # f. NGAUSS=4 only without GBASIS=N311 (redundant with rule c. above)
    if val('NGAUSS') == '4' and val('GBASIS') != 'N311':
        return False
    # g. NGAUSS=5 only with GBASIS=N31
    if val('NGAUSS') == '5' and val('GBASIS') != 'N31':
        return False
    # h. Use NFFUNC and NDFUNC only with N31,N311 and NGAUSS=5,6
    if ((val('NFFUNC') == 'NFFUNC=1' or val('NDFUNC') == 'NDFUNC=1')
        and (val('NGAUSS') not in ['5', '6'] or val('GBASIS') not in ['N31', 'N311'])):
            return False
    # j. every other combination is ok
    return True

GAMESS_INP = Template("""
 $$CONTRL RUNTYP=ENERGY MAXIT=1 UNITS=BOHR $$END
 $$CONTRL ${SCF} ISPHER=${ISPHER} $$END
 $$ACCURACY ITOL=${ITOL} ILOAD=${ILOAD} $$END
 $$SYSTEM MWORDS=10 $$END
 $$BASIS ${BASIS} $$END
 $$GUESS GUESS=HUCKEL $$END

 $$DATA
${GEOMETRY}
 $$END
""",
                      match_ispher_with_basis,
                      GEOMETRY = [
        # beware that GAMESS won't run if there is a blank
        # line at the end of the $DATA section; so be sure
        # to put the closing `"""` at the very end of the
        # last $DATA line
        """Water
C1
O   8.0        0.0              0.0                    0.0
H   1.0        0.0              1.428036               1.0957706
H   1.0        0.0             -1.428036               1.0957706""",
        """Methane
Td

C     6.0   0.0            0.0            0.0
H     1.0   0.6252197764   0.6252197764   0.6252197764""",
        ], # end of GEOMETRY
                      SCF = [Template("SCFTYP=${SCFTYP}", # "MPLEVL=${MPLEVL}", # NODFT
                                      SCFTYP = ["RHF", "ROHF", "UHF"],
                                      ),
                             # Template("SCFTYP=${SCFTYP} DFTTYP=${DFTTYP}", # WITHDFT
                             #          SCFTYP = ["RHF", "ROHF", "UHF"],
                             #          DFTTYP = ["SVWN", "BLYP", "B97-D", "B3LYP", "revTPSS", "TPSSh", "M06"],
                             #          ),
                      ], # end of SCF
                      #DIRSCF = [".TRUE.", ".FALSE."],
                      ITOL = [20, 15, 10],
                      ILOAD = [9, 7, 5],
                      BASIS = [Template("GBASIS=${SIMPLEBASIS}",
                                        SIMPLEBASIS = ["MINI", "MIDI", "DZV", "TZV",
                                                       "CCD", "CCT", "CCQ", "CC5", "CC6"]),
                               Template("GBASIS=${GBASIS} NGAUSS=${NGAUSS} ${NPFUNC} ${NDFUNC} ${NFFUNC}",
                                        acceptable_gbasis_and_ngauss,
                                        GBASIS = ["STO", "N21", "N31", "N311"],
                                        NGAUSS = [2, 3, 4, 5, 6],
                                        NPFUNC = ["", "NPFUNC=1"],
                                        NDFUNC = ["", "NDFUNC=1"],
                                        NFFUNC = ["", "NFFUNC=1"],
                                        ),
                               ], # end of BASIS
                      ISPHER = [-1, +1], # 0 is also a legal value; -1 is default
                     ) # end of GAMESS_INP



## main

if "__main__" == __name__:
    fmt = "gamess_%0" + str(options.width) + "d"
    for n, t in enumerate(expansions(GAMESS_INP)):
        if len(args) == 0:
            # no BASENAME, print to stdout
            print ("==== Input file #"+ (fmt % n) +" ====")
            print (t)
        else:
            if t._keywords.has_key('DFTTYP'):
                dfttyp = t._keywords['DFTTYP']
            else:
                dfttyp = 'NODFT'
            scftyp = t._keywords['SCFTYP']
            dirname = os.path.join(dfttyp, scftyp)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            filename = os.path.join(dirname, args[0] + (fmt % n) + '.inp')
            output = open(filename, 'w+')
            output.write("%s\n" % t)
            output.close()
