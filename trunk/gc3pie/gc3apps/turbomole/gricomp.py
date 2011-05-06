#! /usr/bin/env python
#
"""
Driver script for running Turbomole basis benchmarks
on the SMSCG infrastructure.
"""
# Copyright (C) 2011 GC3, University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
__version__ = '$Revision$'
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
# summary of user-visible changes
__changelog__ = """
  2011-05-06:
    * Record RIDFT/RICC2 output into a `ridft.out`/`ricc2.out` file
      in the corresponding `output/` subdirectory.
    * Workaround for Issue 95: now we have complete interoperability
      with GC3Utils.
"""
__docformat__ = 'reStructuredText'


# ugly workaround for Issue 95,
# see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import gricomp


import ConfigParser
import csv
import math
import os
import os.path
import shutil
import sys
from texttable import Texttable
import types

## interface to Gc3libs

import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.application.turbomole import TurbomoleApplication, TurbomoleDefineApplication
from gc3libs.cmdline import SessionBasedScript
from gc3libs.dag import SequentialTaskCollection, ParallelTaskCollection
from gc3libs.template import Template, expansions
import gc3libs.utils


## support classes

# list basis set names (as accepted by "define") here;
# ORDER IS IMPORTANT: the `acceptable_*_basis_set` functions
# will only allow a jkbas, cbas or cabs if it's not *earlier*
# than the orbital basis; i.e. if the orbital basis is
# ``aug-cc-pVQZ`` then jkbas cannot be ``aug-cc-pVTZ``.
basis_set_names = [
    'aug-cc-pVTZ',
    'aug-cc-pVQZ',
    'aug-cc-pV5Z',
    ]

def acceptable_ridft_basis_set(kw):
    """Define which combination of orbital and JK basis are valid."""
    def order(k): # small aux function
        return basis_set_names.index(kw[k])
    orb_basis_nr = order('ORB_BASIS')
    jkbas_basis_nr = order('RIJK_BASIS')
    # only allow a jkbas if it's not *earlier* than the orbital basis;
    # i.e. if the orbital basis is ``aug-cc-pVQZ`` then jkbas cannot
    # be ``aug-cc-pVTZ``.
    if orb_basis_nr > jkbas_basis_nr:
        return False
    # otherwise, the basis combination is acceptable
    return True

def acceptable_ricc2_basis_set(kw):
    """Define which combination of CABS and CBAS are valid."""
    def order(k): # small aux function
        return basis_set_names.index(kw[k])
    orb_basis_nr = order('ORB_BASIS')
    cabs_basis_nr = order('CABS_BASIS')
    cbas_basis_nr = order('CBAS_BASIS')
    # only allow a cbas or cabs if it's not *earlier*
    # than the orbital basis; i.e. if the orbital basis is
    # ``aug-cc-pVQZ`` then cbas cannot be ``aug-cc-pVTZ``.
    if orb_basis_nr > cbas_basis_nr:
        return False
    if orb_basis_nr > cabs_basis_nr:
        return False
    # otherwise, the basis combination is acceptable
    return True

# TURBOMOLE's ``define`` input for the RIDFT step
RIDFT_DEFINE_IN = """\

${TITLE}
a coord
*
no
b all ${ORB_BASIS}
*
eht



scf
conv
8

ri
on

rijk
on
jkbas
b all ${RIJK_BASIS}
*
m ${RIDFT_MEMORY}

*
"""

# TURBOMOLE's ``define`` input for the RICC2 step
RICC2_DEFINE_IN = """\





cc
freeze
*
cbas
b all ${CBAS_BASIS}
*
memory ${RICC2_MEMORY}
denconv 1.0d-8
ricc2
mp2 energy only
*
f12
pairenergy on
*
cabs
b all ${CABS_BASIS}
*
*
*
"""

def _make_define_in(path, contents):
    """
    Write `contents` to a file named ``define.in`` in directory
    `path`.  Return the full path to the written file.
    """
    define_in_filename = os.path.join(path, 'define.in')
    define_in_file = open(define_in_filename, 'w')
    define_in_file.write(str(contents))
    define_in_file.close()
    return define_in_filename


class BasisSweepPasses(SequentialTaskCollection):
    """
    Build a two-step sequence:
      - first task is RIDFT with given coordinates and ``define.in`` file;
      - second task is a parallel collection of RICC2 that uses the output files from
        the first stage as input, plus a new ``define.in``.
    """
    def __init__(self, name, coord, ridft_in, ricc2_ins, work_dir,
                 grid=None, **kw):
        """
        Construct a new `BasisSweepPasses` sequential collection.

        :param str name: A string uniquely identifying this
        computation, to be used as a title in the TURBOMOLE input
        file.

        :param str coord: Path to the input ``coord`` file.

        :param str ridft_in: Path to the ``define.in`` file for the
        RIDFT step.

        :param ricc2_ins: Iterable, yielding the paths to the
        ``define.in`` file for each of the dependent RICC2 steps.

        :param str work_dir: Path to a directory where input files and
        results will be stored.
    
        """
        orb_basis = ridft_in._keywords['ORB_BASIS']
        rijk_basis = ridft_in._keywords['RIJK_BASIS']
        self.work_dir = os.path.join(work_dir,
                                     'bas-%s/jkbas-%s' % (orb_basis, rijk_basis))
        gc3libs.utils.mkdir(self.work_dir)
        # need to remove this, we override it both in pass1 and pass2
        if kw.has_key('output_dir'):
            del kw['output_dir']
        # run 1st pass in the `ridft` directory
        ridft_dir = os.path.join(self.work_dir, 'ridft')
        gc3libs.utils.mkdir(ridft_dir)
        ridft_define_in = _make_define_in(ridft_dir, ridft_in)
        pass1 = TurbomoleDefineApplication(
            'ridft', ridft_define_in, coord,
            output_dir = os.path.join(ridft_dir, 'output'),
            stdout = 'ridft.out', **kw)
        # remember for later
        self.name = name
        self.ricc2_ins = ricc2_ins
        self.extra = kw
        # init superclass
        SequentialTaskCollection.__init__(self, name, [pass1], grid)
        gc3libs.log.debug("Created RIDFT task '%s' (bas=%s, jkbas=%s) in directory '%s'",
                          name, orb_basis, rijk_basis, ridft_dir)


    def next(self, done):
        if done == 0:
            if self.tasks[0].execution.returncode != 0:
                rc = self.tasks[0].execution.returncode
                if rc is not None:
                    self.execution.returncode = rc
                return Run.State.TERMINATED
            # else, proceeed with 2nd pass
            pass2 = [ ]
            for ricc2_in in self.ricc2_ins:
                cbas = ricc2_in._keywords['CBAS_BASIS']
                cabs = ricc2_in._keywords['CABS_BASIS']
                ricc2_dir = os.path.join(self.work_dir,
                                         'ricc2/cbas-%s/cabs-%s' % (cbas, cabs))
                gc3libs.utils.mkdir(ricc2_dir)
                ricc2_define_in = _make_define_in(ricc2_dir, ricc2_in)
                pass2.append(
                    TurbomoleDefineApplication(
                        'ricc2', ricc2_define_in,
                        # the second pass builds on files defined in the first one
                        os.path.join(self.tasks[0].output_dir, 'coord'),
                        os.path.join(self.tasks[0].output_dir, 'control'),
                        os.path.join(self.tasks[0].output_dir, 'energy'),
                        os.path.join(self.tasks[0].output_dir, 'mos'),
                        os.path.join(self.tasks[0].output_dir, 'basis'),
                        os.path.join(self.tasks[0].output_dir, 'auxbasis'),
                        output_dir = os.path.join(ricc2_dir, 'output'),
                        stdout = 'ricc2.out',
                        **self.extra))
                gc3libs.log.debug("Created RICC2 task in directory '%s'",
                                  ricc2_dir)
            self.tasks.append(ParallelTaskCollection(self.name + '.pass2',
                                                     pass2, grid=self._grid))
            return Run.State.RUNNING
        else:
            # final exit status reflects the 2nd pass exit status
            self.execution.returncode = self.tasks[1].execution.returncode
            return self.tasks[1].execution.state


class BasisSweep(ParallelTaskCollection):
    """
    For each valid combination of bases, perform a RIDFT+RICC2
    analysis of the molecule given in `coord`.

    :param str title: A string to name this TURBOMOLE computation.
        Written unchanged into TURBOMOLE's ``define.in`` file, so it
        should only contain ASCII characters excluding control
        characters.

    :param str coord: Path to a file containing molecular coordinates
        in Turbomole format.

    :param list bases: Names of the orbital bases to sweep.

    :param list jkbases: Names of the RIJK bases to sweep.

    :param list cbases: Values for TURBOMOLE's `cbas` parameter to sweep.
    
    :param list cabses: Values for TURBOMOLE's `cabs` parameter to sweep.

    :param str work_dir: Path to a directory where input files and
        results will be stored.
    
    :param func valid1: A function taking a pair (orbital basis, jk
        basis) and returning `True` iff that combination is valid and
        should be analyzed.

    :param func valid2: A function taking a pair (cbas, cabs) and
        returning `True` iff that combination is valid and should be
        analyzed.
    """
    
    def __init__(self, title, coord, bases, jkbases, cbases, cabses, work_dir,
                 valid1=acceptable_ridft_basis_set,
                 valid2=acceptable_ricc2_basis_set,
                 grid=None, **kw):
        """
        Create a new tasks that runs several analyses in parallel, one
        for each accepted combination of orbital and RIJK basis.
        """
        kw.setdefault('memory', 2000) # XXX: check with `requested_memory`
        
        ridft_define_in = Template(
            RIDFT_DEFINE_IN, valid1,
            TITLE=title,
            ORB_BASIS=bases,
            RIJK_BASIS=jkbases,
            RIDFT_MEMORY = [kw['memory']]
            ) # end of RIDFT template

        ricc2_define_in = Template(
            RICC2_DEFINE_IN, valid2,
            # the ORB_BASIS will be derived from the RIDFT_DEFINE_IN template
            CBAS_BASIS=cbases,
            CABS_BASIS=cabses,
            RICC2_MEMORY = [kw['memory']],
            ) # end of RICC2 template

        tasks = [ ]
        for ridft in expansions(ridft_define_in):
            orb_basis = ridft._keywords['ORB_BASIS']
            tasks.append(
                BasisSweepPasses(
                    title + '.seq', coord, ridft,
                    list(expansions(ricc2_define_in,
                                    ORB_BASIS=orb_basis)),
                    work_dir, **kw))

        ParallelTaskCollection.__init__(self, title, tasks, grid)
            

## main

class GRICompScript(SessionBasedScript):
    """
For each molecule defined in a ``coord`` file given on the
command-line, run TURBOMOLE's ``ridft`` and then ``ricc2``
programs, with each possible combination of orbital and
auxiliary basis sets.

The list of orbital and auxiliary basis sets to try can be
controlled with the ``--bas``, ``--jkbas``, ``--cbas`` and
``--cabs`` options.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = '0.2',
            # TURBOMOLE's "coord" files are input
            input_filename_pattern = 'coord',
            )

    def setup_args(self):
        super(GRICompScript, self).setup_args()

        self.add_param("--bas", metavar="LIST", action="append",
                       dest="bas", default=['aug-cc-pVTZ', 'aug-cc-pVQZ', 'aug-cc-pV5Z'],
                       help="Comma-separated list of orbital bases to sweep."
                       " (Default: %(default)s")
        self.add_param("--jkbas", metavar="LIST", action="append",
                       dest="jkbas", default=['aug-cc-pVTZ', 'aug-cc-pVQZ', 'aug-cc-pV5Z'],
                       help="Comma-separated list of RIJK bases to sweep."
                       " (Default: %(default)s")
        self.add_param("--cbas", metavar="LIST", action="append",
                       dest="cbas", default=['aug-cc-pVTZ', 'aug-cc-pVQZ', 'aug-cc-pV5Z'],
                       help="Comma-separated list of `cbas` bases to sweep."
                       " (Default: %(default)s")
        self.add_param("--cabs", metavar="LIST", action="append",
                       dest="cabs", default=['aug-cc-pVTZ', 'aug-cc-pVQZ', 'aug-cc-pV5Z'],
                       help="Comma-separated list of `cabs` bases to sweep."
                       " (Default: %(default)s")


    def new_tasks(self, extra):
        coords = self._search_for_input_files(self.params.args)

        for coord in coords:
            # XXX: how do we get a unique name for each coord?  for
            # now, assume the directory containing the `coord` file
            # gives the unique name
            name = os.path.basename(os.path.dirname(coord))
            yield (name,
                   gricomp.BasisSweep, [
                       name,
                       coord,
                       self.params.bas,
                       self.params.jkbas,
                       self.params.cbas,
                       self.params.cabs,
                       self.make_directory_path(self.params.output, name),
                       ],
                   {})


# run script
if __name__ == '__main__':
    GRICompScript().run()
