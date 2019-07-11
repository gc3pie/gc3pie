#! /usr/bin/env python
#
"""
Generate the same input files structure as `gricomp.py`, but without
running any job.
"""
# Copyright (C) 2011, 2018  University of Zurich. All rights reserved.
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
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
# summary of user-visible changes
__changelog__ = """
  2011-06-20:
    * Copy the ``coord`` file into each generated ``ridft`` and ``ricc2`` directory.
    * Move the ``ricc2`` directory at the leaf of the generated tree.
  2011-06-09:
    * Forked off from the `gricomp.py` source code.
"""
__docformat__ = 'reStructuredText'


from __future__ import absolute_import, print_function
import fnmatch
import os
import os.path
import shutil
import sys

## interface to Gc3libs

import gc3libs
from gc3libs.cmdline import positive_int, valid_directory
from gc3libs.template import Template, expansions
import gc3libs.utils
from gc3utils.commands import GC3UtilsScript


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

def acceptable_ridft_basis_set(extra_args):
    """Define which combination of orbital and JK basis are valid."""
    def order(k): # small aux function
        return basis_set_names.index(extra_args[k])
    orb_basis_nr = order('ORB_BASIS')
    jkbas_basis_nr = order('RIJK_BASIS')
    # only allow a jkbas if it's not *earlier* than the orbital basis;
    # i.e. if the orbital basis is ``aug-cc-pVQZ`` then jkbas cannot
    # be ``aug-cc-pVTZ``.
    if orb_basis_nr > jkbas_basis_nr:
        return False
    # otherwise, the basis combination is acceptable
    return True

def acceptable_ricc2_basis_set(extra_args):
    """Define which combination of CABS and CBAS are valid."""
    def order(k): # small aux function
        return basis_set_names.index(extra_args[k])
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


## main

class CreateTMFilesScript(GC3UtilsScript):
    """
For each molecule defined in a ``coord`` file given on the
command-line, create TURBOMOLE's ``ridft`` and then ``ricc2`` input
files, with each possible combination of orbital and auxiliary basis
sets.

The list of orbital and auxiliary basis sets to try can be
controlled with the ``--bas``, ``--jkbas``, ``--cbas`` and
``--cabs`` options.
    """

    def __init__(self):
        GC3UtilsScript.__init__(
            self,
            version = '0.1',
            # TURBOMOLE's "coord" files are input
            input_filename_pattern = 'coord',
            )

    def setup_args(self):
        """
        Set up command-line parsing.
        """
        # option arguments
        self.add_param("--bas", metavar="LIST", action="append",
                       dest="bas", default=[],
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
        self.add_param("-m", "--memory",
                       dest="memory", type=positive_int, default=2000,
                       help="Memory (in MB) to set for TURBOMOLE jobs.")
        self.add_param("-o", "--output-directory", metavar='PATH',
                       dest="output_dir", type=valid_directory, default=os.getcwd(),
                       help="Create output files into directories rooted at PATH")
        # positional (mandatory) arguments
        self.add_param('args', nargs='+', metavar='COORDFILE',
                       help="Path to a `coord` file in TURBOMOLE format.")

    def parse_args(self):
        # collect the basis set names given to the ``--bas``,
        # ``--jkbas``, ``--cbas`` and ``--cabs`` options and make them
        # into properly formatted lists.

        if len(self.params.bas) == 0:
            self.params.bas = basis_set_names
        else:
            self.params.bas = str.join(',', self.params.bas).split(',')
        for name in self.params.bas:
            if name not in basis_set_names:
                raise gc3libs.exceptions.InvalidUsage(
                    "Unknown basis set name: '%s'." % name)

        if len(self.params.jkbas) == 0:
            self.params.jkbas = jkbasis_set_names
        else:
            self.params.jkbas = str.join(',', self.params.jkbas).split(',')
        for name in self.params.jkbas:
            if name not in basis_set_names:
                raise gc3libs.exceptions.InvalidUsage(
                    "Unknown basis set name: '%s'." % name)

        if len(self.params.cbas) == 0:
            self.params.cbas = cbasis_set_names
        else:
            self.params.cbas = str.join(',', self.params.cbas).split(',')
        for name in self.params.cbas:
            if name not in basis_set_names:
                raise gc3libs.exceptions.InvalidUsage(
                    "Unknown basis set name: '%s'." % name)

        if len(self.params.cabs) == 0:
            self.params.cabs = cabsis_set_names
        else:
            self.params.cabs = str.join(',', self.params.cabs).split(',')
        for name in self.params.cabs:
            if name not in basis_set_names:
                raise gc3libs.exceptions.InvalidUsage(
                    "Unknown basis set name: '%s'." % name)

        if self.params.memory < 1:
            raise gc3libs.exceptions.InvalidUsage(
                "Argument to option -m/--memory must be a positive integer;"
                " got %d instead" % self.params.memory)


    def _search_for_input_files(self, paths):
        """
        Recursively scan each location in list `paths` for files
        matching the `self.input_filename_pattern` glob pattern, and
        return the set of path names to such files.
        """
        inputs = set()

        pattern = self.input_filename_pattern
        # special case for '*.ext' patterns
        ext = None
        if pattern.startswith('*.'):
            ext = pattern[1:]
            # re-check for more wildcard characters
            if '*' in ext or '?' in ext or '[' in ext:
                ext = None
        #self.log.debug("Input files must match glob pattern '%s' or extension '%s'"
        #               % (pattern, ext))

        def matches(name):
            return (fnmatch.fnmatch(os.path.basename(name), pattern)
                    or fnmatch.fnmatch(name, pattern))
        for path in paths:
            self.log.debug("Now processing input path '%s' ..." % path)
            if os.path.isdir(path):
                # recursively scan for input files
                for dirpath, dirnames, filenames in os.walk(path):
                    for filename in filenames:
                        if matches(filename):
                            self.log.debug("Path '%s' matches pattern '%s',"
                                           " adding it to input list"
                                           % (os.path.join(dirpath, filename),
                                              pattern))
                            inputs.add(os.path.join(dirpath, filename))
            elif matches(path) and os.path.exists(path):
                self.log.debug("Path '%s' matches pattern '%s',"
                               " adding it to input list" % (path, pattern))
                inputs.add(path)
            elif ext is not None and not path.endswith(ext) and os.path.exists(path + ext):
                self.log.debug("Path '%s' matched extension '%s',"
                               " adding to input list"
                               % (path + ext, ext))
                inputs.add(os.path.realpath(path + ext))
            else:
                self.log.error("Cannot access input path '%s' - ignoring it.", path)
            #self.log.debug("Gathered input files: '%s'" % str.join("', '", inputs))

        return inputs


    def _make_define_in(self, path, contents):
        """
        Write `contents` to a file named ``define.in`` in directory
        `path`.  Return the full path to the written file.
        """
        define_in_filename = os.path.join(path, 'define.in')
        define_in_file = open(define_in_filename, 'w')
        define_in_file.write(str(contents))
        define_in_file.close()
        return define_in_filename


    def _make_turbomole_files(self, coord, ridft_in, ricc2_ins, work_dir):
        orb_basis = ridft_in._keywords['ORB_BASIS']
        rijk_basis = ridft_in._keywords['RIJK_BASIS']
        work_dir = os.path.join(work_dir,
                                'bas-%s/jkbas-%s' % (orb_basis, rijk_basis))
        gc3libs.utils.mkdir(work_dir)
        # run 1st pass in the `ridft` directory
        ridft_dir = os.path.join(work_dir, 'ridft')
        gc3libs.utils.mkdir(ridft_dir)
        ridft_coord = os.path.join(ridft_dir, 'coord')
        shutil.copyfile(coord, ridft_coord)
        ridft_define_in = self._make_define_in(ridft_dir, ridft_in)
        gc3libs.log.info("Created RIDFT input files in directory '%s'",
                         ridft_dir)
        # proceeed with 2nd pass
        for ricc2_in in ricc2_ins:
            cbas = ricc2_in._keywords['CBAS_BASIS']
            cabs = ricc2_in._keywords['CABS_BASIS']
            ricc2_dir = os.path.join(work_dir,
                                     'cbas-%s/cabs-%s/ricc2' % (cbas, cabs))
            gc3libs.utils.mkdir(ricc2_dir)
            shutil.copyfile(ridft_coord, ricc2_dir)
            ricc2_define_in = self._make_define_in(ricc2_dir, ricc2_in)
            gc3libs.log.info("Created RICC2 input files in directory '%s'",
                             ricc2_dir)


    def main(self):
        coords = self._search_for_input_files(self.params.args)
        for coord in coords:
            gc3libs.log.info("Processing input file '%s' ..." % coord)
            # XXX: how do we get a unique name for each coord?  for
            # now, assume the directory containing the `coord` file
            # gives the unique name
            name = os.path.basename(os.path.dirname(coord))

            ridft_define_in = Template(
                RIDFT_DEFINE_IN, acceptable_ridft_basis_set,
                TITLE=name,
                ORB_BASIS=self.params.bas,
                RIJK_BASIS=self.params.jkbas,
                RIDFT_MEMORY = [self.params.memory]
                ) # end of RIDFT template

            ricc2_define_in = Template(
                RICC2_DEFINE_IN, acceptable_ricc2_basis_set,
                # the ORB_BASIS will be derived from the RIDFT_DEFINE_IN template
                CBAS_BASIS=self.params.cbas,
                CABS_BASIS=self.params.cabs,
                RICC2_MEMORY = [self.params.memory],
                ) # end of RICC2 template

            for ridft_in in expansions(ridft_define_in):
                orb_basis = ridft_in._keywords['ORB_BASIS']
                self._make_turbomole_files(
                    coord,
                    ridft_in,
                    # ricc2_ins
                    list(expansions(ricc2_define_in,
                                    ORB_BASIS=orb_basis)),
                    os.path.join(self.params.output_dir, name))



# run script
if __name__ == '__main__':
    CreateTMFilesScript().run()
