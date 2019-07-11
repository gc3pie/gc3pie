#! /usr/bin/env python
#
"""
Driver script for running Turbomole basis benchmarks
on the SMSCG infrastructure.
"""
# Copyright (C) 2011-2012  University of Zurich. All rights reserved.
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

from __future__ import absolute_import, print_function

__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
# summary of user-visible changes
__changelog__ = """
  2011-06-20:
    * Copy the ``coord`` file into each generated ``ridft`` and ``ricc2`` directory.
    * Move the ``ricc2`` directory at the leaf of the generated tree.
  2011-05-06:
    * Record RIDFT/RICC2 output into a `ridft.out`/`ricc2.out` file
      in the corresponding `output/` subdirectory.
    * Workaround for Issue 95: now we have complete interoperability
      with GC3Utils.
"""
__docformat__ = 'reStructuredText'


# workaround Issue 95, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gricomp
    gricomp.GRICompScript().run()


# stdlib imports
import ConfigParser
import csv
import math
import os
import os.path
import shutil
import sys
import types

# interface to Gc3libs
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.application.turbomole import TurbomoleApplication, TurbomoleDefineApplication
from gc3libs.cmdline import SessionBasedScript
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import StagedTaskCollection, ParallelTaskCollection
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


class LocalApplication(Application):
    """
    Force an Application to run on `localhost`.
    """
    def compatible_resources(self, resources):
        """
        Only `localhost` matches.
        """
        return [ lrms for lrms in resources if lrms._resource.name == 'localhost' ]


class NonLocalApplication(Application):
    """
    Force an Application *not* to run on `localhost`.
    """
    def compatible_resources(self, resources):
        """
        Only `localhost` does not match.
        """
        return [ lrms for lrms in resources if lrms._resource.name != 'localhost' ]


class NonLocalTurbomoleApplication(NonLocalApplication, TurbomoleApplication):
    """
    Like `TurbomoleApplication`, but force it *not* to run on `localhost`.
    """
    pass


class NonLocalTurbomoleDefineApplication(NonLocalApplication, TurbomoleDefineApplication):
    """
    Like `TurbomoleDefineApplication`, but force it *not* to run on `localhost`.
    """
    pass


class XmlLintApplication(LocalApplication):
    # xmllint --schema /links/xml-recources/xml-validation/CML3scheme.xsd ./control_*.xml 1>/dev/null 2>validation.log
    def __init__(self, turbomole_output_dir, output_dir,
                 validation_log='validation.log',
                 schema='/links/xml-recources/xml-validation/CML3scheme.xsd',
                 **extra_args):
        self.validation_log = validation_log
        # find the control_*.xml in the TURBOMOLE output directory
        control_xml = None
        for filename in os.listdir(turbomole_output_dir):
            if filename.startswith('control_') and filename.endswith('.xml'):
                control_xml = os.path.join(turbomole_output_dir, filename)
                break
        if control_xml is None:
            raise ValueError("Cannot find a 'control_*.xml' file in directory '%s'"
                             % turbomole_output_dir)
        gc3libs.Application.__init__(
            self,
            executable='xmllint',
            arguments = [ '--schema', schema, control_xml ],
            inputs = [ control_xml ],
            outputs = [ validation_log ],
            output_dir = output_dir,
            stdout = None,
            stderr = validation_log,
            **extra_args)

    def terminated(self):
        validation_logfile = open(os.path.join(self.output_dir, self.validation_log), 'r')
        validation_log_contents = validation_logfile.read()
        validation_logfile.close()
        if 'validates' in validation_log_contents:
            self.execution.returncode = (0, 0) # SUCCESS
        else:
            self.execution.returncode = (0, 1) # FAIL


class XmlDbApplication(LocalApplication):
    # /opt/eXist/bin/client.sh -u fox -m "/db/home/fox/${projectdir}" -p control_* -P 'tueR!?05' -s 1>/dev/null 2>&1
    def __init__(self, turbomole_output_dir, output_dir, db_dir, db_user, db_pass, db_log='db.log', **extra_args):
        # find the control_*.xml in the TURBOMOLE output directory
        control_xml = None
        for filename in os.listdir(turbomole_output_dir):
            if filename.startswith('control_') and filename.endswith('.xml'):
                control_xml = os.path.join(turbomole_output_dir, filename)
                break
        if control_xml is None:
            raise ValueError("Cannot find a 'control_*.xml' file in directory '%s'"
                             % turbomole_output_dir)
        # pre-process control_*.xml to remove the 'xmlns' part,
        # which confuses eXist
        to_remove = 'xmlns="http://www.xml-cml.org/schema"'
        os.rename(control_xml, control_xml + '.orig')
        control_xml_file_in = open(control_xml + '.orig', 'r')
        control_xml_file_out = open(control_xml, 'w')
        for line in control_xml_file_in:
            line = line.replace(to_remove, '')
            control_xml_file_out.write(line)
        control_xml_file_in.close()
        control_xml_file_out.close()

        gc3libs.Application.__init__(
            self,
            executable='/opt/eXist/bin/client.sh',
            arguments = [
                '-u', db_user,
                '-P', db_pass,
                '-m', db_dir,
                '-p', control_xml,
                '-s',
            ],
            inputs = [ control_xml ],
            outputs = [ db_log ],
            output_dir = output_dir,
            stdout = db_log,
            stderr = db_log,
            join=True,
            **extra_args)


class TurbomoleAndXmlProcessingPass(StagedTaskCollection):
    """
    Run a TURBOMOLE application, then validate the 'control_*.xml'
    file produced, and store it into an eXist database.

    """
    def __init__(self, name, turbomole_application, output_dir,
                 db_dir, db_user, db_pass,
                 **extra_args):
        self.turbomole_application = turbomole_application
        self.output_dir = output_dir
        self.db_dir = db_dir
        self.db_user = db_user
        self.db_pass = db_pass
        self.extra = extra_args
        # init superclass
        StagedTaskCollection.__init__(self, name)


    def stage0(self):
        """Run the TURBOMOLE application specified to the constructor."""
        return self.turbomole_application


    def stage1(self):
        """Run 'xmllint'."""
        # terminate if first stage was unsuccessful
        rc = self.tasks[0].execution.returncode
        if rc is not None and rc != 0:
            return rc
        self.turbomole_output_dir = self.tasks[0].output_dir
        return XmlLintApplication(self.turbomole_output_dir,
                                  os.path.join(self.output_dir, 'xmllint'),
                                  **self.extra)


    def stage2(self):
        """Run 'eXist/client.sh'."""
        # terminate if first stage was unsuccessful
        rc = self.tasks[1].execution.returncode
        if rc is not None and rc != 0:
            return rc
        return XmlDbApplication(self.turbomole_output_dir,
                                os.path.join(self.output_dir, 'eXist'),
                                self.db_dir, self.db_user, self.db_pass,
                                **self.extra)


class BasisSweepPasses(StagedTaskCollection):
    """
    Build a two-step sequence:
      - first task is RIDFT with given coordinates and ``define.in`` file;
      - second task is a parallel collection of RICC2 that uses the output files from
        the first stage as input, plus a new ``define.in``.
    """
    def __init__(self, name, coord, ridft_in, ricc2_ins, work_dir, **extra_args):
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
        # need to remove this, we override it both in pass1 and pass2
        if extra_args.has_key('output_dir'):
            del extra_args['output_dir']
        # remember for later
        self.orb_basis = ridft_in._keywords['ORB_BASIS']
        self.rijk_basis = ridft_in._keywords['RIJK_BASIS']
        self.work_dir = os.path.join(work_dir, 'bas-%s/jkbas-%s'
                                     % (self.orb_basis, self.rijk_basis))
        self.name = name
        self.coord = coord
        self.ridft_in = ridft_in
        self.ricc2_ins = ricc2_ins
        self.extra = extra_args
        # init superclass
        StagedTaskCollection.__init__(self, name)


    def stage0(self):
        """
        Run a RIDFT job for the BAS/JKBAS combination given by the
        keywords ``ORB_BASIS`` and ``RIJK_BASIS`` in
        `self.ridft_in`.

        """
        # run 1st pass in the `ridft` directory
        gc3libs.utils.mkdir(self.work_dir)
        ridft_dir = os.path.join(self.work_dir, 'ridft')
        gc3libs.utils.mkdir(ridft_dir)
        shutil.copyfile(self.coord, os.path.join(ridft_dir, 'coord'))
        ridft_define_in = _make_define_in(ridft_dir, self.ridft_in)
        ridft_output_dir =  os.path.join(ridft_dir, 'output')
        # application to run in pass 1
        gc3libs.log.debug("Creating RIDFT task '%s' (bas=%s, jkbas=%s) in directory '%s'",
                          self.name, self.orb_basis, self.rijk_basis, ridft_dir)
        # RIDFT expected to complete in 1 hour regardless
        extra = self.extra.copy()
        extra.setdefault('requested_walltime', 1*hours)
        return TurbomoleAndXmlProcessingPass(
            # job name
            ('ridft-%s-%s-%s' % (self.name, self.orb_basis, self.rijk_basis)),
            # TURBOMOLE application to run
            NonLocalTurbomoleDefineApplication(
                'ridft', ridft_define_in, self.coord,
                output_dir = ridft_output_dir,
                stdout = 'ridft.out',
                **extra),
            # base output directory for xmllint and eXist jobs
            ridft_dir,
            # DB parameters
            # FIXME: make these settable on the command-line
            db_dir='/db/home/fox/gricomp', db_user='fox', db_pass='tueR!?05',
            # TaskCollection required params
            **self.extra)


    def stage1(self):
        """
        Run a RICC2 job for each valid CBAS/CABS basis combination,
        re-using the results from RIDFT in `stage0`.

        If RIDFT failed, exit immediately.
        """
        # terminate if first stage was unsuccessful
        rc = self.tasks[0].execution.returncode
        if rc is not None and rc != 0:
            return rc
        # else, proceeed with 2nd pass
        pass2 = [ ]
        ridft_coord = os.path.join(self.tasks[0].turbomole_output_dir, 'coord')
        for ricc2_in in self.ricc2_ins:
            cbas = ricc2_in._keywords['CBAS_BASIS']
            cabs = ricc2_in._keywords['CABS_BASIS']
            ricc2_dir = os.path.join(self.work_dir,
                                     'cbas-%s/cabs-%s/ricc2' % (cbas, cabs))
            gc3libs.utils.mkdir(ricc2_dir)
            shutil.copyfile(ridft_coord, ricc2_dir)
            ricc2_define_in = _make_define_in(ricc2_dir, ricc2_in)
            ricc2_output_dir = os.path.join(ricc2_dir, 'output')
            # guess duration of the RICC2 job
            extra = self.extra.copy()
            if ('aug-cc-pV5Z' == self.orb_basis
                or 'aug-cc-pV5Z' == self.rijk_basis
                or 'aug-cc-pV5Z' == cbas
                or 'aug-cc-pV5Z' == cabs):
                extra.setdefault('requested_walltime', 4*hours)
            else:
                extra.setdefault('requested_walltime', 1*hours)
            pass2.append(
                TurbomoleAndXmlProcessingPass(
                    # job name
                    ('ricc2-%s-%s-%s' % (self.name, cbas, cabs)),
                    # TURBOMOLE application to run
                    NonLocalTurbomoleDefineApplication(
                        'ricc2', ricc2_define_in,
                        # the second pass builds on files defined in the first one
                        os.path.join(ricc2_dir, 'coord'),
                        os.path.join(self.tasks[0].turbomole_output_dir, 'control'),
                        os.path.join(self.tasks[0].turbomole_output_dir, 'energy'),
                        os.path.join(self.tasks[0].turbomole_output_dir, 'mos'),
                        os.path.join(self.tasks[0].turbomole_output_dir, 'basis'),
                        os.path.join(self.tasks[0].turbomole_output_dir, 'auxbasis'),
                        output_dir = ricc2_output_dir,
                        stdout = 'ricc2.out',
                        **extra),
                    os.path.join(ricc2_output_dir, 'xml-processing'),
                    # DB parameters
                    # FIXME: make these settable on the command-line
                    db_dir='/db/home/fox/gricomp', db_user='fox', db_pass='tueR!?05',
                    # TaskCollection required params
                    **self.extra))
            gc3libs.log.debug("Created RICC2 task in directory '%s'", ricc2_dir)
        return (ParallelTaskCollection(self.name + '.pass2', pass2))


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
                 **extra_args):
        """
        Create a new tasks that runs several analyses in parallel, one
        for each accepted combination of orbital and RIJK basis.
        """
        extra_args.setdefault('memory', 2000) # XXX: check with `requested_memory`

        ridft_define_in = Template(
            RIDFT_DEFINE_IN, valid1,
            TITLE=title,
            ORB_BASIS=bases,
            RIJK_BASIS=jkbases,
            RIDFT_MEMORY = [extra_args['memory']]
            ) # end of RIDFT template

        ricc2_define_in = Template(
            RICC2_DEFINE_IN, valid2,
            # the ORB_BASIS will be derived from the RIDFT_DEFINE_IN template
            CBAS_BASIS=cbases,
            CABS_BASIS=cabses,
            RICC2_MEMORY = [extra_args['memory']],
            ) # end of RICC2 template

        tasks = [ ]
        for ridft in expansions(ridft_define_in):
            orb_basis = ridft._keywords['ORB_BASIS']
            tasks.append(
                BasisSweepPasses(
                    title + '.seq', coord, ridft,
                    list(expansions(ricc2_define_in,
                                    ORB_BASIS=orb_basis)),
                    work_dir, **extra_args))
        ParallelTaskCollection.__init__(self, title, tasks)


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
