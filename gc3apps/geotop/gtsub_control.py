#! /usr/bin/env python
#
#   Copyright (C) 2010 2013, 2019  University of Zurich. All rights reserved.
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
"""
Front-end script which runs multiple GEOTop simulations over
a presegmented geographical area (usually 18 segments). The
workflow is as follows:

- identify the segment in the global map,
- run GEOTop on the sub-parts of the segment,
- agregate the results into a set of output files,

It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gtsub_control --help`` for program usage instructions.

"""
__docformat__ = 'reStructuredText'


from __future__ import absolute_import, print_function
import os
import os.path
import shlex
import time
import sys
import tempfile
import shutil

import tarfile

import gc3libs
import gc3libs.exceptions
from gc3libs.utils import fgrep
from gc3libs import Application, Run, Task
from gc3libs.quantity import Memory, GB
from gc3libs.cmdline import SessionBasedScript, existing_directory, existing_file
from gc3libs.workflow import TaskCollection
from gc3libs.workflow import ParallelTaskCollection, SequentialTaskCollection
from pkg_resources import Requirement, resource_filename

GEOTOP_INPUT_ARCHIVE = "input.tgz"

class GTSubControlApplication(Application):

    # This part starts the different Applications for the
    # specified simulations boxes.

    def __init__(self, sim_box, inputs, **extra_args):


        # source the execution script
        run_gtsub_control = resource_filename(Requirement.parse("gc3pie"), "gc3libs/etc/run_gtsub_control.sh")
        os.chmod(run_gtsub_control,0o777)
        inputs[run_gtsub_control] = 'run_gtsub_control.sh'

        outputs = {
            './sim/result/':             'sim/result',
            './topoApp_complete.r.Rout': 'topoApp_complete.r.Rout',
            './src/TopoAPP/parfile.r':   'parfile.r',
        }

        Application.__init__(self,
                arguments = [ "./run_gtsub_control.sh", sim_box ],
                inputs = inputs,
                outputs = outputs,
                stdout = 'gt_sub.log',
                join=True,
                **extra_args)


class GTSubControlScript(SessionBasedScript):
    """
    GTSubControl Script is used for submiting the GeoTop Application
    on parametric basics. It takes as arguments the [root] location of the
    all the input files and the [nseq] containing the sequence of elements
    corrisponding to a single simulation box
    """
    version = '1.0'

    def setup_options(self):

        self.add_param("-p", "--parameter-file", dest="par_file",
                        type=existing_file,
                        default=None,
                        help="Indicate the parameter file to be used")

        # change default for the "-m"/"--memory-per-core" option
        self.actions['memory_per_core'].default = 7*GB

    def setup_args(self):
        self.add_param("root", type=existing_directory, help='The root directory where to script is looking for input data')
        self.add_param("nseq", type=str, help='The sequence number of sim boxes to be executed. Formats: INT:INT | INT,INT,INT,..,INT | INT')

    def parse_args(self):
        "Split the nseq argument and set sim_boxes parameters."
        try:
            if self.params.nseq.count(':') == 1:
                start, end = self.params.nseq.split(':')
                self.sim_boxes = range(int(start), int(end)+1)
            elif self.params.nseq.count(',') >= 1:
                simulations = self.params.nseq.split(',');
                self.sim_boxes = [ int(s) for s in simulations ]
            else:
                self.sim_boxes = [ int(self.params.nseq) ];
        except ValueError:
            raise gc3libs.exceptions.InvalidUsage(
                "Invalid argument '%s', use on of the following formats: INT:INT | INT,INT,INT,..,INT | INT " % (nseq,))

    def _prepare_tar(self, simulation_dir):
        """Prepare the input tarball."""
        try:
            gc3libs.log.debug("Compressing input folder in'%s' ", simulation_dir)
            cwd = os.getcwd()
            os.chdir(simulation_dir)
            # check if input archive already present. If yes delete and reuse it
            if os.path.isfile(GEOTOP_INPUT_ARCHIVE):
                try:
                    os.remove(GEOTOP_INPUT_ARCHIVE)
                except OSError, x:
                    gc3libs.log.error("Failed removing '%s': %s: %s",
                                      GEOTOP_INPUT_ARCHIVE, x.__class__, x.message)
                    pass
            tar = tarfile.open(GEOTOP_INPUT_ARCHIVE, "w:gz", dereference=True)
            tar.add('./src')
            tar.add('./sim/_master')
            tar.close()
            os.chdir(cwd)
            yield (tar.name, GEOTOP_INPUT_ARCHIVE)
        except Exception, x:
             gc3libs.log.error("Failed creating input archive '%s': %s: %s",
                                os.path.join(simulation_dir, GEOTOP_INPUT_ARCHIVE),
                                x.__class__,x.message)
             raise

    def _get_output_dir_naming(self, paramfile):
        """
        Get the strings needed for the naming of the output directory.
        """
        # parse the parfile and take "Nclust", "col", "beg"
        # and "end" values needed for output directory naming.

        output_dir_naming_strings = []

        # Get the nclust value from the parfile.r
        lines = list(gc3libs.utils.fgrep("Nclust=", paramfile))
        if len(lines) != 1:
          raise RuntimeError(
              "Expecting one and only one `Nclust=` line in %s, found %d"
              % (paramfile, len(lines)))
        line = lines[0].strip()
        start, end = line.split('=')
        if (end == "" or start == ""):
            raise ValueError("Nclust option in the parfile.r is not properly set."
                             " It is set like %s, must be Nclust=Value." % ( line ))
        nclust = "Nc"+end
        output_dir_naming_strings.append(nclust)

        # Get the col value from the parfile.r
        lines = list(gc3libs.utils.fgrep("col=", paramfile))
        if len(lines) != 1:
          raise RuntimeError(
              "Expecting one and only one `col=` line in %s, found %d"
              % (paramfile, len(lines)))
        line = lines[0].strip()
        start, end = line.split('=')
        if (end == "" or start == ""):
            raise ValueError("col option in the parfile.r is not properly set."
                            " It is set like %s, must be col=Value." % ( line ))
        col = end
        col = col.replace('"','')
        output_dir_naming_strings.append(col)

        # Get the beginning value out of the "beg" line
        lines = list(gc3libs.utils.fgrep("beg <-", paramfile))
        if len(lines) != 1:
          raise RuntimeError(
              "Expecting one and only one `beg=` line in %s, found %d"
              % (paramfile, len(lines)))
        line = lines[0].strip()
        line_elements = line.split(' ')
        beg = line_elements[2].replace('"','')
        beg = beg.replace('/','')
        output_dir_naming_strings.append(beg)

        # Get the ending value out of the "end" line
        lines = list(gc3libs.utils.fgrep("end <-", paramfile))
        if len(lines) != 1:
          raise RuntimeError(
              "Expecting one and only one `end=` line in %s, found %d"
              % (paramfile, len(lines)))
        line = lines[0].strip()
        line_elements = line.split(' ')
        end = line_elements[2].replace('"','')
        end = end.replace('/','')
        output_dir_naming_strings.append(end)

        return output_dir_naming_strings

    def new_tasks(self, extra):

        # Call the _prepare_tar method which recreates
        # the tarball each time.
        inputs = dict(self._prepare_tar(self.params.root))

        # Manage the parfile.r and call the _get_output_dir_formatting
        # method which gives back the strings needed for the output dir
        # naming.
        paramfile = self.params.root + '/src/TopoAPP/parfile.r'
        if self.params.par_file:
                inputs[self.params.par_file] = 'parfile.r'
                paramfile = self.params.par_file

        out_dir_nm = self._get_output_dir_naming(paramfile)

        # create tasks for the specified sumulation boxes
        for sim_box in self.sim_boxes:
            extra_args = extra.copy()
            ## specifiedcify the jobname by root and number of box seq
            jobname = ('GTSC_nS%s_%s_%s_%s_%s'
                        % (sim_box, out_dir_nm[0], out_dir_nm[1],
                            out_dir_nm[2], out_dir_nm[3]))
            extra_args['jobname'] = jobname
            yield GTSubControlApplication(
                sim_box, # pass the simulation box number
                inputs.copy(),
                **extra_args)

## main: run tests

if "__main__" == __name__:
        import gtsub_control
        gtsub_control.GTSubControlScript().run()
