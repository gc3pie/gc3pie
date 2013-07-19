#! /usr/bin/env python
#
# Copyright (C) 2012, 2013, GC3, University of Zurich. All rights reserved.
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
__docformat__ = 'reStructuredText'
__version__ = '$Revision$'


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

GEOTOP_INPUT_ARCHIVE = "input.tgz"

class GTSubControllApplication(Application):

    # This part starts the different Applications for the
    # specified simulations boxes.

    def __init__(self, sim_box, inputs, **extra_args):

        # prepare execution script from command
        execution_script = """
#!/bin/sh

# check tar
RES=`command tar --version 2>&1`
if [ $? -ne 0 ]; then
     echo "[tar command is not present]"
     echo $RES
     exit 1
else
     echo "[OK, tar command is present, continue...]"
fi

# Untar the input files in the
tar -xzvf input.tgz -C .
if [ $? -ne 0 ]; then
     echo "[untar failed]"
     exit 1
else
    echo "[OK, untar of the input archive command finished successfully, continue...]"
fi
# Remove input.tgz
rm input.tgz

# create the sym link to the topo directory
echo -n "Checking Input folder..."
if [ -d ./sim/_master/topo ]; then
    echo "[${PWD}/sim/_master/topo]"
elif [ -d ${HOME}/sim/_master/topo ]; then
    ln -s ${HOME}/sim/_master/topo ./sim/_master/topo
    if [ $? -ne 0 ]; then
        echo "[creating sim link to the available topo directory has failed]"
        exit 1
    else
        echo "[${HOME}/sim_master/topo]"
        echo "[OK, sym link to the available topo data has been created, continue...]"
    fi
else
    echo "[FAILED: no folder found in './sim/_master/topo' nor in '${HOME}/sim/_master/topo']"
    exit 1
fi

# sed the root dir be used
sed -i -e "s|root=|root='"$PWD"\/'|g" ./src/TopoAPP/topoApp_complete.r
if [ $? -ne 0 ]; then
     echo "[sed the ROOT directory in topoApp_complete.r failed]"
     exit 1
else
    echo "[OK, changing the root directory in the topoApp_complete.r, continue...]"
fi

# copy parfile if it is present in the root directory
if [ -f ./parfile.r ]; then
        echo "[It seems that a different parfile has been passed to the application, replacing the current one...]"
        cp ./parfile.r ./src/TopoAPP/
        if [ $? -ne 0 ]; then
            echo "[Some problems copying the specified parfile.r occured, please check...]"
            exit 1
        fi
else
    echo "[No paramter file has been specified, using the one passed through the input.tgz file]"
fi

# sed the box sequence to be used
sed -i -e 's/nboxSeq=/nboxSeq=%s/g' ./src/TopoAPP/parfile.r
if [ $? -ne 0 ]; then
     echo "[sed the nboxSeq sequence failed]"
     exit 1
else
    echo "[OK, changing the nboxSeq has been done correctly, continue...]"
fi

# check R
RES=`command R --version 2>&1`
if [ $? -ne 0 ]; then
     echo "[failed]"
     echo $RES
     exit 1
else
     echo "[OK, R command is present, starting R script]"
fi
R CMD BATCH --no-save --no-restore ./src/TopoAPP/topoApp_complete.r
        """ % (sim_box)

        # create script file
        (handle, self.tmp_filename) = tempfile.mkstemp(prefix='gc3pie-gtsub_control', suffix=str(sim_box))
        fd = open(self.tmp_filename,'w')
        fd.write(execution_script)
        fd.close()
        os.chmod(fd.name,0777)

        inputs[fd.name] = 'gtsub_control.sh'

        outputs = {}
        outputs['./sim/result/'] = 'sim/result'
        outputs['./topoApp_complete.r.Rout'] = 'topoApp_complete.r.Rout'
        outputs ['./src/TopoAPP/parfile.r'] = 'parfile.r'

        Application.__init__(self,
                arguments = ['./gtsub_control.sh'],
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

    def _scan_and_tar(self, simulation_dir):
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
        for line in gc3libs.utils.fgrep("Nclust=", paramfile):
            line = line.strip()
            start, end = line.split('=')
            if (end == "" or start == ""):
                raise ValueError("Nclust option in the parfile.r is not properly set."
                                 " It is set like %s, must be Nclust=Value." % ( line ))
            nclust = "Nc"+end
            output_dir_naming_strings.append(nclust)
            break

        # Get the col value from the parfile.r
        for line in gc3libs.utils.fgrep("col=", paramfile):
            line = line.strip()
            start, end = line.split('=')
            if (end == "" or start == ""):
                raise ValueError("col option in the parfile.r is not properly set."
                                " It is set like %s, must be col=Value." % ( line ))
            col = end
            col = col.replace('"','')
            output_dir_naming_strings.append(col)
            break

        # Get the beginning value out of the "beg" line
        for i in gc3libs.utils.fgrep("beg <-", paramfile):
            line_elements = i.split(' ')
            beg = line_elements[2].replace('"','')
            beg = beg.replace('/','')
            output_dir_naming_strings.append(beg)
            break

        # Get the ending value out of the "end" line
        for i in gc3libs.utils.fgrep("end <-", paramfile):
            line_elements = i.split(' ')
            end = line_elements[2].replace('"','')
            end = end.replace('/','')
            output_dir_naming_strings.append(end)
            break

        return output_dir_naming_strings

    def new_tasks(self, extra):

        # Call the _scan_and_tar method which recreates
        # the tarball each time.
        inputs = dict(self._scan_and_tar(self.params.root))

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
            jobname = 'GTSC' + '_nS' + str(sim_box)+'_'+out_dir_nm[0]+'_'+out_dir_nm[1]+'_'+out_dir_nm[2]+'_'+out_dir_nm[3] ## specifiedcify the jobname by root and number of box seq
            extra_args['jobname'] = jobname
            yield GTSubControllApplication(
                sim_box, # pass the simulation box number
                inputs.copy(),
                **extra_args)

    def after_main_loop(self):
        """
        This method prints information about the status of
        the execution at the end of each sumulation.
        """
        for task in self.session:
            gc3libs.log.info("Task %s: %s terminated with exit code: %s"
                             % (task.jobname, task.execution.state, task.execution.returncode))

## main: run tests

if "__main__" == __name__:
        import gtsub_control
        gtsub_control.GTSubControlScript().run()
