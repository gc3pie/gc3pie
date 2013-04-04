#! /usr/bin/env python
#
#   gsnowpack.py -- Front-end script for running 'snowpack' on data gathered
#   from GSN stations. Generate plots and return the corresponding plot images.
#
#   Copyright (C) 2011, 2012 GC3, University of Zurich
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
gsnowpack is a driver script that allows to run 'snowpack' on a series of input
data gathered from publicly available GSN stations. Data correspond to an
arbitrary number of GSN stations (first prototype used 2 stations).
The script is meant to be invoked every 30' and analyse updated data from the 
same stations.
For each station, gsnowpack launches a single job whose task is to run snowpack
with a gsn configuration that allows to fetch fresh data from a given station
( 1 station per job ); analyse such data and generate the plot image.
Each snowpack analysis should take no more than few seconds with the default
configuration.
Plot images are then retrieved to the client side (optionally data could be also
uploaded on an ftp or http server that would make data available for upload
to the central GSN repository).
Plot images are organized with the following schema:
<output_folder>/<station_name>/<time_stamp>.

XXX: to agree what timestamp should be used (e.g. submission time or something
that could be extracted from the fetched data)

Input parameters consists of:
:param str filename: path to a .sno file that correspond to the selected
station.

Options parameters:
:param str GSN ini file: use an alternative GSN ini file

:param str timespamp: fetch data with a specific timestamp (default: last 30')

:param str destination URI: upload data to an accessible URI

"""

__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
  2013-03-22:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>'
__docformat__ = 'reStructuredText'

# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import gsnowpack
    gsnowpack.GsnowpackScript().run()

import os
import sys
import time
import tempfile

import shutil

import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask


## custom application class
class GsnowpackApplication(Application):
    """
    Custom class to wrap the execution of snowpack and the plotting of the
    result data.
    """
    application_name = 'gsnowpack'

    def __init__(self, sno_file, gsn_ini=None, time_stamp=None, **extra_args):

        command = """#!/bin/bash
echo [`date`] Start

# verify consistency of data
GSN_INI='io_gsn.ini'
SNO_FILE='./output/%s'
SNO_NAME=`basename $SNO_FILE .sno`

echo -n "Checking GSN ini file [$GSN_INI]... "
if [ ! -e $GSN_INI ]; then
   echo "[failed]"
   exit 1
else
   echo "[ok]"
fi

echo -n "Checking .sno file [$SNO_FILE]... "
if [ ! -e $SNO_FILE ]; then
   echo "[failed]"
   exit 1
else
   echo "[ok]"
fi

# Create filesystem layout
mkdir output
mkdir img

echo "XXX: setting explicitly LD_LIBRARY_PATH. This should rather go"
echo "in the image"

export LD_LIBRARY_PATH=/usr/local/lib/:$LD_LIBRARY_PATH

# run snowpack
CMD=`snowpack -c $GSN_INI -e NOW -s $SNO_NAME -m operational`

if [ $? -ne 0 ];then
   echo "Failed running snowpack"
   exit 1
fi

echo "Plotting results"
# Do the plotting part
# image files will be places in `img` folder
echo "XXX: Missing plot part."
echo "For the time being just move the .pro and .haz files"

mv ./output/*.haz img/
mv ./output/*.pro img/

create-matrix.sh img/${SNO_NAME}.pro img/

echo "[`date`] End"
""" % (os.path.basename(sno_file))

        outputs = ['./img/']

        # setup input references
        inputs = [(sno_file,os.path.join('./output',os.path.basename(sno_file)))]
        if gsn_ini:
            inputs.append((gsn_ini,'io_gsn.ini'))

        try:
            # create script file
            f_handle = tempfile.NamedTemporaryFile(prefix='gc3pie-gc_gps', suffix=extra_args['jobname'], delete=False)
            self.execution_script_filename = f_handle.name

            f_handle.file.write(command)
            f_handle.file.close()
            os.chmod(f_handle.name,0777)
        except Exception, ex:
            gc3libs.log.debug("Error creating execution script." +
                              "Error type: %s." % type(ex) +
                              "Message: %s"  %ex.message)
            raise

        inputs.append((self.execution_script_filename,'gsnowpack.sh'))

        Application.__init__(
            self,
            arguments = ['./gsnowpack.sh'],
            inputs = inputs,
            outputs = outputs,
            stdout = 'gsnowpack.log',
            join=True,
            **extra_args)

    def terminated(self):
        """
        Extract output file from 'out' 
        """


class GsnowpackScript(SessionBasedScript):
    """
Scan the specified INPUT directories recursively for simulation
directories and submit a job for each one found; job progress is
monitored and, when a job is done, its output files are retrieved back
into the simulation directory itself.

A simulation directory is defined as a directory containing a
``geotop.inpts`` file.

The ``ggeotop`` command keeps a record of jobs (submitted, executed
and pending) in a session file (set name with the ``-s`` option); at
each invocation of the command, the status of all recorded jobs is
updated, output from finished jobs is collected, and a summary table
of all known jobs is printed.  New jobs are added to the session if
new input files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; ``ggeotop`` will delay submission of
newly-created jobs so that this limit is never exceeded.

    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GsnowpackApplication,
            )

    def setup_options(self):
        self.add_param("-g", "--gsn", metavar="PATH", #type=executable_file,
                       dest="gsn_ini", default=None,
                       help="Path to an alternative gsn_ini file.")

        # self.add_param("-t", "--time-stamp", metavar="TIME", #type=executable_file,
        #                dest="time_stamp", default=None,
        #                help="Alternative timestamp for fetching GSN data. format: HH:MM:SS")


    # def setup_args(self):

    #     self.add_param('sno_dir', type=str,
    #                    help="Path to folder containing .sno files")

    # def parse_args(self):
    #     """
    #     Check presence of input folder (should contains R scripts).
    #     path to command_file should also be valid.
    #     """
        
    #     # check args:
    #     if not os.path.isdir(self.params.sno_dir):
    #         raise gc3libs.exceptions.InvalidUsage(
    #             "Invalid sno_dir argument: '%s'. Path not found"
    #             % self.params.sno_file)

    #     if not self.params.sno_file.endswith('.sno'):
    #         raise gc3libs.exceptions.InvalidUsage(
    #             "Unrecognised file extension for file '%s'. Required .sno"
    #             % self.params.sno_file)

    #     self.log.info("Using .sno file %s" % self.params.sno_file)

    #     if self.params.gsn_ini:
    #         self.log.info("Alternative GSN init file: %s" % self.params.gsn_ini)

    def new_tasks(self, extra):
        """
        Read content of 'command_file'
        For each command line, generate a new GcgpsTask
        """

        tasks = []

        ## collect input directories/files
        def contain_sno_files(paths):
            for path in paths:
                if path.endswith('.sno'):
                    return True
            return False

        inputfiles = []

        extra_args = extra.copy()

        for path in self.params.args:
            self.log.debug("Now processing input argument '%s' ..." % path)
            if not os.path.isdir(path):
                gc3libs.log.error("Argument '%s' is not a directory path." % path)
                continue

            # recursively scan for input files
            for dirpath, dirnames, filenames in os.walk(path):
                for file in filenames:
                    if file.endswith('.sno'):
                        # inputfiles.append(os.path.join(dirpath,file))

                        jobname = 'snopack-%s' % os.path.basename(file).split('.sno')[0]

                        extra_args['jobname'] = jobname
                        extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', os.path.join('.computation',jobname))

                        tasks.append(GsnowpackApplication(
                                os.path.join(dirpath,file),
                                gsn_ini = self.params.gsn_ini,
                                **extra_args))

        return tasks
