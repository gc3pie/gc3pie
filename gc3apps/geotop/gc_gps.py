#! /usr/bin/env python
#
#   gc_gps.py -- Front-end script for submitting multiple `GC-GPS` R-baseed jobs.
#
#   Copyright (C) 2011, 2012, 2019  University of Zurich. All rights reserved.
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
Front-end script for submitting multiple `R` jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gc_gps.py --help`` for program usage instructions.

Input parameters consists of:
:param str command_file: Path to the file containing all the commands to be executed
   example:
          R CMD BATCH --no-save --no-restore '--args pos=27 realizations=700 snr=1
          mast.h=0.5 sd.mast.o=0' ./src/processit.R ./out/screen.out

:param str src_dir: path to folder containing R scripts to be transferred to
'src' folder on the remote execution node

"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2013-01-24:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gc_gps
    gc_gps.GcgpsScript().run()

import os
import sys
import time
import tempfile

import shutil

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask


## custom application class
class GcgpsApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'gc_gps'

    def __init__(self, command, src_dir, result_dir, input_dir, **extra_args):


        # setup output references
        self.result_dir = result_dir
        self.output_dir = extra_args['output_dir']

        # extract from command relevant parameters to build output filename
        command_args = _parse_command(command)
        if not command_args:
            outputs = gc3libs.ANY_OUTPUT
        else:
            self.output_file_name = "pos-%s_rates_MC%s_snr%s_h%s_sd_o%s.Rdata" \
                % (command_args['pos'],
                   command_args['realizations'],
                   command_args['snr'], command_args['mast.h'],
                   command_args['sd.mast.o'])

            self.local_output_file = os.path.join(self.output_dir, self.output_file_name)
            self.local_result_output_file = os.path.join(self.result_dir, self.output_file_name)
            outputs = ['./out/screen.out', ('./out/pos.output', self.output_file_name)]

        # setup input references
        inputs = [ (os.path.join(src_dir,v),os.path.join("./src",v))
                   for v in os.listdir(src_dir)
                   if not os.path.isdir(os.path.join(src_dir,v)) ]

        if input_dir:
            # take everything from input_dir/
            # XXX: N.B. NOT recursively
            for elem in os.listdir(input_dir):
                if os.path.isfile(os.path.join(input_dir,elem)):
                    inputs.append((os.path.join(input_dir, elem),
                                   os.path.join('./in',elem)))

        # prepare execution script from command
        execution_script = """
#!/bin/sh

# Checking 'in' folder
# Checks first if './in' has been created
# otherwise try default in '$HOME/in'

echo -n "Checking Input folder... "
if [ -d ./in ]; then
   echo "[${PWD}/in]"
elif [ -d ${HOME}/in ]; then
   ln -s ${HOME}/in in
   echo [${HOME}/in]
else
   echo [FAILED: no folder found in './in' nor in '${HOME}/in']
   exit 1
fi

mkdir out

# Install docker
echo "Installing dockers... "
if ! command -v docker; then
  curl https://get.docker.com/ | sudo bash
fi
sudo usermod -aG docker $USER

# execute command
echo "Runnning: sudo sudo -u $USER -g docker docker run -i --rm -v "$PWD":/home/docker -w /home/docker -u docker r-base %s"
sudo sudo -u $USER -g docker docker run -i --rm -v "$PWD":/home/docker -w /home/docker -u docker r-base %s
RET=$?

echo Program terminated with exit code $RET

mv out/pos* out/pos.output
exit $RET
        """ % (command, command)

        try:
            # create script file
            (handle, self.tmp_filename) = tempfile.mkstemp(prefix='gc3pie-gc_gps', suffix=extra_args['jobname'])

            # XXX: use NamedTemporaryFile instead with 'delete' = False

            fd = open(self.tmp_filename,'w')
            fd.write(execution_script)
            fd.close()
            os.chmod(fd.name,0o777)
        except Exception, ex:
            gc3libs.log.debug("Error creating execution script" +
                              "Error type: %s." % type(ex) +
                              "Message: %s"  %ex.message)
            raise

        inputs.append((fd.name,'gc_gps.sh'))


        Application.__init__(
            self,
            arguments = ['./gc_gps.sh'],
            inputs = inputs,
            outputs = outputs,
            stdout = 'gc_gps.log',
            join=True,
            **extra_args)

    def fetch_output_error(self, ex):
        # Ignore errors if `pos.output` file has not been created by
        # the application.
        if isinstance(ex, gc3libs.exceptions.CopyError):
            if os.path.basename(ex.source) == 'pos.output':
                return None
        return ex

    def terminated(self):
        """
        Extract output file from 'out'
        """
        # Cleanup tmp file
        try:
            os.remove(self.tmp_filename)
        except Exception, ex:
            gc3libs.log.error("Failed removing temporary file %s. " % self.tmp_filename +
                              "Error type %s. Message %s" % (type(ex), str(ex)))

        if not self.local_output_file:
            # outputs = gc3libs.ANY_OUTPUT
            for path in os.path.listdir(self.output_dir):
                if os.path.isfile(path) and path.startswith('pos'):
                    # We assume this is the output file to retrieve
                    self.local_output_file = path
                    self.local_result_output_file = os.path.join(self.result_dir,path)

        # copy output file `pos*` in result_dir
        if not os.path.isfile(self.local_output_file):
            gc3libs.log.error("Output file %s not found"
                              % self.local_output_file)
            self.execution.returncode = (0, 100)
        else:
            try:
                shutil.move(self.local_output_file,
                            self.local_result_output_file)
            except Exception, ex:
                gc3libs.log.error("Failed while transferring output file " +
                                  "%s " % self.local_output_file +
                                  "to result folder %s. " % self.result_dir +
                                  "Error type %s. Message %s. "
                                  % (type(ex),str(ex)))

                self.execution.returncode = (0, 100)

def _parse_command(command):
        """
        Parse a command like the following:
        "R CMD BATCH --no-save --no-restore '--args pos=5 realizations=700
         snr=1 mast.h=0.5 sd.mast.o=0' ./src/processit.R ./out/screen.out"
        and should return a dictionary like the following:
        {'pos': '5', 'realizations': '700', 'snr': '1',
         'mast.h': '0.5', 'sd.mast.o': '0' }
        """
        try:
            # filter out the part before and after the arguments
            args = command.split("'")[1]

            # split and ignore '--args'
            args = args.split()[1:]

            # return a dictionary from "pos=5 realizations=700 snr=1
            # mast.h=0.5 sd.mast.o=0"
            # return { k:v for k,v in [ v.split('=') for v in args ] }
            return dict( (k,v) for k,v in [ v.split('=') for v in args ])
        except Exception, ex:
            gc3libs.log.error("Failed while parsing command: %s. " % command +
                              "Type: %s. Message: %s" % (type(ex),str(ex)))
            return None

class GcgpsTask(RetryableTask):
    def __init__(self, command, src_dir, result_dir, input_dir, **extra_args):
        RetryableTask.__init__(
            self,
            # actual computational job
            GcgpsApplication(
                command,
                src_dir,
                result_dir,
                input_dir,
                **extra_args),
            **extra_args
            )

    def retry(self):
        """
        Task will be retried iif the application crashed
        due to an error within the exeuction environment
        (e.g. VM crash or LRMS kill)
        """
        # XXX: check whether it is possible to distingish
        # between the error conditions and set meaningfull exitcode
        return False


class GcgpsScript(SessionBasedScript):
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
            application = GcgpsApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GcgpsApplication,
            )

    def setup_options(self):
        self.add_param("-i", "--input", metavar="PATH", #type=executable_file,
                       dest="input_dir", default=None,
                       help="Path to the input data folder.")

    def setup_args(self):

        self.add_param('command_file', type=str,
                       help="Command file full path name")

        self.add_param('R_source_folder', type=str,
                       help="Path to folder containing scripts to be executed.")

    def parse_args(self):
        """
        Check presence of input folder (should contains R scripts).
        path to command_file should also be valid.
        """

        # check args:
        # XXX: make them position independent
        if not os.path.isdir(self.params.R_source_folder):
            raise gc3libs.exceptions.InvalidUsage(
                "Invalid path to R scripts folder: '%s'. Path not found"
                % self.params.R_source_folder)
        # XXX: shall we check/validate the content ( presence of valid R scripts ) ?

        self.log.info("source dir: %s" % self.params.R_source_folder)

        if not os.path.exists(self.params.command_file):
            raise gc3libs.exceptions.InvalidUsage(
                "gc_gps command file '%s' does not exist;"
                % self.params.command_file)
        gc3libs.utils.check_file_access(self.params.command_file, os.R_OK,
                                gc3libs.exceptions.InvalidUsage)

        if self.params.input_dir and not os.path.isdir(self.params.input_dir):
            raise gc3libs.exceptions.InvalidUsage(
                "Input folder '%s' does not exists"
                % self.params.input_dir)

        self.log.info("Command file: %s" % self.params.command_file)
        self.log.info("R source dir: %s" % self.params.R_source_folder)
        if self.params.input_dir:
            self.log.info("Input data dir: '%s'" % self.params.input_dir)

    def new_tasks(self, extra):
        """
        Read content of 'command_file'
        For each command line, generate a new GcgpsTask
        """

        tasks = []

        try:
            fd = open(self.params.command_file)

            self.result_dir = os.path.dirname(self.params.output)

            for line in fd:
                command = line.strip()

                if not command:
                    # ignore black lines
                    continue

                cmd_args = _parse_command(command)

                # setting jobname
                jobname = "gc_gps-%s%s%s%s%s" % (cmd_args['pos'],
                                                 cmd_args['realizations'],
                                                 cmd_args['snr'],
                                                 cmd_args['mast.h'],
                                                 cmd_args['sd.mast.o'])

                extra_args = extra.copy()
                extra_args['jobname'] = jobname
                # FIXME: ignore SessionBasedScript feature of customizing
                # output folder
                extra_args['output_dir'] = self.params.output

                extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', os.path.join('.computation',jobname))
                extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', os.path.join('.computation',jobname))
                extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', os.path.join('.computation',jobname))
                extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', os.path.join('.computation',jobname))

                self.log.debug("Creating Task for command: %s" % command)

                tasks.append(GcgpsTask(
                        command,
                        self.params.R_source_folder,
                        self.result_dir,
                        self.params.input_dir,
                        **extra_args))

        except IOError, ioe:
            self.log.error("Error while reading command file " +
                           "%s." % self.params.command_file +
                           "Message: %s" % ioe.message)
        except Exception, ex:
            self.log.error("Unexpected error. Error type: %s, Message: %s" % (type(ex),str(ex)))

        finally:
            fd.close()

        return tasks
