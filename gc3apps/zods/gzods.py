#!/usr/bin/env python
#
#   gzods.py -- Front-end script for submitting multiple ZODS jobs
#
#   Copyright (C) 2012  University of Zurich. All rights reserved.
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
Front-end script for submitting multiple ZODS jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gzods --help`` for program usage instructions.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2012-07-17:
    * Initial releases.
  2012-07-31:
    * Added support for submission of multiple files.
  2012-08-06:
    * Added support of restarting the job
"""
__author__ = 'Lukasz Miroslaw <lukasz.miroslaw@uzh.ch>'
__docformat__ = 'reStructuredText'


if __name__ == '__main__':
        import gzods
        gzods.ZodsScript().run()


import os
import xml.dom.minidom
from xml.etree.ElementTree  import ElementTree
import gc3libs
import gc3libs.cmdline

DEFAULT_ZODS_VERSION='0.344'


class GzodsApp(gc3libs.Application):
    """
    This class is derived from gc3libs.Application and defines ZODS app with its input and output files.
    """

    application_name = 'zods'

    def __init__(self, filename, version=DEFAULT_ZODS_VERSION, **extra_args):
        if self.check_input(filename) is None:
                raise gc3libs.exceptions.InputFileError(
                        "Cannot find auxiliary files for input file '%s'."
                        " (Auxiliary files are the average structure '.cif' file,"
                        " the reference intensities file,"
                        " and -optionally- the restart file.)"
                        % (filename,))
        inputFiles = self.check_input(filename)
        gc3libs.log.debug("Detected input files for ZODS: %s, and %s.", filename, [inputFiles])
        myinputs = [filename]
        for inputFile in inputFiles:
                inputFile = os.path.abspath(inputFile)
                myinputs.append(inputFile)

        gc3libs.Application.__init__(
            self,
            tags=[ ("APPS/CHEM/ZODS-%s" % version) ],
            arguments = [
                'mpiexec',
                # these are arguments to `mpiexec`
                "-n", extra_args['requested_cores'],
                # this is the real ZODS command-line
                '$ZODS_BINDIR/simulator', os.path.basename(filename),
            ],
            inputs = myinputs,   # mandatory, inputs are files that will be copied to the site
            outputs = gc3libs.ANY_OUTPUT,           # mandatory
            stdout = "stdout+err.txt",
            join=True,
            **extra_args)


    def terminated(self):
        filename = os.path.join(self.output_dir, self.stdout)
        gc3libs.log.debug("ZODS single job based on %s has TERMINATED", filename)
        for output in self.outputs:
                gc3libs.log.debug("Retrieved the following file from ZODS job %s", output)


#       Detect the following references to external files in input.xml
#       input1:
#       <average_structure>
#          <file format="cif" name="californium_simple_3.cif"/>
#        </average_structure>
#        input2:
#       <reference_intensities file_format="xml" file_name="data.xml"/>
#       input3:
#       <optimization method>
#               <restart file="diff_ev2.xml" />
#       </optimization method>

    def check_input(self,filename):

        if not os.path.exists(filename):
            raise RuntimeError("Input file '%s' DOES NOT exists." % filename)
        basedir = os.path.dirname(filename)
        DOMTree = xml.dom.minidom.parse(filename)
        tree = ElementTree()
        XMLTree = tree.parse(filename)
        cNodes = DOMTree.childNodes

        # Detect <reference_intensities> and <average_structure>
        if len(cNodes[0].getElementsByTagName('reference_intensities')) > 0 and len(cNodes[0].getElementsByTagName('average_structure')) > 0:
            data_file = cNodes[0].getElementsByTagName('reference_intensities')[0].getAttribute('file_name')
            avg_file = cNodes[0].getElementsByTagName('average_structure')[0].getElementsByTagName('file')[0].getAttribute('name')
            data_file = os.path.join(basedir,data_file)
            avg_file = os.path.join(basedir,avg_file)
            gc3libs.log.debug("gzods.py: %s references to the following files: %s and  %s.", filename, avg_file, data_file)
            if os.path.exists(avg_file)  == False or os.path.exists(data_file) == False:
                gc3libs.log.warning("gzods.py: Averaged structure file %s or reference intensities file %s DO NOT exist.", avg_file, data_file)
                raise RuntimeError("gzods.py: Input file '%s' DOES NOT exists." % filename)
            else:
                gc3libs.log.info(
                    "Input file '%s' references"
                    " averaged structure file '%s'"
                    " and reference intesities file '%s'.",
                    filename, avg_file, data_file)
        # Detect optional argument <restart>
                if len(cNodes[0].getElementsByTagName('run_type')) > 0:
                        gc3libs.log.debug("gzods.py: <run_type> tag detected.")
                        if len(cNodes[0].getElementsByTagName('optimization_method')) > 0:
                                gc3libs.log.debug("gzods.py: <optimization_method> tag detected.")
                                if len(cNodes[0].getElementsByTagName('restart')) > 0:
                                        gc3libs.log.debug("gzods.py: <restart> tag and optional restart file detected.")
                                        restart_file = cNodes[0].getElementsByTagName('optimization_method')[0].getElementsByTagName('restart')[0].getAttribute('file')
                                        restart_file = os.path.join(basedir,restart_file)
                                        #restart_file = os.path.abspath(restart_file)
                                        if os.path.exists(restart_file)  == False:
                                                raise gc3libs.exceptions.InputFileError("gzods.py: Input file '%s' references a file '%s' that DOES NOT exists." % (filename, restart_file))
                                        else:
                                                gc3libs.log.info("gzods.py: %s references an optional restart file: %s.", filename, restart_file)

                                        return (data_file, avg_file, restart_file)
                                else:
                                        return (data_file, avg_file)

                        else:
                                return (data_file, avg_file)

class ZodsScript(gc3libs.cmdline.SessionBasedScript):
        """
This application runs the ZODS program on distributed resources.

Any (non-option) argument given on the command line is interpreted as
a directory name; the script scans any directories recursively for
'input*.xml' files, and submits a ZODS job for each input file found.

Job progress is monitored and, when a job is done, its output files
are retrieved back into the same directory where the input '.xml' file is.

The `-c` option allows to set the number of CPU cores for running
parallel jobs; for instance, the following will run parallel ZODS on
file `input.xml` using on 5 CPUs::

  ./gzods input.xml -c 5

        """
        version = "1.0"

        def setup_options(self):
                self.add_param("-R", "--verno", metavar="VERNO",
                               dest="verno", default=DEFAULT_ZODS_VERSION,
                               help="Request the specified version of ZODS"
                               " (default: %(default)s).")

        def new_tasks(self, extra):
           if self.params.args is None or len(self.params.args) == 0:
                self.log.warning(
                        "No arguments given on the command line: no new jobs are created."
                        " Existing jobs will still be managed.")
                return
           tasks=[]
           listOfFiles = self._search_for_input_files(self.params.args, pattern="input*.xml")
           gc3libs.log.debug("List of detected input files for ZODS: %s", listOfFiles)
           for filename in listOfFiles:
               filepath = os.path.abspath(filename)
               extra_args = extra.copy()
               extra_args['version'] = self.params.verno
               # job name comes from input file name, minus the `.xml` ext
               extra_args['jobname'] = os.path.basename(filename)[:-len('.xml')]
               yield GzodsApp(filepath, **extra_args)
