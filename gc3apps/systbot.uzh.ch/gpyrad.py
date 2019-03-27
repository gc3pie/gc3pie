#! /usr/bin/env python
#
#   gpyrad.py -- Front-end script for submitting multiple `PyRAD` jobs.
#
#   Copyright (C) 2013, 2014  University of Zurich. All rights reserved.
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
Front-end script for submitting multiple `PyRAD` jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gpyrad --help`` for program usage instructions.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """

  2014-04-16:
  * Retrieve only specific result folder as output.
  * Initial experimental support for S3 repository

  2014-02-24:
  * Initial release, forked off the ``ggeosphere`` sources.
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gpyrad
    gpyrad.GpyradScript().run()

from pkg_resources import Requirement, resource_filename
import os
import posix

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds, GiB
from gc3libs.workflow import RetryableTask
import gc3libs.url

# Default values
DEFAULT_WCLUST="0.9"

## custom application class

class GpyradApplication(Application):
    """
    Fetches execution wrapper, input file and checks
    whether optional arguments have been passed.
    Namely ''wclust'' that sets the pyRAD clustering
    treshold and ''paramsfile'' needed to run PyRAD.
    """

    application_name = 'pyrad'

    def __init__(self, input_file, **extra_args):
        """
        The wrapper script is being used for start the simulation.
        """

        inputs = []
        outputs = []

        gpyrad_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gpyrad_wrapper.sh")


        inputs.append((gpyrad_wrapper_sh,os.path.basename(gpyrad_wrapper_sh)))

        cmd = "./gpyrad_wrapper.sh "

        if 's3cfg' in extra_args:
            inputs.append((extra_args['s3cfg'],
                           "etc/s3cfg"))

        if 'wclust' in extra_args:
            cmd += " -w %s " % extra_args['wclust']
            output_folder_name = 'clust%s' % extra_args['wclust']
        else:
            # This is a convention of PyRAD
            output_folder_name = 'clust%.1f' % DEFAULT_WCLUST
        outputs.append(output_folder_name)

        if 'debug' in extra_args:
            cmd += "-d "
            outputs.append('strace.log')

        if 'paramsfile' in extra_args:
            cmd += " -p ./params.tmpl "
            #XXX: params file contains important paths needed
            # by pyRAD. If we deploy an alternative params.txt file supplied
            # by the end-user, we might incurr in the risk that we can no longer use
            # the assumptions we made with the original params.txt file.
            inputs.append((extra_args['paramsfile'],'./params.tmpl'))

        remote_input_file = os.path.join('./input',os.path.basename(input_file))

        cmd += " %s " % remote_input_file
        inputs.append((input_file,remote_input_file))

        # Add memory requirement
        extra_args.setdefault('requested_memory', 1.5*GiB)

        Application.__init__(
            self,
            # arguments should mimic the command line interfaca of the command to be
            # executed on the remote end
            arguments = cmd,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gpyrad.log',
            join=True,
            **extra_args)

class GpyradTask(RetryableTask, gc3libs.utils.Struct):
    """
    Run ``pyrad`` on a given simulation directory until completion.
    """
    def __init__(self, input_file, **extra_args):
        RetryableTask.__init__(
            self,
            # actual computational job
            GpyradApplication(input_file, **extra_args),
            # keyword arguments
            **extra_args)

## main script class

class GpyradScript(SessionBasedScript):
    """
Scan the specified INPUT directories recursively for simulation
directories and submit a job for each one found; job progress is
monitored and, when a job is done, its output files are retrieved back
into the simulation directory itself.

The ``gpyrad`` command keeps a record of jobs (submitted, executed
and pending) in a session file (set name with the ``-s`` option); at
each invocation of the command, the status of all recorded jobs is
updated, output from finished jobs is collected, and a summary table
of all known jobs is printed.  New jobs are added to the session if
new input files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; ``gpyrad`` will delay submission of
newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GpyradTask,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GpyradTask,
            )


    def setup_options(self):
        self.add_param("-T", "--treshold", metavar="INT",
                       dest="wclust", default=None,
                       help="Wclust: clustering threshold as a decimal."+
                       "Note: if '-p' is used, this option will be ignored.")

        self.add_param("-P", "--params", metavar="PATH",
                       dest="paramsfile", default=None,
                       help="Location of params.txt fiule required by pyrad. "+
                       "WARNING: The default params.txt is used to make "+
                       "assumption on the location of pyRAD and the "+
                       "input folder where the *.fastq files are located. "+
                       "Passing an alternative params.txt file might break "+
                       "such assumptions resulting in a falied execution. " +
                       "Please check the default params.txt file located in "+
                       "'test.pyRAD' folder where gpyrad.py is located.")


        self.add_param("-Y", "--s3cfg", metavar="PATH",
                       dest="s3cfg", default=None,
                       help="Location of the s3cfg configuration file. "+
                       "Default assumes 's3cfg' configuration file being "+
                       "available on the remote execution node.")

    def setup_args(self):

        self.add_param('input_container', type=str,
                       help="Path to local folder containing fastq files.")

    def parse_args(self):
        """
        Check presence of input folder (should contains R scripts).
        path to command_file should also be valid.
        """

        self.input_folder_url = gc3libs.url.Url(self.params.input_container)

        if self.params.paramsfile and not os.path.isfile(self.params.paramsfile):
            raise gc3libs.exceptions.InvalidUsage(
                "Invalid params file '%s': File not found"
                % self.params.paramsfile)

        if self.params.s3cfg and not os.path.isfile(self.params.s3cfg):
            raise gc3libs.exceptions.InvalidUsage(
                "Invalid S3cmd config file '%s': File not found"
                % self.params.s3cfg)

    def new_tasks(self, extra):

        tasks = []

        #XXX: I would have like to use the same forumla as in
        # cmdline:405 but I found it very confusing
        # easier to use a linear threshold
        # 0: error 1: warning 2: info 3: debug
        if self.params.verbose > 3:
            extra['debug'] = True

        for input_file in self._list_folder_by_url(self.input_folder_url):

            jobname = "%s" % os.path.basename(input_file).split(".")[0]

            extra_args = extra.copy()
            extra_args['jobname'] = jobname

            # FIXME: ignore SessionBasedScript feature of customizing
            # output folder
            extra_args['output_dir'] = self.params.output

            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)

            if self.params.paramsfile:
                extra_args['paramsfile'] = self.params.paramsfile
            elif self.params.wclust:
                extra_args['wclust'] = self.params.wclust

            if self.params.s3cfg:
                extra_args['s3cfg'] = self.params.s3cfg

            self.log.info("Creating Task for input file: %s" % input_file)

            tasks.append(GpyradTask(
                input_file,            # path to valid input file
                **extra_args
                ))

        return tasks


    def _list_folder_by_url(self, url):
        """
        """

        if url.scheme == "s3":
            return self._list_s3_container(url)
        elif url.scheme ==  "file":
            return self._list_local_folder(url)
        else:
            gc3libs.log.error("Unsupported Input folder URL %s. "+
                              "Only supported protocols are: [file,s3] " % url.scheme)
            return None

    def _list_local_folder(self, input_folder):
        """
        return a list of all .fastq files in the input folder
        """

        return [ os.path.join(input_folder.path,infile) for infile in os.listdir(input_folder.path) if infile.endswith('.fastq') ]

    def _list_S3_container(self, s3_url):
        """
        Use s3cmd command line interface to interact with
        a remote S3-compatible ObjectStore.
        Assumption:
        . s3cmd configuration file available
        and correctly pointing to the right ObjectStore.
        . s3cmd available in PATH environmental variable.
        . Valid for only 1 S3_URL path
        """

        import subprocess

        # read content of remote S3CMD_URL
        try:
            # 's3cmd ls' should return a list of model archives
            # for each of them bundle archive_name and working_dir
            _process = subprocess.Popen("s3cmd ls %s" % s3_url,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        close_fds=True, shell=True)

            (out, err) = _process.communicate()
            exitcode = _process.returncode

            if (exitcode != 0):
                raise Exception("Error: %s, %s", (out,err))

            # Parse 'out_list' result and extract all .tgz or .zip archive names

            for s3_obj in out.strip().split("\n"):
                if s3_obj.startswith("DIR"):
                    # it's a S3 directory; ignore
                    continue
                # object string format: '2014-01-09 16:41 3627374 s3://a4mesh/model_1.zip'
                s3_url = s3_obj.split()[3]
                if(s3_url.endswith(".fastq")):
                   yield s3_url

        except Exception, ex:
            gc3libs.log.error("Failed while reading remote S3 container. "+
                              "%s", ex.message)
