#! /usr/bin/env python
#
#   ggeosphere.py -- Front-end script for submitting multiple `GeoSphere` jobs.
#
#   Copyright (C) 2013, 2014 GC3, University of Zurich
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
Front-end script for submitting multiple `GeoSphere` jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``ggeosphere --help`` for program usage instructions.
"""

__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """

  2013-08-16:
  * Initial release, forked off the ``ggeoshpere`` sources.
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import ggeosphere
    ggeosphere.GeoSphereScript().run()

from pkg_resources import Requirement, resource_filename
import os
import posix

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

## custom application class

class GeoSphereApplication(Application):
    """
    """

    application_name = 'geosphere'

    def __init__(self, input_dir, working_dir, output_container, **extra_args):
        """
        Prepare remote execution of geosphere wrapper script.
        The resulting Application will associate a remote execution like:

        geosphere_wrapper.sh [options] <input archive> <working dir> <model name>

        Options:
        -g <grok binary file>    path to 'grok' binary. Default in PATH
        -h <hgs binary file>     path to 'hgs' binary. Default in PATH
        -o <S3 url>              store output result on an S3 container
        -d                       enable debug
        """

        inputs = []
        
        geosphere_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/geosphere_wrapper.sh")


        inputs.append((geosphere_wrapper_sh,os.path.basename(geosphere_wrapper_sh)))

        cmd = "./geosphere_wrapper.sh -d "
        

        if extra_args.has_key('s3cfg'):
            inputs.append((extra_args['s3cfg'],
                           "etc/s3cfg"))

        if extra_args.has_key('grok_bin'):
            cmd += "-g %s " % extra_args['grok_bin']

            inputs.append((extra_args['grok_bin'],
                          os.path.join("./bin",
                                       os.path.basename(extra_args['grok_bin']))))

        if extra_args.has_key('hgs_bin'):
            cmd += "-h %s " % extra_args['hgs_bin']

            inputs.append((extra_args['hgs_bin'],
                          os.path.join("./bin",
                                       os.path.basename(extra_args['hgs_bin']))))

        cmd += "%s %s %s" % (input_dir,
                             working_dir,
                             output_container)


        Application.__init__(
            self,
            # arguments should mimic the command line interfaca of the command to be
            # executed on the remote end
            arguments = cmd,
            inputs = inputs,
            outputs = [],
            stdout = 'geosphere.log',
            join=True,
            **extra_args)

    def terminated(self):
        """
        Check whether the output has been properly produced.
        If not, mark the application to be re-executed
        """
        # XXX: for the time being do not resubmit.
        # Need more info on how whether and how to check failures.
        self.execution.returncode = (0, posix.EX_OK)

    def check_output(self):
        """
        To be implemented according to specs
        """
        return True

class GeoSphereTask(RetryableTask, gc3libs.utils.Struct):
    """
    Run ``geosphere`` on a given simulation directory until completion.
    """
    def __init__(self, input_dir, working_dir, output_container, **extra_args):
        RetryableTask.__init__(
            self,
            # actual computational job
            GeoSphereApplication(input_dir, working_dir, output_container, **extra_args),
            # keyword arguments
            **extra_args)

## main script class

class GeoSphereScript(SessionBasedScript):
    """
Scan the specified INPUT directories recursively for simulation
directories and submit a job for each one found; job progress is
monitored and, when a job is done, its output files are retrieved back
into the simulation directory itself.

The ``ggeosphere`` command keeps a record of jobs (submitted, executed
and pending) in a session file (set name with the ``-s`` option); at
each invocation of the command, the status of all recorded jobs is
updated, output from finished jobs is collected, and a summary table
of all known jobs is printed.  New jobs are added to the session if
new input files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; ``ggeosphere`` will delay submission of
newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GeoSphereTask,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GeoSphereTask,
            )


    def setup_options(self):
        self.add_param("-G", "--grok", metavar="PATH",
                       dest="grok_bin", default=None,
                       help="Location of the 'grok' binary. "+
                       "Default assumes 'grok' binary being "+
                       "available on the remote execution node.")

        self.add_param("-H", "--hgs", metavar="PATH",
                       dest="hgs_bin", default=None,
                       help="Location of the 'hgs' binary. "+
                       "Default assumes 'hgs' binary being "+
                       "available on the remote execution node.")

        self.add_param("-Y", "--s3cfg", metavar="PATH",
                       dest="s3cfg", default=None,
                       help="Location of the s3cfg configuration file. "+
                       "Default assumes 's3cfg' configuration file being "+
                       "available on the remote execution node.")

    def setup_args(self):

        self.add_param('input_container', type=str,
                       help="Path to the S3 ObjectStore input container.")

        self.add_param('output_container', type=str,
                       help="Path to the S3 ObjectStore output container.")

    def parse_args(self):
        """
        Check presence of input folder (should contains R scripts).
        path to command_file should also be valid.
        """

        if self.params.grok_bin:
            if not os.path.isfile(self.params.grok_bin):
                raise gc3libs.exceptions.InvalidUsage(
                    "grok binary '%s' does not exists"
                    % self.params.input_dir)
            self.log.info("Grok binary: [%s]" % self.params.grok_bin)
        else:
            self.log.info("Grok binary: [use remote]")

        if self.params.hgs_bin:
            if not os.path.isfile(self.params.hgs_bin):
                raise gc3libs.exceptions.InvalidUsage(
                    "hgs binary '%s' does not exists"
                    % self.params.hgs_bin)
            self.log.info("hgs binary: [%s]" % self.params.hgs_bin)
        else:
            self.log.info("hgs binary: [use remote]")


    def new_tasks(self, extra):

        tasks = []

        for input_dir in self._list_S3_container(self.params.input_container):

            working_dir = os.path.splitext(os.path.basename(input_dir))[0]

            jobname = "a4mesh-%s" % working_dir
            
            extra_args = extra.copy()
            extra_args['jobname'] = jobname

            # FIXME: ignore SessionBasedScript feature of customizing 
            # output folder
            extra_args['output_dir'] = self.params.output
                
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)

            if self.params.hgs_bin:
                extra_args['hgs_bin'] = self.params.hgs_bin

            if self.params.grok_bin:
                extra_args['grok_bin'] = self.params.grok_bin

            if self.params.s3cfg:
                extra_args['s3cfg'] = self.params.s3cfg

            self.log.info("Creating Task for input file: %s" % input_dir)
                
            tasks.append(GeoSphereTask(
                input_dir,            # path to valid archive input file
                working_dir,
                self.params.output_container,
                **extra_args
                ))

        return tasks

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
                if(s3_url.startswith("s3://")):
                   yield s3_url

        except Exception, ex:
            gc3libs.log.error("Failed while reading remote S3 container. "+
                              "%s", ex.message)




