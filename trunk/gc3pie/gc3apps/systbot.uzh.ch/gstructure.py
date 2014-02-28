#! /usr/bin/env python
#
#   gstructure.py -- Front-end script for submitting multiple `gstructure` jobs.
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
Front-end script for submitting multiple `Structure` jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gstructure --help`` for program usage instructions.
"""

__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """

  2014-02-27:
  * Initial release, forked off the ``gpyrad`` sources.
"""
__author__ = 'Tyanko Aleksiev <tyanko.aleksiev@chem.uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import gstructure
    gstructure.GStructureScript().run()

from pkg_resources import Requirement, resource_filename
import os
import posix

# gc3 library imports
import gc3libs.utils
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

## custom application class

class GStructureApplication(Application):
    """
    """

    application_name = 'structure'

    def __init__(self, inputs, **extra_args):
        """
        The wrapper script is being used for start the simulation. 
        """

        gstructure_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gstructure_wrapper.sh")


        inputs.append((gstructure_wrapper_sh,os.path.basename(gstructure_wrapper_sh)))

        cmd = "./gstructure_wrapper.sh -d "  

        if extra_args.has_key('mainparam_file'):
            cmd += " -p %s " % extra_args['mainparam_file']

        if extra_args.has_key('extraparam_file'):
            cmd += " -x %s " % extra_args['extraparam_file']

        if extra_args.has_key('output_file'):
            cmd += " -u %s " % extra_args['output_file']

        if extra_args.has_key('k_range'):
            cmd += " -r %s " % extra_args['k_range']

        if extra_args.has_key('replica'):
            cmd += " -e %s " % extra_args['replica']

        cmd += " %s %s %s " % extra_args['loc'], extra_args['ind'], extra_args['output_file']

        Application.__init__(
            self,
            # arguments should mimic the command line interfaca of the command to be
            # executed on the remote end
            arguments = cmd,
            inputs = inputs,
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'gstructure.log',
            join=True,
            **extra_args)

class GstructureTask(RetryableTask, gc3libs.utils.Struct):
    """
    Run ``gstructure`` on a given simulation directory until completion.
    """
    def __init__(self, input_file, **extra_args):
        RetryableTask.__init__(
            self,
            # actual computational job
            GstructureApplication(input_file, **extra_args),
            # keyword arguments
            **extra_args)

## main script class

class GStructureScript(SessionBasedScript):
    """
Scan the specified INPUT directories recursively for simulation
directories and submit a job for each one found; job progress is
monitored and, when a job is done, its output files are retrieved back
into the simulation directory itself.

The ``gstructure`` command keeps a record of jobs (submitted, executed
and pending) in a session file (set name with the ``-s`` option); at
each invocation of the command, the status of all recorded jobs is
updated, output from finished jobs is collected, and a summary table
of all known jobs is printed.  New jobs are added to the session if
new input files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; ``gstructure`` will delay submission of
newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GstructureTask,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GstructureTask,
            )


    def setup_options(self):

        self.add_param("-p", "--mainparams", metavar="str", default="mainparams.txt",
                       dest="mainparam_file", help="Uses a different main parameters file.")

        self.add_param("-x", "--extraparams", metavar="str", default="extraparams.txt",
                       dest="extraprams_file", help="Uses a different extra parameters file.")

        self.add_param("-u", "--output", metavar="str", default="struct.out",
                       dest="output_file", help="Output file name where results will be saved.")

        self.add_param("-r", "--K-range", metavar="str", default="1:20",
                       dest="k_range", help="Structure K range.")

        self.add_param("-e", "--replica", metavar="int", default=3,
                       dest="replica", help="Structure replicates.")

    def setup_args(self):

        self.add_param('loc', type=int,
                       help="Number of loci in the structure input file")
        self.add_param('ind', type=int, 
                       help="Number of individuals in the structure input file")
        self.add_param('input_source', type=str,
                       help="Structure input file/Structure input directory") 

    def parse_args(self):
        """
        """

        self.params.input_source = os.path.abspath(self.params.input_source)


    def new_tasks(self, extra):

        tasks = []
        input_files = [] 
        extentions = [ '.tsv', '.txt', '.struc' ]

        if os.path.isdir(self.params.input_source): 
             
            for i in self._list_local_folder(self.params.input_source):
                if not hidden and i.endswith(extentions):
                    input_files.append(i)
    
        elif os.path.isfile(self.params.input_source):

            if not hidded and self.params.input_source.endswith(extentions):
                input_files.append(self.params.input_source) 

        for input_file in input_files:  

                jobname = "%s" % input_file.split(".")[0]
                
                extra_args = extra.copy()
                extra_args['jobname'] = jobname
    
                # FIXME: ignore SessionBasedScript feature of customizing 
                # output folder
                extra_args['output_dir'] = self.params.output
                extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
                extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
                extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
                extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)
    
                
                extra_args['loc'] = self.params.loc
                extra_args['ind'] = self.params.ind
                extra_args['input_source'] = self.params.input_source

                if self.params.mainparam_file:
                    extra_args['mainparam_file'] = self.params.mainparam_file
                    inputs[self.params.mainparam_file] = 'mainparams.txt'

                if self.params.extraparam_file:
                    extra_args['extraparam_file'] = self.params.extraparam_file
                    inputs[self.params.extraparam_file] = 'extraparams.txt'    

                if self.params.output_file:
                    extra_args['output_file'] = self.params.output_file
    
                if self.params.k_range:
                    extra_args['k_range'] = self.params.k_range
   
                if self.params.replica:
                    extra_args['replica'] = self.params.replica 

                inputs[self.params.input_source] = self.params.input_source  
 
                self.log.info("Creating Task for input file: %s" % input_file)
                    
                tasks.append(GstructureTask(
                    inputs,
                    **extra_args
                    ))

        return tasks


    def _list_local_folder(self, input_folder):
        """
        return a list of all .fastq files in the input folder
        """
    
        return [ os.path.join(input_folder,infile) for infile in os.listdir(input_folder) if infile.endswith('.fastq') ]


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




