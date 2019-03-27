#! /usr/bin/env python
#
#   ggeosphere.py -- Front-end script for submitting multiple `GeoSphere` jobs.
#
#   Copyright (C) 2013, 2014, 2018  University of Zurich. All rights reserved.
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

See the output of ``ggeosphere.py --help`` for program usage instructions.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """

  2014-03-21:
  * Initial release, forked off the ``ggeoshpere`` sources.
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import ggeosphere
    ggeosphere.GeoSphereScript().run()

from pkg_resources import Requirement, resource_filename
import os
import posix
import logging
import ConfigParser
import time

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

# Default values
TODAY=time.strftime('%Y%m%d-%H%M')
DEFAULT_S3CFG="$HOME/.s3cfg"
DEFAULT_CLOUD_ALL="*"
DEFAULT_MAX_RUNNING=10
DEFAULT_MAX_RETRIES=3
DEFAULT_LOG_LEVEL=logging.ERROR
# A4MESH_CONFIG_FILE="/etc/a4mesh/a4mesh.cfg"

## custom application class
def validate_config_option(config, option_name, default_value=None):
    if not config.has_option("default",option_name):
        if default_value:
            gc3libs.log.warning("No option '%s' defined. " \
                                "Using default: '%s'" % (option_name, str(default_value)))
            return default_value
        else:
            raise ConfigParser.ParsingError("No option '%s' defined." % option_name)
    else:
        return config.get('default',option_name)

class GeoSphereApplication(Application):
    """
    ``ggeosphere`` can work in two modes:
    1. CLI: as a regular SessionBasedScript can be invoked from the command like
    to process input model data. It runs as all SessionBasedScripts.
    2. Daemon: it runs continuously (like a daemon) and checks periodically for changes
    in the input/output folders. Every new model addedd to the input folder, triggers
    a new simualtion; every result removed from the output folder, triggers a re-run of the
    corresponding completed simualtion. Every model removed from the input folder also removes
    the corresponding result (unless overwritten by the ``PreserveOutputWhenInputRemoved``).
    """

    application_name = 'geosphere'

    def __init__(self, input_model, output_model, **extra_args):
        """
        Prepare remote execution of geosphere wrapper script.
        The resulting Application will associate a remote execution like:

        geosphere_wrapper.sh [options] <input model <output model>

        Options:
        -w <working dir>         name of the working dir extracted from
                                 .tgz file
        -g <grok binary file>    path to 'grok' binary. Default in PATH
        -h <hgs binary file>     path to 'hgs' binary. Default in PATH
        -d                       enable debug
        """


        self.input_model = input_model
        self.output_model = output_model

        inputs = []
        outputs = []

        geosphere_wrapper_sh = resource_filename(
            Requirement.parse("gc3pie"),"gc3libs/etc/geosphere_wrapper.sh"
        )

        inputs.append((geosphere_wrapper_sh,
                       os.path.basename(geosphere_wrapper_sh)))

        cmd = "./geosphere_wrapper.sh -d "

        if 's3cfg' in extra_args:
            inputs.append((extra_args['s3cfg'],
                           "etc/s3cfg"))

        if 'grok_bin' in extra_args:
            cmd += "-g %s " % extra_args['grok_bin']

            inputs.append((extra_args['grok_bin'],
                          os.path.join("./bin",
                                       os.path.basename(extra_args['grok_bin']))
                       ))

        if 'hgs_bin' in extra_args:
            cmd += "-h %s " % extra_args['hgs_bin']

            inputs.append((extra_args['hgs_bin'],
                          os.path.join("./bin",
                                       os.path.basename(extra_args['hgs_bin']))
                       ))

        # Pass input model location
        if self.input_model.scheme == 'file':
            # Include local input files as part of Application
            inputs.append((str(self.input_model),os.path.join(".",
                                                              os.path.basename(
                                                                  str(self.input_model)))))
            cmd += " ./%s " % os.path.basename(str(self.input_model))
        else:
            # just pass remote input model URI
            cmd += "%s" % str(input_model)

        # Pass working dir name argument
        cmd += " %s " % extra_args['jobname']

        # Pass output location argument
        if self.output_model.scheme == 'file':
            # Include output as part of data to be retrieved locally
            outputs.append((os.path.basename(str(self.output_model)),
                            str(self.output_model)))
            cmd += "./%s " % os.path.basename(str(self.output_model))
        else:
            # just pass remote output model URI
            cmd += "%s " % str(self.output_model)

        Application.__init__(
             self,
             # arguments should mimic the command line interface of the command to be
             # executed on the remote end
             arguments = cmd,
             inputs = inputs,
             outputs = outputs,
             stdout = 'geosphere.log',
             join=True,
             **extra_args)


        # #XXX: TESTING PURPOSES...
        # extra_args['requested_memory'] = 1*MB

        # Application.__init__(
        #     self,
        #     # the following arguments are mandatory:
        #     arguments = ["/bin/hostname"],
        #     inputs = [],
        #     outputs = [],
        #     stdout = "stdout.txt",
        #     join = True,
        #     **extra_args)

class GeoSphereTask(RetryableTask, gc3libs.utils.Struct):
    """
    Run ``geosphere`` on a given simulation directory until completion.
    """
    def __init__(self, input_model, output_model, **extra_args):
        RetryableTask.__init__(
            self,
            # actual computational job
            GeoSphereApplication(input_model, output_model, **extra_args),
            # keyword arguments
            **extra_args)

## main script class

class GeoSphereScript(SessionBasedScript):
    """
    Loads the configuration file.
    Scans the specified INPUT for model files to simulate with
    ggeosphere. Submit a job for each one found; job progress is
    monitored and, when a job is done, its output files are retrieved back
    into the simulation directory itself.

    The ``ggeosphere`` has two working models: classical CLI based that can be executed
    as local script or as web-based application (use the '--daemon-mode' to enable
    web-based application mode). In web-based application mode ``ggeosphere``
    does not terminate when all input models have been processed but it
    periodically checks whether new models have been added.
    In case, a new simulation is launched.
    It keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

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
        self.add_param("-I", "--Input",
                       metavar="PATH",
                       action="store",
                       default=None,
                       dest="input_folder",
                       help="Path to the input folder. "+
                       "Valid URLs: [local path], [s3://].")

        self.add_param("-O", "--Output",
                       metavar="PATH",
                       action="store",
                       default=None,
                       dest="output_folder",
                       help="Path to the output folder. "+
                       "Valid URLs: [local path], [s3://].")

        self.add_param("-c", "--config",
                       metavar="PATH",
                       action="store",
                       default=None,
                       dest="config_file",
                       help="Location of the configuration file. "+
                       "Command line options take precedence over config file.")

        self.add_param("-G", "--grok", metavar="PATH",
                       dest="grok_bin", default=None,
                       help="Location of the 'grok' binary. "+
                       "Default: assumes 'grok' binary being "+
                       "available on the remote execution node.")

        self.add_param("-H", "--hgs", metavar="PATH",
                       dest="hgs_bin", default=None,
                       help="Location of the 'hgs' binary. "+
                       "Default: assumes 'hgs' binary being "+
                       "available on the remote execution node.")

        self.add_param("-Y", "--s3cfg", metavar="PATH",
                       dest="s3cfg", default=None,
                       help="Location of the s3cfg configuration file. "+
                       "Default: assumes 's3cfg' configuration file being "+
                       "available on the remote execution node.")

        self.add_param("-P", "--preserve-output", metavar="BOOLEAN",
                       dest="preserve_output", default=True,
                       help="When an input model is removed, the corresponding "+
                       "output files will be kept. Default: False (remove output).")

        self.add_param("-d", "--daemon-mode", metavar="BOOLEAN",
                       dest="is_daemon", default=False,
                       help="Run ggeosphere in daemon mode. Default: False (no daemon).")

    def parse_args(self):
        """
        Check presence of input folder (should contains R scripts).
        path to command_file should also be valid.
        """

        # Check configuration file
        if self.params.config_file:
            if not os.path.isfile(self.params.config_file):
                raise gc3libs.exceptions.InvalidUsage("Config file '%s' not found" % self.params.config_file)

            self._read_config_file(self.params.config_file)

        if self.params.input_folder:
            # Overwrite configuration file
            self.extra['input'] = self.params.input_folder

        if self.params.output_folder:
            # Overwrite configuration file
            self.extra['output'] = self.params.output_folder

        if not self.extra['input']:
            raise gc3libs.exceptions.InvalidUsage(
                "Missing required argument 'input_folder'. " \
                "Either set in configuration file as " \
                "'InputLocation' or using the '--input' option.")

        if not self.extra['output']:
            raise gc3libs.exceptions.InvalidUsage(
                "Missing required argument 'output_folder'. " \
                "Either set in configuration file as "
                "'OutputLocation' or using the '--input' option.")

        # Transform input and output references in Url
        if not self.extra['input'].endswith('/'):
            self.extra['input'] += '/'
        self.extra['input'] = gc3libs.url.Url(self.extra['input'])

        if not self.extra['output'].endswith('/'):
            self.extra['output'] += '/'
        self.extra['output'] = gc3libs.url.Url(self.extra['output'])

        # Read command line options
        if self.params.grok_bin:
            if not os.path.isfile(self.params.grok_bin):
                raise gc3libs.exceptions.InvalidUsage(
                    "grok binary '%s' does not exists"
                    % self.params.grok_bin)
            self.extra['grok'] = self.params.grok_bin
            self.log.info("Grok binary: [%s]" % self.extra['grok'])
        else:
            self.log.info("Grok binary: [use remote]")

        if self.params.hgs_bin:
            if not os.path.isfile(self.params.hgs_bin):
                raise gc3libs.exceptions.InvalidUsage(
                    "hgs binary '%s' does not exists"
                    % self.params.hgs_bin)
            self.extra['hgs'] = self.params.hgs_bin
            self.log.info("hgs binary: [%s]" % self.extra['hgs'])
        else:
            self.log.info("hgs binary: [use remote]")

        if self.params.s3cfg:
            if not os.path.isfile(self.params.s3cfg):
                raise gc3libs.exceptions.InvalidUsage(
                    "s3cfg config file '%s' does not exists"
                    % self.params.s3cfg)
            self.extra['s3cfg'] = self.params.s3cfg
            self.log.info("s3cfg config: [%s]" % self.extra['s3cfg'])
        else:
            self.log.info("s3cfg config: [use remote]")

        self.extra['is_daemon'] = self.params.is_daemon

        # Overwrite default only if set to False
        if self.params.preserve_output == False:
            self.extra['preserve_output'] = False

        # Get input/output folder URL schema
        if self.extra['input'].scheme.lower() == "s3" or self.extra['output'].scheme.lower() == "s3":
            # Then check validity of s3cmd configuration file
            if not os.path.isfile(self.extra['s3cfg']):
                raise gc3libs.exceptions.InvalidArgument("S3cmd config file '%s' not found" \
                                                         % self.extra['s3cfg'])
        if self.extra['input'].scheme.lower() == "file":
            # Check local filesystem
            if not os.path.exists(os.path.abspath(self.extra['input'].path)):
                raise gc3libs.exceptions.InvalidArgument("Local input folder %s not found" \
                                                         % os.path.abspath(self.extra['input'].path))
        if self.extra['output'].scheme.lower() == "file":
            # Check local folder; create it in case
            if not os.path.exists(os.path.abspath(self.extra['output'].path)):
                gc3libs.log.info("Creating output folder '%s'", self.extra['output'].path)
                os.makedirs(self.extra['output'].path)


        self.log.info("Input models location: [%s]" % str(self.extra['input']))
        self.log.info("Output results location: [%s]" % str(self.extra['output']))
        self.log.info("Running as daemon: [%s]" % str(self.extra['is_daemon']))



    def _create_task(self, input_model, output_model, **extra_args):

        # XXX: Weak
        # Convention is that input model files are named after the folder
        # they represent. So an input model:
        # "emme_v2_20131125-lowres-all2012_365d_monthly_DKpaper_LINUX.tgz"
        # will produce, once expanded, a folder named:
        # emme_v2_20131125-lowres-all2012_365d_monthly_DKpaper_LINUX
        working_dir = os.path.splitext(os.path.basename(str(input_model)))[0]

        extra_args['jobname'] = working_dir

        # FIXME: ignore SessionBasedScript feature of customizing
        # output folder
        # extra_args['output_dir'] = self.params.output
        extra_args['output_dir'] = os.path.join(self.extra['simulation_log_location'],extra_args['jobname'])

        extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', extra_args['jobname'])
        extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', extra_args['jobname'])
        extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', extra_args['jobname'])
        extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', extra_args['jobname'])

        extra_args['s3cfg'] = self.extra['s3cfg']

        self.log.info("Creating Task for input file: %s" % os.path.basename(str(input_model)))

        return GeoSphereTask(
            input_model,
            output_model,
            **extra_args
        )


    def new_tasks(self, extra):

        self.tasks = []

        for input_model in self._list_models_by_url(self.extra['input']):
            self.tasks.append(self._create_task(input_model, self._get_output(input_model), **extra.copy()))

        return self.tasks

    def every_main_loop(self):
        # Check for new tasks
        # eventually add them to self.controller

        gc3libs.log.debug("Reading S3 folder for new models")

        input_list = self._list_models_by_url(self.extra['input'])
        output_list = self._list_models_by_url(self.extra['output'])

        # for task in self.session:
        #     if task.execution.state in [Run.State.TERMINATED]:
        #         # Check whether output is present
        #         if task.output_model



        for input_model in self._list_models_by_url(self.extra['input']):
            # check if input_model is already associated to a running Task

            # Check for input not yet processed: create new task
            # Check for running tasks with missing input: terminate task
            # Check for missing output: re-start task (from TERMIANTED to NEW)

            skip = False

            # skip those 'input_model' that either have been already
            # processed or are already associated to a running task
            for task in self.session:
                if task.input_model == input_model:
                    skip = True
                    break

            if not skip:
                # Add as a new task
                gc3libs.log.info("Found new model to add: '%s'" % os.path.basename(str(input_model)))
                task =  self._create_task(input_model, self._get_output(input_model), **self.extra.copy())
                self.add(task)

        # # Remove all terminated tasks
        # for task in self.session.tasks:
        #     if task.state in [Run.State.TERMINATED]:
        #         # Remove task from controller
        #         # This should help free memory
        #         self.controlled.remove(task)
        #         self.session.remove(task.id)


    def _get_output(self, input_model):
        """
        Returns an output gc3libs.url.Url representing
        the expected output file procudes at the end of the
        GeoSphere simulation given the provided input_model.
        Agreed convention with the A4Mesh team:
        output: 'result_<input_model>_<date>.tgz'
        """
        return gc3libs.url.Url(os.path.join(str(self.extra['output']),
                                            "result_%s_%s.tgz" %
                                            (os.path.splitext(
                                                os.path.basename(
                                                    str(input_model)))[0],
                                             TODAY)))

    def _list_models_by_url(self, url):
        """
        """

        if url.scheme == "s3":
            return self._list_s3(url)
        elif url.scheme ==  "file":
            return self._list_local(url)
        else:
            gc3libs.log.error("Unsupported Input folder URL %s. "+
                              "Only supported protocols are: [file,s3] " % url.scheme)
            return None

    def _list_local(self, input_folder):
        """
        return a list of all .fastq files in the input folder
        """

        return [ gc3libs.url.Url(os.path.join(input_folder.path,infile)) for infile in os.listdir(input_folder.path) if infile.endswith('.tgz') ]

    def _list_s3(self, s3_url):
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
        # 's3cmd ls' should return a list of model archives
        # for each of them bundle archive_name and working_dir
        _command = "s3cmd "
        if self.extra['s3cfg']:
            _command += " -c %s " % self.extra['s3cfg']

        _command += " ls %s"  % str(s3_url)

        try:
            _process = subprocess.Popen(_command,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        close_fds=True, shell=True)

            (out, err) = _process.communicate()
            exitcode = _process.returncode

            if (exitcode != 0):
                gc3libs.log.error("Failed accessing S3 folder. "
                                  "Exitcode %s, error %s" % (exitcode,err))
                yield None
        except Exception, ex:
            gc3libs.log.error("Failed while reading remote S3 folder: "+
                              "%s", str(ex))
            yield None


        # Parse 'out_list' result and extract all .tgz or .zip archive names

        for s3_obj in out.strip().split("\n"):
            if s3_obj.startswith("DIR"):
                # it's a S3 directory; ignore
                continue
            # object string format: '2014-01-09 16:41 3627374 s3://a4mesh/model_1.tgz'
            try:
                s3_url = s3_obj.split()[3]
                if(s3_url.startswith("s3://")) and(s3_url.endswith("tgz")):
                    yield gc3libs.url.Url(s3_url)
            except IndexError:
                gc3libs.log.warning("Unexpected model file: '%s'. " \
                                    "Ingoring error." % s3_obj)
                continue


    def _main_loop_exitcode(self, stats):
        """
        This works in daemon mode;
        Thus do not end if all simulations have been
        processed. Continue and wait for new models to be uploaded.
        """
        if self.extra['is_daemon']:
            return 4
        else:
            return SessionBasedScript._main_loop_exitcode(self, stats)

    def _read_config_file(self, config_file):

        config = ConfigParser.RawConfigParser()
        config.read(self.params.config_file)
        if not config.has_section("default"):
            raise ConfigParser.ParsingError("Section 'default' not found")

        self.extra['s3cfg'] = os.path.expandvars(validate_config_option(config,
                                                                        "S3ConfigFileLocation",
                                                                        DEFAULT_S3CFG))

        self.extra['input'] = os.path.expandvars(validate_config_option(config,
                                                                        "InputLocation"))

        self.extra['output'] = os.path.expandvars(validate_config_option(config,
                                                                         "OutputLocation"))

        self.extra['preserve-output'] = validate_config_option(config,
                                                               "PreserveOutputWhenInputRemoved")


        self.extra['computing_resources'] = validate_config_option(config,
                                                                   "computing_resources",
                                                                   DEFAULT_CLOUD_ALL)

        self.extra['max_running'] = validate_config_option(config,
                                                           "max_running",
                                                           DEFAULT_MAX_RUNNING)

        self.extra['max_retries'] = validate_config_option(config,
                                                           "max_retries",
                                                           DEFAULT_MAX_RETRIES)

        self.extra['log_level'] = validate_config_option(config,
                                                         "log_level",
                                                         DEFAULT_LOG_LEVEL)

        self.extra['save_simulation_log'] = validate_config_option(config,
                                                                   "save_simulation_log",
                                                                   False)

        self.extra['simulation_log_location'] = os.path.expandvars(validate_config_option(config,
                                                                       "simulation_log_location",
                                                                       False))


###  TODO  ####
#
# . Wrapper script review
# . pass also 'working_dir' name to wrapper script
# . error in output_file location (distinguish between local vs S3)
# . error in output file location for wrapper

# wrapper script should be invoked as follow:
# ./geosphere_wrapper.sh -d s3://models/input/emme_v2_20131125-lowres-all2011_365d_monthly_DKpaper_LINUX.tgz emme_v2_20131125-lowres-all2011_365d_monthly_DKpaper_LINUX s3://models/output/result_emme_v2_20131125-lowres-all2011_365d_monthly_DKpaper_LINUX_20140507-1612.tgz
