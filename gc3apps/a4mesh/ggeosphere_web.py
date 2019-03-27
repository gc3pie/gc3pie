#! /usr/bin/env python
#
#   ggeosphere_web.py -- Front-end script for submitting multiple `GeoSphere` jobs.
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

See the output of ``ggeosphere_web.py --help`` for program usage instructions.
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
    import ggeosphere_web
    ggeosphere_web.GeoSphereScript().run()

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
import logging

# Default values
DEFAULT_S3CFG="$HOME/.s3cfg"
DEFAULT_CLOUD_ALL="*"
DEFAULT_MAX_RUNNING=10
DEFAULT_MAX_RETRIES=3
DEFAULT_LOG_LEVEL=logging.ERROR
A4MESH_CONFIG_FILE="/etc/a4mesh/a4mesh.cfg"

## custom application class
def validate_config_option(config, option_name, default_value=None):
    if not config.has_option("default",option_name):
        if default_value:
            gc3libs.log.warning("No option '%s' defined. "+
                                "Using default: '%s'" % (option_name, default_value))
            return default_value
        else:
            raise ConfigParser.ParsingError("No option '%s' defined." % option_name)
    else:
        return config.get('default',option_name)

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


        self.input_dir = input_dir

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


        # Application.__init__(
        #     self,
        #     # arguments should mimic the command line interfaca of the command to be
        #     # executed on the remote end
        #     arguments = cmd,
        #     inputs = inputs,
        #     outputs = [],
        #     stdout = 'geosphere.log',
        #     join=True,
        #     **extra_args)


        #XXX: TESTING PURPOSES...
        extra_args['requested_memory'] = 1*MB

        Application.__init__(
            self,
            # the following arguments are mandatory:
            arguments = ["/bin/hostname"],
            inputs = [],
            outputs = [],
            stdout = "stdout.txt",
            join = True,
            **extra_args)

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
    Loads the configuration file.
    Scans the specified INPUT for model files to simulate with
    ggeosphere. Submit a job for each one found; job progress is
    monitored and, when a job is done, its output files are retrieved back
    into the simulation directory itself.

    The ``ggeosphere_web`` is a variant of the ``ggeosphere`` script. It is
    conceived for a web-based execution. It does not terminate when all input models
    have been processed but it periodically checks whether new models have been added.
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
        self.add_param("-c", "--config",
                       metavar="PATH",
                       action="store",
                       default=A4MESH_CONFIG_FILE,
                       dest="config_file",
                       help="Location of the configuration file. "+
                       "Default: /etc/a4mesh/a4mesh.cfg")

    def parse_args(self):
        """
        Check presence of input folder (should contains R scripts).
        path to command_file should also be valid.
        """

        # Load configuration file
        if not os.path.isfile(self.params.config_file):
            raise gc3libs.exceptions.InvalidUsage("Config file '%s' not found" % self.params.config_file)

        self._read_config_file(self.params.config_file)

        self.input_folder_url = gc3libs.url.Url(os.path.join(self.extra['fs_endpoint'],
                                                             self.extra['model_input']))
        self.output_folder_url = gc3libs.url.Url(os.path.join(self.extra['fs_endpoint'],
                                                              self.extra['model_output']))

        if self.input_folder_url.scheme.lower() == "s3":
            # Then check validity of s3cmd configuration file
            if not os.path.isfile(self.extra['s3cmd_conf_location']):
                raise gc3libs.exceptions.InvalidArgument("S3cmd config file '%s' not found" \
                                                         % self.extra['s3cmd_conf_location'])
        elif self.input_folder_url.scheme.lower() == "file":
            # Check local filesystem
            if not os.path.exists(os.path.abspath(self.input_folder_url.path)):
                raise gc3libs.exceptions.InvalidArgument("Local input folder %s not found" \
                                                         % os.path.abspath(self.input_folder_url.path))

    def _create_task(self, input_dir, **extra_args):
        working_dir = os.path.splitext(os.path.basename(input_dir))[0]

        jobname = "a4mesh-%s" % working_dir

        extra_args['jobname'] = jobname

        # FIXME: ignore SessionBasedScript feature of customizing
        # output folder
        # extra_args['output_dir'] = self.params.output
        extra_args['output_dir'] = os.path.join(self.extra['simulation_log_location'],jobname)

        extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
        extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
        extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
        extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)

        extra_args['s3cfg'] = self.extra['s3cmd_conf_location']

        self.log.info("Creating Task for input file: %s" % input_dir)

        return GeoSphereTask(
            input_dir,
            working_dir,
            self.output_folder_url,
            **extra_args
        )


    def new_tasks(self, extra):

        self.tasks = []

        for input_dir in self._list_S3_container(self.input_folder_url, self.extra['s3cmd_conf_location']):
            self.tasks.append(self._create_task(input_dir, **extra.copy()))

        return self.tasks

    def every_main_loop(self):
        # Check for new tasks
        # eventually add them to self.controller

        gc3libs.log.debug("Reading S3 container for new models")

        for input_dir in self._list_S3_container(self.input_folder_url, self.extra['s3cmd_conf_location']):
            # check if input_dir is already associated to a running Task

            skip = False

            # skip those 'input_dir' that either have been already
            # processed or are already associated to a running task
            for task in self.session:
                if task.input_dir == input_dir:
                    skip = True
                    break

            if not skip:
                # Add as a new task
                gc3libs.log.debug("Found new model to add: '%s'" % input_dir)
                task =  self._create_task(input_dir, **self.extra.copy())
                self.add(task)

        # # Remove all terminated tasks
        # for task in self.session.tasks:
        #     if task.state in [Run.State.TERMINATED]:
        #         # Remove task from controller
        #         # This should help free memory
        #         self.controlled.remove(task)
        #         self.session.remove(task.id)

    def _main_loop_exitcode(self, stats):
        """
        This works in daemon mode;
        Thus do not end if all simulations have been
        processed. Continue and wait for new models to be uploaded.
        """
        return 4

    def _read_config_file(self, config_file):
        import ConfigParser

        config = ConfigParser.RawConfigParser()
        config.read(self.params.config_file)
        if not config.has_section("default"):
            raise ConfigParser.ParsingError("Section 'default' not found")


        self.extra['s3cmd_conf_location'] = validate_config_option(config,
                                                                   "s3cmd_conf_location",
                                                                   DEFAULT_S3CFG)

        self.extra['fs_endpoint'] = validate_config_option(config,
                                                           "fs_endpoint")

        self.extra['model_input'] = validate_config_option(config,
                                                           "model_input")

        self.extra['model_output'] = validate_config_option(config,
                                                            "model_output")

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

        self.extra['simulation_log_location'] = validate_config_option(config,
                                                                       "simulation_log_location",
                                                                       False)


    def _list_S3_container(self, s3_url, s3_config_file=None):
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
            _command = "s3cmd "
            if s3_config_file:
                _command += " -c %s " % s3_config_file

            _command += " ls %s/"  % str(s3_url)

            _process = subprocess.Popen(_command,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        close_fds=True, shell=True)

            (out, err) = _process.communicate()
            exitcode = _process.returncode

            if (exitcode != 0):
                gc3libs.log.error("Failed accessing S3 container. "
                                  "Exitcode %s, error %s" % (exitcode,err))
                yield None

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

##################

# Proposal: Associate input models with output results
# For each model stored in personal folder 'input'
# there should be a corresponding result (match by name ?)
# in personal 'output' folder
#
# The web interface builds a representation 'per-model'
# were the user can inspect the model and its associated result
# For those models whose result is not yet available (e.g. in processing)
# an icon is displayed instead.
# End users can remove both the input and the output for a given model;
# semantic of such operations is:
# * remove output: re-simulate the input model
# * remove input: remove also output
# * remove both: remove both


# Configuration files:
# 1. gc3pie.conf
# 2. s3cmd.conf
# 3. ggeosphere_web.conf

# Details of ggeosphere_web.conf
# S3 locations: input / output
# Computing cloud infrastructure: [GC3Pie resource(s)]
# Max number of simultaneous simulations
# Max number of retries per failed simulation
# Verbosity level
# Retain log information for each completed simulation [True/False]
# location where info on sompleted simulation should be stored
"""
example:

[default]
gc3pie_conf_location = /etc/gc3/gc3pie.conf
s3cmd_conf_location = /etc/s3cmd/s3cfg
s3_endpoint = s3://models
s3_input = 'input'
s3_output = 'output'
computing_resources = a4mesh.uzh, a4mesh.zhaw, a4mesh.hes-so
max_running = 10
max_retries = 2
log_level = DEBUG
save_simulation_log = False
simulation_log_location = '/tmp/simulation_logs/

"""
