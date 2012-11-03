#! /usr/bin/env python
#
"""
  Finding minimum energy for a certain set of geometries. 
"""

# Copyright (C) 2011, 2012 University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
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
__version__ = '$Revision$'
__author__ = 'Benjamin Jonen <benjamin.jonen@bf.uzh.ch>'
# summary of user-visible changes
__changelog__ = """

"""
__docformat__ = 'reStructuredText'



import os
import sys
from gc3libs import Application
from gc3libs.cmdline import SessionBasedScript
sys.path.append('../../')
from support_gc3 import update_parameter_in_file
# optimizer specific imports
from dif_evolution import DifferentialEvolution
from gc3libs.application.gamess import GamessApplication
import numpy as np

import logging
import gc3libs
gc3libs.configure_logger(logging.DEBUG)

import gc3libs.optimizer.global_opt
from gc3libs.optimizer.global_opt import GlobalOptimizer 
from dif_evolution import DifferentialEvolution

optimization_dir = os.path.join(os.getcwd(), 'optimizeRosenBrock')

float_fmt = '%25.15f'

from gc3libs import Run
class Error(Exception):
    pass

class Abort(Error):
    """Raised when an application exits unexpectedly.

    :class:`Abort` takes a single integer argument indicating the exit status of
    the application.

    .. versionadded:: 1.0.4
    """

    def __init__(self, status):
        self.status = status
        message = "Application terminated (%s)" % self.status
        super(Abort, self).__init__(message, self.status)


import lockfile, cli
def sessionBasedScriptRun(self):
    """
    Execute `cli.app.Application.run`:meth: if any exception is
    raised, catch it, output an error message and then exit with
    an appropriate error code.
    """
    try:
        return cli.app.CommandLineApp.run(self)
    except gc3libs.exceptions.InvalidUsage, ex:
        # Fatal errors do their own printing, we only add a short usage message
        sys.stderr.write("Type '%s --help' to get usage help.\n" % self.name)
        return 64  # EX_USAGE in /usr/include/sysexits.h
    except KeyboardInterrupt:
        sys.stderr.write("%s: Exiting upon user request (Ctrl+C)\n" % self.name)
        return 13
    except SystemExit, ex:
        #  sys.exit() has been called in `post_run()`.
        raise
    # the following exception handlers put their error message
    # into `msg` and the exit code into `rc`; the closing stanza
    # tries to log the message and only outputs it to stderr if
    # this fails
    except lockfile.Error, ex:
        exc_info = sys.exc_info()
        msg = ("Error manipulating the lock file (%s: %s)."
               " This likely points to a filesystem error"
               " or a stale process holding the lock."
               " If you cannot get this command to run after"
               " a system reboot, please write to gc3pie@googlegroups.com"
               " including any output you got by running '%s -vvvv %s'.")
        if len(sys.argv) > 0:
            msg %= (ex.__class__.__name__, str(ex),
                    self.name, str.join(' ', sys.argv[1:]))
        else:
            msg %= (ex.__class__.__name__, str(ex), self.name, '')
        rc = 1
    except AssertionError, ex:
        exc_info = sys.exc_info()
        msg = ("BUG: %s\n"
               "Please send an email to gc3pie@googlegroups.com"
               " including any output you got by running '%s -vvvv %s'."
               " Thanks for your cooperation!")
        if len(sys.argv) > 0:
            msg %= (str(ex), self.name, str.join(' ', sys.argv[1:]))
        else:
            msg %= (str(ex), self.name, '')
        rc = 1
    # except Exception, ex:
    #     msg = "%s: %s" % (ex.__class__.__name__, str(ex))
    #     if isinstance(ex, cli.app.Abort):
    #         rc = (ex.status)
    #     elif isinstance(ex, EnvironmentError):
    #         rc = 74  # EX_IOERR in /usr/include/sysexits.h
    #     else:
    #         # generic error exit
    #         rc = 1
    # output error message and -maybe- backtrace...
    try:
        self.log.critical(msg,
                          exc_info=(self.params.verbose > self.verbose_logging_threshold + 2))
    except:
        # no logging setup, output to stderr
        sys.stderr.write("%s: FATAL ERROR: %s\n" % (self.name, msg))
        if self.params.verbose > self.verbose_logging_threshold + 2:
            sys.excepthook(* sys.exc_info())
    # ...and exit
    return 1

from gc3libs.cmdline import SessionBasedScript
SessionBasedScript.run = sessionBasedScriptRun

def engineProgress(self):
    """
    Update state of all registered tasks and take appropriate action.
    Specifically:

      * tasks in `NEW` state are submitted;

      * the state of tasks in `SUBMITTED`, `RUNNING`, `STOPPED` or `UNKNOWN` state is updated;

      * when a task reaches `TERMINATING` state, its output is downloaded.

      * tasks in `TERMINATED` status are simply ignored.

    The `max_in_flight` and `max_submitted` limits (if >0) are
    taken into account when attempting submission of tasks.
    """
    # prepare
    currently_submitted = 0
    currently_in_flight = 0
    if self.max_in_flight > 0:
        limit_in_flight = self.max_in_flight
    else:
        limit_in_flight = utils.PlusInfinity()
    if self.max_submitted > 0:
        limit_submitted = self.max_submitted
    else:
        limit_submitted = utils.PlusInfinity()

    # update status of SUBMITTED/RUNNING tasks before launching
    # new ones, otherwise we would be checking the status of
    # some tasks twice...
    #gc3libs.log.debug("Engine.progress: updating status of tasks [%s]"
    #                  % str.join(', ', [str(task) for task in self._in_flight]))
    transitioned = []
    for index, task in enumerate(self._in_flight):
        try:
            self._core.update_job_state(task)
            if self._store and task.changed:
                self._store.save(task)
            state = task.execution.state
            if state == Run.State.SUBMITTED:
                # only real applications need to be counted
                # against the limit; policy tasks are exempt
                # (this applies to all similar clause below)
                if isinstance(task, Application):
                    currently_submitted += 1
                    currently_in_flight += 1
            # elif state == Run.State.RUNNING or state == Run.State.UNKNOWN:
            elif state == Run.State.RUNNING:
                if isinstance(task, Application):
                    currently_in_flight += 1
            elif state == Run.State.STOPPED:
                transitioned.append(index) # task changed state, mark as to remove
                self._stopped.append(task)
            elif state == Run.State.TERMINATING:
                transitioned.append(index) # task changed state, mark as to remove
                self._terminating.append(task)
            elif state == Run.State.TERMINATED:
                transitioned.append(index) # task changed state, mark as to remove
                self._terminated.append(task)
        except gc3libs.exceptions.ConfigurationError:
            # Unrecoverable; no sense in continuing -- pass
            # immediately on to client code and let it handle
            # this...
            raise
        # except Exception, x:
        #     gc3libs.log.error("Ignoring error in updating state of task '%s': %s: %s"
        #                       % (task, x.__class__.__name__, str(x)),
        #                       exc_info=True)
    # remove tasks that transitioned to other states
    for index in reversed(transitioned):
        del self._in_flight[index]

    # execute kills and update count of submitted/in-flight tasks
    #gc3libs.log.debug("Engine.progress: killing tasks [%s]"
    #                  % str.join(', ', [str(task) for task in self._to_kill]))
    transitioned = []
    for index, task in enumerate(self._to_kill):
        try:
            old_state = task.execution.state
            self._core.kill(task)
            if self._store:
                self._store.save(task)
            if old_state == Run.State.SUBMITTED:
                if isinstance(task, Application):
                    currently_submitted -= 1
                    currently_in_flight -= 1
            elif old_state == Run.State.RUNNING:
                if isinstance(task, Application):
                    currently_in_flight -= 1
            self._terminated.append(task)
            transitioned.append(index)
        except Exception, x:
            gc3libs.log.error("Ignored error in killing task '%s': %s: %s",
                              task, x.__class__.__name__, str(x))
            # print again with traceback info at a higher log level
            gc3libs.log.debug("Ignored error in killing task '%s': %s: %s",
                              task, x.__class__.__name__, str(x), exc_info=True)
    # remove tasks that transitioned to other states
    for index in reversed(transitioned):
        del self._to_kill[index]

    # update state of STOPPED tasks; again need to make before new
    # submissions, because it can alter the count of in-flight
    # tasks.
    #gc3libs.log.debug("Engine.progress: updating status of stopped tasks [%s]"
    #                  % str.join(', ', [str(task) for task in self._stopped]))
    transitioned = []
    for index, task in enumerate(self._stopped):
        try:
            self._core.update_job_state(task)
            if self._store and task.changed:
                self._store.save(task)
            state = task.execution.state
            if state in [Run.State.SUBMITTED, Run.State.RUNNING]:
                if isinstance(task, Application):
                    currently_in_flight += 1
                    if task.execution.state == Run.State.SUBMITTED:
                        currently_submitted += 1
                self._in_flight.append(task)
                transitioned.append(index) # task changed state, mark as to remove
            elif state == Run.State.TERMINATING:
                self._terminating.append(task)
                transitioned.append(index) # task changed state, mark as to remove
            elif state == Run.State.TERMINATED:
                self._terminated.append(task)
                transitioned.append(index) # task changed state, mark as to remove
        except Exception, x:
            gc3libs.log.error("Ignoring error in updating state of STOPPED task '%s': %s: %s"
                              % (task, x.__class__.__name__, str(x)),
                              exc_info=True)
    # remove tasks that transitioned to other states
    for index in reversed(transitioned):
        del self._stopped[index]

    # now try to submit NEW tasks
    #gc3libs.log.debug("Engine.progress: submitting new tasks [%s]"
    #                  % str.join(', ', [str(task) for task in self._new]))
    transitioned = []
    if self.can_submit:
        index = 0
        while (currently_submitted < limit_submitted
               and currently_in_flight < limit_in_flight
               and index < len(self._new)):
            task = self._new[index]
            # try to submit; go to SUBMITTED if successful, FAILED if not
            if currently_submitted < limit_submitted and currently_in_flight < limit_in_flight:
                try:
                    self._core.submit(task)
                    if self._store:
                        self._store.save(task)
                    self._in_flight.append(task)
                    transitioned.append(index)
                    if isinstance(task, Application):
                        currently_submitted += 1
                        currently_in_flight += 1
                except Exception, x:
                    gc3libs.log.error("Ignored error in submitting task '%s': %s: %s",
                                      task, x.__class__.__name__, str(x))
                    # print again with traceback at a higher log level
                    gc3libs.log.debug("Ignored error in submitting task '%s': %s: %s",
                                      task, x.__class__.__name__, str(x), exc_info=True)
                    # record the fact in the task's history
                    task.execution.history("Submission failed: %s: %s"
                                           % (x.__class__.__name__, str(x)))
            index += 1
    # remove tasks that transitioned to SUBMITTED state
    for index in reversed(transitioned):
        del self._new[index]

    # finally, retrieve output of finished tasks
    #gc3libs.log.debug("Engine.progress: fetching output of tasks [%s]"
    #                  % str.join(', ', [str(task) for task in self._terminating]))
    if self.can_retrieve:
        transitioned = []
        for index, task in enumerate(self._terminating):
            # try to get output
            try:
                self._core.fetch_output(task)
            except gc3libs.exceptions.UnrecoverableDataStagingError, ex:
                gc3libs.log.error("Error in fetching output of task '%s',"
                                  " will mark it as TERMINATED"
                                  " (with error exit code %d): %s: %s",
                                  task, posix.EX_IOERR,
                                  ex.__class__.__name__, str(ex), exc_info=True)
                task.execution.returncode = (Run.Signals.DataStagingFailure,
                                             posix.EX_IOERR)
                task.execution.state = Run.State.TERMINATED
                task.changed = True
            # except Exception, x:
            #     gc3libs.log.error("Ignored error in fetching output of task '%s': %s: %s",
            #                       task, x.__class__.__name__, str(x))
            #     gc3libs.log.debug("Ignored error in fetching output of task '%s': %s: %s",
            #                       task, x.__class__.__name__, str(x), exc_info=True)
            if task.execution.state == Run.State.TERMINATED:
                self._terminated.append(task)
                self._core.free(task)
                transitioned.append(index)
            if self._store and task.changed:
                self._store.save(task)
        # remove tasks for which final output has been retrieved
        for index in reversed(transitioned):
            del self._terminating[index]

import gc3libs.core
gc3libs.core.Engine.progress = engineProgress





def compute_target_geometries(pop_task_tuple):
    '''
      Given a list of (population, task), compute and return list of target 
      values. 
    '''
    import re
    enrgstr = re.compile(r'FINAL .+ ENERGY IS +(?P<enrgstr>-[0-9]+\.[0-9]+)')
    fxVals = []
    for (pop, task) in pop_task_tuple:
        outputDir = task.output_dir
        f = open(os.path.join(outputDir, 'games.log'))
        content = f.read()
        f.close()
        match = enrgstr.search(content)
        if match:
            fxVal = float(match.group('enrgstr'))
        fxVals.append(fxVal)
    return fxVals

def create_gammes_input_file(geom, dirname):
    '''
      geom: 1d numpy array defining the geometry to produce an input file for. 
    '''

    import os
    import numpy as np

    inptmpl = []
    inptmpl.append("""
    $CONTRL SCFTYP=UHF RUNTYP=ENERGY $END
     $BASIS GBASIS=STO NGAUSS=3 $END
     $DATA 
    Title
    C1
    """)
    inptmpl.append('$END')

    inpfl = 'H2CO3'
    natm = 6
    element = ('C', 'O', 'O', 'O', 'H', 'H')
    nchrg = (6.0, 8.0, 8.0, 8.0, 1.0, 1.0)
    ngeom = 1
#    geom = np.array([[ 1.,1.,1.,2.,2.,2.,3.,3.,3.,4.,4.,4.,5.,5.,5.,6.,6.,6.]])
#    dirname = 'blub'

    # creating directory for input files for current set of geometries
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    # GENERATING INPUT FILES FOR GAMESS

    # taking ith geometry / molecule
    geomstr = ''
    for j in xrange(natm):
        # taking jth atom of current molecule
        geomstr = geomstr + element[j] + '  ' + str(nchrg[j]) + \
            '  ' + '%10.8f'%geom[3*j] + '  ' + '%10.8f'%geom[3*j+1] + \
            '  ' + '%11.8f'%geom[3*j+2] + '\n'
    file_name = os.path.join(dirname, inpfl+'.inp')
    file = open(file_name, 'w')
    file.write(inptmpl[0] + geomstr + inptmpl[1])
    file.close()
    return file_name

#create_gammes_input_file(np.array([ 1.,1.,1.,2.,2.,2.,3.,3.,3.,4.,4.,4.,5.,5.,5.,6.,6.,6.]), os.getcwd())
#print 'done'

def task_constructor_geometries(x_vals, iteration_directory, **extra_args):
    '''
      Given solver guess `x_vals`, return an instance of :class:`Application`
      set up to produce the output :def:`target_fun` of :class:`GlobalOptimizer'
      analyzes to produce the corresponding function values. 
    '''
    import shutil

    # Set some initial variables
    path_to_geometries_example = os.getcwd()

    index = 0 # We are dealing with scalar inputs

    jobname = 'para_' + '_'.join([('%8.3f' % val).strip() for val in x_vals])
    path_to_stage_dir = os.path.join(iteration_directory, jobname)
    path_to_stage_base_dir = os.path.join(path_to_stage_dir, 'base')
    inp_file_path = create_gammes_input_file(x_vals, path_to_stage_base_dir)

    kwargs = extra_args # e.g. architecture
    kwargs['stdout'] = 'games' + '.log'
#    kwargs['join'] = True
    kwargs['output_dir'] =  os.path.join(path_to_stage_dir, 'output')
    gc3libs.log.debug("Output dir: %s" % kwargs['output_dir'])
    kwargs['requested_architecture'] = 'x86_64'
    kwargs['requested_cores'] = 1    
    kwargs['verno'] = '2011R1'
    
    return GamessApplication(inp_file_path=inp_file_path, **kwargs)

    ### Generate input file in path_to_stage_dir

    ##shutil.copytree(base_dir, path_to_stage_base_dir, ignore=shutil.ignore_patterns('.svn'))
    ##for var, val, para_file, para_file_format in zip(x_vars, x_vals, para_files, para_file_formats):
        ##val = (float_fmt % val).strip() 
        ##update_parameter_in_file(os.path.join(path_to_stage_base_dir, para_file),
                    ##var, index, val, para_file_format)

    #prefix_len = len(path_to_stage_base_dir) + 1        
    ## start the inputs dictionary with syntax: client_path: server_path
    #inputs = {}
    #for dirpath,dirnames,filenames in os.walk(path_to_stage_base_dir):
        #for filename in filenames:
        ## cut the leading part, which is == to path_to_stage_dir
        #relpath = dirpath[prefix_len:]
        ## ignore output directory contents in resubmission
        #if relpath.startswith('output'):
            #continue
        #remote_path = os.path.join(relpath, filename)
        #inputs[os.path.join(dirpath, filename)] = remote_path
    ## all contents of the `output` directory are to be fetched
    ## outputs = { 'output/':'' }
    #outputs = gc3libs.ANY_OUTPUT
    ##{ '*':'' }
    ##kwargs = extra.copy()
    #kwargs = extra_args # e.g. architecture
    #kwargs['stdout'] = executable + '.log'
    #kwargs['join'] = True
    #kwargs['output_dir'] =  os.path.join(path_to_stage_dir, 'output')
    #gc3libs.log.debug("Output dir: %s" % kwargs['output_dir'])
    #kwargs['requested_architecture'] = 'x86_64'
    #kwargs['requested_cores'] = 1
    ## hand over job to create


class GeometriesScript(SessionBasedScript):
    """
      Execute Geometries optimization. 
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = '0.2',
            stats_only_for = Application
        )    

    def new_tasks(self, extra):     
        
        path_to_stage_dir = os.getcwd()
        n_atoms = 6
        vec_dimension = 3 * n_atoms
    
        de_solver = DifferentialEvolution(dim = vec_dimension, pop_size = 10, de_step_size = 0.85, prob_crossover = 1., itermax = 200, 
                                      y_conv_crit = 0.1, de_strategy = 1, plotting = False, working_dir = path_to_stage_dir, 
                                      lower_bds = [-2] * vec_dimension, upper_bds = [2] * vec_dimension, x_conv_crit = None, verbosity = 'DEBUG')
    
        initial_pop = []
        if not initial_pop:
            de_solver.newPop = de_solver.drawInitialSample()
        else:
            de_solver.newPop = initial_pop    
        
        # create an instance globalObt
         
        jobname = 'geometries'
        kwargs = extra.copy()
        kwargs['path_to_stage_dir'] = path_to_stage_dir
        kwargs['optimizer'] = de_solver
        kwargs['task_constructor'] = task_constructor_geometries
        kwargs['target_fun'] = compute_target_geometries

        
        return [GlobalOptimizer(jobname=jobname, **kwargs)]


            
if __name__ == '__main__':
    print 'starting'
    if os.path.isdir(optimization_dir):
        import shutil
        shutil.rmtree(optimization_dir)
    os.mkdir(optimization_dir)
    GeometriesScript().run()
    print 'done'
