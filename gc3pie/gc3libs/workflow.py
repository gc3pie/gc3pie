#! /usr/bin/env python
#
"""
Implementation of task collections.

Tasks can be grouped into collections, which are tasks themselves,
therefore can be controlled (started/stopped/cancelled) like a single
whole.  Collection classes provided in this module implement the basic
patterns of job group execution; they can be combined to form more
complex workflows.  Hook methods are provided so that derived classes
can implement problem-specific job control policies.
"""
# Copyright (C) 2009-2012 GC3, University of Zurich. All rights reserved.
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
__version__ = '2.0.0-rc4 version (SVN $Revision$)'

import time
import os

from gc3libs.compat.collections import defaultdict

from gc3libs import log, Run, Task
import gc3libs.exceptions
import gc3libs.utils


class TaskCollection(Task):
    """
    Base class for all task collections. A "task collection" is a
    group of tasks, that can be managed collectively as a single one.

    A task collection implements the same interface as the `Task`
    class, so you can use a `TaskCollection` everywhere a `Task` is
    required.  A task collection has a `state` attribute, which is an
    instance of `gc3libs.Run.State`; each concrete collection class
    decides how to deduce a collective state based on the individual
    task states.
    """

    def __init__(self, tasks=None, **extra_args):
        if tasks is None:
            self.tasks = [ ]
        else:
            self.tasks = tasks
        Task.__init__(self, **extra_args)

    # manipulate the "controller" interface used to control the associated task
    def attach(self, controller):
        """
        Use the given Controller interface for operations on the job
        associated with this task.
        """        
        raise NotImplementedError("Called abstract method TaskCollection.attach() - this should be overridden in derived classes.")

    def detach(self):
        for task in self.tasks:
            task.detach()
        Task.detach(self)


    def add(self, task):
        """
        Add a task to the collection.
        """
        raise NotImplementedError("Called abstract method TaskCollection.add() - this should be overridden in derived classes.")


    def remove(self, task):
        """
        Remove a task from the collection.
        """
        self.tasks.remove(task)
        task.detach()


    # task execution manipulation -- these methods should be overriden
    # in derived classes, to implement the desired policy.

    def submit(self, resubmit=False, **extra_args):
        raise NotImplementedError("Called abstract method TaskCollection.submit() - this should be overridden in derived classes.")


    def update_state(self, **extra_args):
        """
        Update the running state of all managed tasks.
        """
        for task in self.tasks:
            self._controller.update_job_state(task, **extra_args)


    def kill(self, **extra_args):
        # XXX: provide default implementation that kills all jobs?
        raise NotImplementedError("Called abstract method TaskCollection.kill() - this should be overridden in derived classes.")


    def fetch_output(self, output_dir=None, overwrite=False, **extra_args):
        # if `output_dir` is not None, it is interpreted as the base
        # directory where to download files; each task will get its
        # own subdir based on its `.persistent_id`
        coll_output_dir = self._get_download_dir(output_dir)
        for task in self.tasks:
            if task.execution.state == Run.State.TERMINATED:
                continue
            if 'output_dir' in task:
                task_output_dir = task.output_dir
            else:
                task_output_dir = task.persistent_id
            # XXX: uses a feature from `os.path.join`: if the second
            # path is absolute, the first path is discarded and the
            # second one is returned unchanged
            task_output_dir = os.path.join(coll_output_dir, task_output_dir)
            self._controller.fetch_output(
                task,
                task_output_dir,
                overwrite,
                **extra_args)
        for task in self.tasks:
            if task.execution.state != Run.State.TERMINATED:
                return coll_output_dir
        self.execution.state = Run.State.TERMINATED
        self.changed = True
        return coll_output_dir


    def peek(self, what, offset=0, size=None, **extra_args):
        """
        Raise a `gc3libs.exceptions.InvalidOperation` error, as there
        is no meaningful semantics that can be defined for `peek` into
        a generic collection of tasks.
        """
        # is there any sensible semantic here?
        raise gc3libs.exceptions.InvalidOperation("Cannot `peek()` on a task collection.")

    def progress(self):
        raise NotImplementedError("Called abstract method TaskCollection.progress() - this should be overridden in derived classes.")



    def stats(self, only=None):
        """
        Return a dictionary mapping each state name into the count of
        tasks in that state. In addition, the following keys are defined:

        * `ok`:  count of TERMINATED tasks with return code 0

        * `failed`: count of TERMINATED tasks with nonzero return code

        * `total`: count of managed tasks, whatever their state

        If the optional argument `only` is not None, tasks whose
        class is not contained in `only` are ignored.

        :param tuple only: Restrict counting to tasks of these classes.

        """
        result = defaultdict(lambda: 0)
        for task in self.tasks:
            if only and not isinstance(task, only):
                continue
            state = task.execution.state
            result[state] += 1
            if state == Run.State.TERMINATED:
                if task.execution.returncode == 0:
                    result['ok'] += 1
                else:
                    result['failed'] += 1
        if only:
            result['total'] = len([task for task in self.tasks
                                                 if isinstance(task, only)])
        else:
            result['total'] = len(self.tasks)
        return result


    def terminated(self):
        """
        Called when the job state transitions to `TERMINATED`, i.e.,
        the job has finished execution (with whatever exit status, see
        `returncode`) and the final output has been retrieved.

        Default implementation for `TaskCollection` is to set the
        exitcode to the maximum of the exit codes of its tasks.
        """
        self.execution._exitcode = max(
            task.execution._exitcode for task in self.tasks
            )

class SequentialTaskCollection(TaskCollection):
    """
    A `SequentialTaskCollection` runs its tasks one at a time.

    After a task has completed, the `next` method is called with the
    index of the finished task in the `self.tasks` list; the return
    value of the `next` method is then made the collection
    `execution.state`.  If the returned state is `RUNNING`, then the
    subsequent task is started, otherwise no action is performed.

    The default `next` implementation just runs the tasks in the order
    they were given to the constructor, and sets the state to
    `TERMINATED` when all tasks have been run.
    """

    def __init__(self, tasks, **extra_args):
        # XXX: check that `tasks` is a sequence type
        TaskCollection.__init__(self, tasks, **extra_args)
        self._current_task = 0

    def add(self, task):
        task.detach()
        self.tasks.append(task)

    def attach(self, controller):
        """
        Use the given Controller interface for operations on the job
        associated with this task.
        """
        for task in self.tasks:
            if not task._attached:
                task.attach(controller)
                break
        Task.attach(self, controller)

    def kill(self, **extra_args):
        """
        Stop execution of this sequence.  Kill currently-running task
        (if any), then set collection state to TERMINATED.
        """
        if self._current_task is not None:
            self.tasks[self._current_task].kill(**extra_args)
        self.execution.state = Run.State.TERMINATED
        self.execution.returncode = (Run.Signals.Cancelled, -1)
        self.changed = True


    def next(self, done):
        """
        Return the state or task to run when step number `done` is completed.

        This method is called when a task is finished; the `done`
        argument contains the index number of the just-finished task
        into the `self.tasks` list.  In other words, the task that
        just completed is available as `self.tasks[done]`.

        The return value from `next` can be either a task state (i.e.,
        an instance of `Run.State`), or a valid index number for
        `self.tasks`. In the first case:

        - if the return value is `Run.State.TERMINATED`,
          then no other jobs will be run;
        - otherwise, the return value is assigned to `execution.state`
          and the next job in the `self.tasks` list is executed.

        If instead the return value is a (nonnegative) number, then
        tasks in the sequence will be re-run starting from that index.

        The default implementation runs tasks in the order they were
        given to the constructor, and sets the state to TERMINATED
        when all tasks have been run.  This method can (and should) be
        overridden in derived classes to implement policies for serial
        job execution.
        """
        if done == len(self.tasks) - 1:
            return Run.State.TERMINATED
        else:
            return Run.State.RUNNING


    def submit(self, resubmit=False, **extra_args):
        """
        Start the current task in the collection.
        """
        if self._current_task is None:
            self._current_task = 0
        task = self.tasks[self._current_task]
        task.submit(resubmit, **extra_args)
        if task.execution.state == Run.State.NEW:
            # submission failed, state unchanged
            self.execution.state = Run.State.NEW
        elif task.execution.state == Run.State.SUBMITTED:
            self.execution.state = Run.State.SUBMITTED
        else:
            self.execution.state = Run.State.RUNNING
        self.changed = True
        return self.execution.state


    def update_state(self, **extra_args):
        """
        Update state of the collection, based on the jobs' statuses.
        """
        if self._current_task is None:
            # it's either NEW or TERMINATED, no update
            assert self.execution.state in [ Run.State.NEW, Run.State.TERMINATED ]
            pass
        else:
            # update state of current task
            task = self.tasks[self._current_task]
            task.update_state(**extra_args)
            gc3libs.log.debug("Task #%d in state %s"
                             % (self._current_task, task.execution.state))
        # set state based on the state of current task
        if self._current_task == 0 and task.execution.state in [ Run.State.NEW, Run.State.SUBMITTED ]:
            self.execution.state = task.execution.state
        elif (task.execution.state == Run.State.TERMINATED):
            nxt = self.next(self._current_task)
            if nxt in Run.State:
                self.execution.state = nxt
                if self.execution.state not in [ Run.State.STOPPED,
                                                 Run.State.TERMINATED ]:
                    self._current_task += 1
                    self.changed = True
                    next_task = self.tasks[self._current_task]
                    next_task.attach(self._controller)
                    self.submit(resubmit=True)
            else:
                # `nxt` must be a valid index into `self.tasks`
                self._current_task = nxt
                self.submit(resubmit=True)
        else:
            self.execution.state = Run.State.RUNNING
        return self.execution.state


class StagedTaskCollection(SequentialTaskCollection):
    """
    Simplified interface for creating a sequence of Tasks.
    This can be used when the number of Tasks to run is
    fixed and known at program writing time.

    A `StagedTaskCollection` subclass should define methods `stage0`,
    `stage1`, ... up to `stageN` (for some arbitrary value of N positive
    integer).  Each of these `stageN` must return a `Task`:class:
    instance; the task returned by the `stage0` method will be executed
    first, followed by the task returned by `stage1`, and so on.
    The sequence stops at the first N such that `stageN` is not defined.

    The exit status of the whole sequence is the exit status of the
    last `Task` instance run.  However, if any of the `stageX` methods
    returns an integer value instead of a `Task` instance, then the
    sequence stops and that number is used as the sequence exit
    code.

    """
    def __init__(self, **extra_args):
        try:
            first_stage = self.stage0()
            if isinstance(first_stage, Task):
                # init parent class with the initial task
                SequentialTaskCollection.__init__(self, [first_stage], **extra_args)
            elif isinstance(first_stage, (int, long, tuple)):
                # init parent class with no tasks, an dimmediately set the exitcode
                SequentialTaskCollection.__init__(self, [], **extra_args)
                self.execution.returncode = first_stage
                self.execution.state = Run.State.TERMINATED
            else:
                raise AssertionError("Invalid return value from method `stage0()` of"
                                     " `StagedTaskCollection` object %r:"
                                     " must return `Task` instance or number" % self)
        except AttributeError, ex:
            raise AssertionError("Invalid `StagedTaskCollection` instance %r: %s"
                                 % (self, str(ex)))


    def next(self, done):
        # get next stage (1); if none exists, log it and exit
        try:
            next_stage_fn = getattr(self, "stage%d" % (done+1))
        except AttributeError:
            gc3libs.log.debug("StagedTaskCollection '%s' has no stage%d,"
                              " ending sequence now.", self, (done+1))
            self.execution.returncode = self.tasks[done].execution.returncode
            return Run.State.TERMINATED
        # get next stage (2); if we get an error here, something is wrong in the code
        try:
            next_stage = next_stage_fn()
        except AttributeError, err:
            raise AssertionError("Invalid `StagedTaskCollection` instance %r: %s"
                                 % (self, str(err)))
        # add next stage to the collection, or end graciously
        if isinstance(next_stage, Task):
            self.add(next_stage)
            return Run.State.RUNNING
        elif isinstance(next_stage, (int, long, tuple)):
            self.execution.returncode = next_stage
            return Run.State.TERMINATED
        else:
            raise AssertionError("Invalid return value from method `stage%d()` of"
                                 " `StagedTaskCollection` object %r:"
                                 " must return `Task` instance or number"
                                 % (done+1, self))



class ParallelTaskCollection(TaskCollection):
    """
    A `ParallelTaskCollection` runs all of its tasks concurrently.

    The collection state is set to `TERMINATED` once all tasks have
    reached the same terminal status.
    """

    def __init__(self, tasks=None, **extra_args):
        TaskCollection.__init__(self, tasks, **extra_args)


    def _state(self):
        """
        Return the state of the collection.

        For a `ParallelTaskCollection`, the state of dependent jobs is
        computed by looping across the states STOPPED, RUNNING,
        SUBMITTED, TERMINATING, UNKNOWN, TERMINATED, NEW in the order
        given: the first state for which there is at least one job in
        that state is returned as the overall collection state.  As an
        exception, if the collection is a mixture of NEW and
        TERMINATED jobs, then the global state is RUNNING (presuming
        we're in the middle of a computation).
        """
        stats = self.stats()
        if (stats[Run.State.NEW] > 0
            and stats[Run.State.TERMINATED] > 0
            and stats[Run.State.NEW] + stats[Run.State.TERMINATED] == len(self.tasks)):
            # we're in the middle of a computation (there's a mixture
            # of unsubmitted and finished tasks), so let's chalk this
            # up to ``RUNNING`` state
            return Run.State.RUNNING
        for state in [ Run.State.STOPPED,
                       Run.State.RUNNING,
                       Run.State.SUBMITTED,
                       Run.State.UNKNOWN,
                       Run.State.TERMINATING,
                       # if we get here, then all jobs are TERMINATED or all NEW
                       Run.State.TERMINATED,
                       Run.State.NEW,
                       ]:
            if stats[state] > 0:
                return state
        return Run.State.UNKNOWN

    def add(self, task):
        """
        Add a task to the collection.
        """
        task.detach()
        self.tasks.append(task)
        if self._attached:
            task.attach(self._controller)

    def attach(self, controller):
        """
        Use the given Controller interface for operations on the job
        associated with this task.
        """
        for task in self.tasks:
            if not task._attached:
                task.attach(controller)
        Task.attach(self, controller)

    def kill(self, **extra_args):
        """
        Terminate all tasks in the collection, and set collection
        state to `TERMINATED`.
        """
        for task in self.tasks:
            task.kill(**extra_args)
        self.execution.state = Run.State.TERMINATED
        self.execution.returncode = (Run.Signals.Cancelled, -1)
        self.changed = True


    def progress(self):
        """
        Try to advance all jobs in the collection to the next state in
        a normal lifecycle.  Return list of task execution states.
        """
        return [ task.progress() for task in self.tasks ]


    def submit(self, resubmit=False, **extra_args):
        """
        Start all tasks in the collection.
        """
        for task in self.tasks:
            task.submit(resubmit, **extra_args)
        self.execution.state = self._state()


    def update_state(self, **extra_args):
        """
        Update state of all tasks in the collection.
        """
        for task in self.tasks:
            #gc3libs.log.debug("Updating state of %s in collection %s ..."
            #                  % (task, self))
            task.update_state(**extra_args)
        self.execution.state = self._state()
        if self.execution.state == Run.State.TERMINATED:
            self.execution.returncode = (0, 0)
            # set exitcode based on returncode of sub-tasks
            for task in self.tasks:
                if task.execution.returncode != 0:
                    self.execution.exitcode = 1
            # FIXME: incorrectly sets `changed` each time it's called!
            self.changed = True


class ChunkedParameterSweep(ParallelTaskCollection):

    def __init__(self, min_value, max_value, step, chunk_size, **extra_args):
        """
        Like `ParallelTaskCollection`, but generate a sequence of jobs
        with a parameter varying from `min_value` to `max_value` in
        steps of `step`.  Only `chunk_size` jobs are generated at a
        time, to distribute the burden of job creation along the whole run.
        """
        self.min_value = min_value
        self.max_value = max_value
        self.step = step
        self.chunk_size = chunk_size
        self._floor = min(min_value + (chunk_size * step), max_value)
        initial = list()
        for param in range(min_value, self._floor, step):
            initial.append(self.new_task(param))
        # start with the initial chunk of jobs
        ParallelTaskCollection.__init__(self, initial, **extra_args)


    def new_task(self, param, **extra_args):
        """
        Return the `Task` corresponding to the parameter value `param`.

        This method *must* be overridden in subclasses to generate tasks.
        """
        raise NotImplementedError("Abstract method `ChunkedParameterSweep.new_task()` called - this should have been defined in a derived class.")


    # this is called at every cycle
    def update_state(self, **extra_args):
        """
        Like `ParallelTaskCollection.update_state()`,
        but also creates new tasks if less than
        `chunk_size` are running.
        """
        # XXX: proposal, reset chuck_size from self._controller.max_in_flight
        # this is the way to pass new 'max-running' value to the class
        # this creates though, a tigh coupling with 'controller' and maybe
        # limits the flexibility of the class.
        # In this way we obsolete 'chunked_size' as part of the __init__ args
        # if self._controller:
        #     gc3libs.log.info("Updating %s chunk_size from %d to %d" %
        #                      (self.__class__, self.chunk_size, self._controller.max_in_flight))
        #     self.chunk_size =  self._controller.
        # XXX: shall we als could jobs in Run.State.STOPPED ?
        num_running = len([task for task in self.tasks if
                           task.execution.state in  [ Run.State.NEW,
                                                      Run.State.SUBMITTED,
                                                      Run.State.RUNNING ]])
                                                      # Run.State.UNKNOWN ]])
        # add more jobs if we're close to the end
        # XXX: why using 2*self.chunk_size as treshold ?
        # is the idea to submit more jobs once we reach at least 50% of completion ?
        # if num_running < 2*self.chunk_size and self._floor < self.max_value:
        if 2*num_running < self.chunk_size and self._floor < self.max_value:
            # generate more tasks
            top = min(self._floor + (self.chunk_size * self.step), self.max_value)
            for param in range(self._floor, top, self.step):
                self.add(self.new_task(param, **extra_args))
            self._floor = top
            self.changed = True
        return ParallelTaskCollection.update_state(self, **extra_args)


class RetryableTask(Task):
    """
    Wrap a `Task` instance and re-submit it until a specified
    termination condition is met.

    By default, the re-submission upon failure happens iff execution
    terminated with nonzero return code; the failed task is retried up
    to `self.max_retries` times (indefinitely if `self.max_retries` is 0).

    Override the `retry` method to implement a different retryal policy.

    *Note:* The resubmission code is implemented in the
    `terminated`:meth:, so be sure to call it if you override in
    derived classes.
    """

    def __init__(self, task, max_retries=0, **extra_args):
        """
        Wrap `task` and resubmit it until `self.retry()` returns `False`.

        :param Task task: A `Task` instance that should be retried.

        :param int max_retries: Maximum number of times `task` should be
            re-submitted; use 0 for 'no limit'.
        """
        self.max_retries = max_retries
        self.retried = 0
        self.task = task
        Task.__init__(self, **extra_args)

    def __getattr__(self, name):
        """Proxy public attributes of the wrapped task."""
        if name.startswith('_'):
            raise AttributeError(
                "'%s' object has no attribute '%s'"
                % (self.__class__.__name__, name))
        return getattr(self.task, name)

    def retry(self):
        """
        Return `True` or `False`, depending on whether the failed task
        should be re-submitted or not.

        The default behavior is to retry a task iff its execution
        terminated with nonzero returncode and the maximum retry limit
        has not been reached.  If `self.max_retries` is 0, then the
        dependent task is retried indefinitely.

        Override this method in subclasses to implement a different
        policy.
        """
        if (self.task.execution.returncode != 0
            and ((self.max_retries > 0
                  and self.retried < self.max_retries)
                 or self.max_retries == 0)):
            return True
        else:
            return False

    def attach(self, controller):
        # here `Task.attach` is the invocation of the superclass'
        # `attach` method (which attaches *this* object to a controller),
        # while `self.task.attach` is the propagation of the `attach`
        # method to the wrapped task. (Same for `detach` below.)
        Task.attach(self, controller)
        self.task.attach(controller)

    def detach(self):
        # see comment in `attach` above
        Task.detach(self)
        self.task.detach()

    def fetch_output(self, *args, **extra_args):
        self.task.fetch_output(*args, **extra_args)

    def free(self, **extra_args):
        self.task.free(**extra_args)

    def kill(self, **extra_args):
        self.task.kill(**extra_args)

    def peek(self, *args, **extra_args):
        return self.task.peek(*args, **extra_args)

    def submit(self, resubmit=False, **extra_args):
        self.task.submit(**extra_args)
        # immediately update state if submission of managed task was successful;
        # otherwise this task may remain in ``NEW`` state which causes an
        # unwanted resubmission if the managing programs ends or is interrupted
        # just after the submission...
        # XXX: this is a case for a generic publish/subscribe mechanism!
        if self.task.execution.state != Run.State.NEW:
            self.execution.state = self._recompute_state()

    def _recompute_state(self):
        """
        Determine and return the state based on the current state and
        the state of the wrapped task.
        """
        own_state = self.execution.state
        task_state = self.task.execution.state
        if own_state == task_state:
            return own_state
        elif own_state == Run.State.NEW:
            if task_state == Run.State.NEW:
                return Run.State.NEW
            elif task_state in [ Run.State.SUBMITTED,
                                 Run.State.RUNNING,
                                 Run.State.STOPPED,
                                 Run.State.UNKNOWN ]:
                return task_state
            else:
                return Run.State.RUNNING
        elif own_state == Run.State.SUBMITTED:
            if task_state in [ Run.State.NEW, Run.State.SUBMITTED ]:
                return Run.State.SUBMITTED
            elif task_state in [ Run.State.RUNNING,
                                 Run.State.TERMINATING,
                                 Run.State.TERMINATED ]:
                return Run.State.RUNNING
            else:
                return task_state
        elif own_state == Run.State.RUNNING:
            if task_state in [ Run.State.STOPPED, Run.State.UNKNOWN ]:
                return task_state
            else:
                # if task is NEW, SUBMITTED, RUNNING, etc. -- keep our state
                return own_state
        elif own_state in [ Run.State.TERMINATING, Run.State.TERMINATED ]:
            assert task_state == Run.State.TERMINATED
            return Run.State.TERMINATED
        elif own_state in [ Run.State.STOPPED, Run.State.UNKNOWN ]:
            if task_state in [ Run.State.NEW,
                               Run.State.SUBMITTED,
                               Run.State.RUNNING,
                               Run.State.TERMINATING,
                               Run.State.TERMINATED ]:
                return Run.State.RUNNING
            else:
                return own_state
        else:
            # should not happen!
            raise AssertionError("Unhandled own state '%s'"
                                 " in RetryableTask._recompute_state()", own_state)

    def update_state(self):
        """
        Update the state of the dependent task, then resubmit it if it's
        TERMINATED and `self.retry()` is `True`.
        """
        own_state_old = self.execution.state
        self.task.update_state()
        own_state_new = self._recompute_state()
        if (self.task.execution.state == Run.State.TERMINATED and own_state_old != Run.State.TERMINATED):
            self.execution.returncode = self.task.execution.returncode
            if self.retry():
                self.retried += 1
                self.task.submit(resubmit=True)
                own_state_new = Run.State.RUNNING
            else:
                own_state_new = Run.State.TERMINATED
            self.changed = True
        if own_state_new != own_state_old:
            self.execution.state = own_state_new
            self.changed = True


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="dag",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
