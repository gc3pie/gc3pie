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
__version__ = 'development version (SVN $Revision$)'


import time

from gc3libs.compat.collections import defaultdict

from gc3libs import log, Run, Task
import gc3libs.exceptions
import gc3libs.utils


class TaskCollection(Task, gc3libs.utils.Struct):
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

    def __init__(self, jobname, tasks=None, grid=None):
        if tasks is None:
            self.tasks = [ ]
        else:
            self.tasks = tasks
        for task in self.tasks:
            if grid is not None:
                task.attach(grid)
            else:
                task.detach()
        Task.__init__(self, jobname, grid)

    # manipulate the "grid" interface used to control the associated task
    def attach(self, grid):
        """
        Use the given Grid interface for operations on the job
        associated with this task.
        """
        for task in self.tasks:
            task.attach(grid)
        Task.attach(self, grid)


    def detach(self):
        for task in self.tasks:
            task.detach()
        Task.detach(self)


    def add(self, task):
        """
        Add a task to the collection.
        """
        task.detach()
        self.tasks.append(task)
        if self._attached:
            task.attach(self._grid)

    def remove(self, task):
        """
        Remove a task from the collection.
        """
        self.tasks.remove(task)
        task.detach()


    # task execution manipulation -- these methods should be overriden
    # in derived classes, to implement the desired policy.

    def submit(self, resubmit=False, **kw):
        raise NotImplementedError("Called abstract method TaskCollection.submit() - this should be overridden in derived classes.")


    def update_state(self, **kw):
        """
        Update the running state of all managed tasks.
        """
        for task in self.tasks:
            self._grid.update_job_state(task, **kw)


    def kill(self, **kw):
        # XXX: provide default implementation that kills all jobs?
        raise NotImplementedError("Called abstract method TaskCollection.kill() - this should be overridden in derived classes.")


    def fetch_output(self, output_dir=None, overwrite=False, **kw):
        # if `output_dir` is not None, it is interpreted as the base
        # directory where to download files; each task will get its
        # own subdir based on its `.persistent_id`
        for task in self.tasks:
            if output_dir is not None:
                self._grid.fetch_output(
                    task,
                    os.path.join(output_dir, task.permanent_id),
                    overwrite,
                    **kw)


    def peek(self, what, offset=0, size=None, **kw):
        """
        Raise a `gc3libs.exceptions.InvalidOperation` error, as there
        is no meaningful semantics that can be defined for `peek` into
        a generic collection of tasks.
        """
        # is there any sensible semantic here?
        raise gc3libs.exceptions.InvalidOperation("Cannot `peek()` on a task collection.")

    def progress(self):
        raise NotImplementedError("Called abstract method TaskCollection.progress() - this should be overridden in derived classes.")


    def wait(self, interval=60):
        """
        Block until execution state reaches `TERMINATED`, then return
        a list of return codes.  Note that this does not automatically
        fetch the output.

        :param integer interval: Poll job state every this number of seconds
        """
        # FIXME: I'm not sure how to deal with this... Ideally, this
        # call should suspend the current thread and wait for
        # notifications from the Engine, but:
        #  - there's no way to tell if we are running threaded,
        #  - `self.grid` could be a `Core` instance, thus not capable
        #    of running independently.
        # For now this is a busy-wait loop, but we certainly need to revise this.
        while True:
            self.progress()
            if self.execution.state == Run.State.TERMINATED:
                return [ task.execution.returncode
                         for task in self.tasks ]
            time.sleep(interval)


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

    def __init__(self, jobname, tasks, grid=None, **kw):
        # XXX: check that `tasks` is a sequence type
        TaskCollection.__init__(self, jobname, tasks, grid)
        self._current_task = 0


    def kill(self, **kw):
        """
        Stop execution of this sequence.  Kill currently-running task
        (if any), then set collection state to TERMINATED.
        """
        if self._current_task is not None:
            self.tasks[self._current_task].kill(**kw)
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


    def progress(self):
        """
        Sequentially advance tasks in the collection through all steps
        of a regular lifecycle.  When the last task transitions to
        TERMINATED state, the collection's state is set to TERMINATED
        as well and this method becomes a no-op.  If during execution,
        any of the managed jobs gets into state `STOPPED` or
        `UNKNOWN`, then an exception `Task.UnexpectedExecutionState`
        is raised.
        """
        if execution.state == Run.State.TERMINATED:
            return
        if self._current_task is None:
            # (re)submit initial task
            self._current_task = 0
        task = self.tasks[self._current_task]
        task.progress()
        if task.execution.state == Run.State.SUBMITTED and self._current_task == 0:
            self.execution.state = Run.State.SUBMITTED
        elif task.execution.state == Run.State.TERMINATED:
            self._next_step()
        else:
            self.execution.state = Run.State.RUNNING

    def _next_step(self):
        """
        Book-keeping for advancement through the task list.
        Call :meth:`next` and set `execution.state` based on its
        return value.  Also, advance `self._current_task` if not at end
        of the list.
        """
        nxt = self.next(self._current_task)
        if nxt == Run.State.TERMINATED:
            # set returncode when all tasks are terminated
            self.execution.returncode = 0 # optimistic start...
            # ...but override if something has gone wrong
            for task in self.tasks:
                if task.execution.returncode != 0:
                    self.execution.exitcode = 1
                    break
            # returncode can be overridden in the `terminated()` hook
            self.execution.state = Run.State.TERMINATED
            self._current_task = None
        elif nxt in Run.State:
            self.execution.state = nxt
            self._current_task += 1
        else:
            # `nxt` must be a valid index into `self.tasks`
            self._current_task = nxt
            self.submit(resubmit=True)
        self.changed = True


    def submit(self, resubmit=False, **kw):
        """
        Start the current task in the collection.
        """
        if self._current_task is None:
            self._current_task = 0
        task = self.tasks[self._current_task]
        task.submit(resubmit, **kw)
        if task.execution.state == Run.State.NEW:
            # submission failed, state unchanged
            self.execution.state = Run.State.NEW
        elif task.execution.state == Run.State.SUBMITTED:
            self.execution.state = Run.State.SUBMITTED
        else:
            self.execution.state = Run.State.RUNNING
        self.changed = True
        return self.execution.state


    def update_state(self, **kw):
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
            task.update_state(**kw)
            gc3libs.log.debug("Task #%d in state %s"
                             % (self._current_task, task.execution.state))
        # set state based on the state of current task
        if self._current_task == 0 and task.execution.state in [ Run.State.NEW, Run.State.SUBMITTED ]:
            self.execution.state = task.execution.state
        elif (task.execution.state == Run.State.TERMINATED
              and self._current_task == len(self.tasks)-1):
            nxt = self.next(self._current_task)
            if nxt in Run.State:
                self.execution.state = nxt
                if self.execution.state not in [ Run.State.STOPPED,
                                                 Run.State.TERMINATED ]:
                    self._current_task += 1
                    self.changed = True
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
    def __init__(self, jobname, grid=None, **kw):
        try:
            first_stage = self.stage0()
            if isinstance(first_stage, Task):
                # init parent class with the initial task
                SequentialTaskCollection.__init__(self, jobname, [first_stage], grid, **kw)
            elif isinstance(first_stage, (int, long, tuple)):
                # init parent class with no tasks, an dimmediately set the exitcode
                SequentialTaskCollection.__init__(self, jobname, [], grid, **kw)
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

    def __init__(self, jobname, tasks=None, grid=None, **kw):
        TaskCollection.__init__(self, jobname, tasks, grid)


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


    def kill(self, **kw):
        """
        Terminate all tasks in the collection, and set collection
        state to `TERMINATED`.
        """
        for task in self.tasks:
            task.kill(**kw)
        self.execution.state = Run.State.TERMINATED
        self.execution.returncode = (Run.Signals.Cancelled, -1)
        self.changed = True


    def progress(self):
        """
        Try to advance all jobs in the collection to the next state in
        a normal lifecycle.  Return list of task execution states.
        """
        return [ task.progress() for task in self.tasks ]


    def submit(self, resubmit=False, **kw):
        """
        Start all tasks in the collection.
        """
        for task in self.tasks:
            task.submit(resubmit, **kw)
        self.execution.state = self._state()


    def update_state(self, **kw):
        """
        Update state of all tasks in the collection.
        """
        for task in self.tasks:
            #gc3libs.log.debug("Updating state of %s in collection %s ..."
            #                  % (task, self))
            task.update_state(**kw)
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

    def __init__(self, jobname, min_value, max_value, step, chunk_size, grid=None, **kw):
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
        initial = [ self.new_task(param) for param in
                    range(min_value, self._floor, step) ]
        # start with the initial chunk of jobs
        ParallelTaskCollection.__init__(self,jobname, initial, grid, **kw)


    def new_task(self, param, **kw):
        """
        Return the `Task` corresponding to the parameter value `param`.

        This method *must* be overridden in subclasses to generate tasks.
        """
        raise NotImplementedError("Abstract method `ChunkedParameterSweep.new_task()` called - this should have been defined in a derived class.")


    # this is called at every cycle
    def update_state(self, **kw):
        """
        Like `ParallelTaskCollection.update_state()`,
        but also creates new tasks if less than
        `chunk_size` are running.
        """

        # XXX: proposal, reset chuck_size from self._grid.max_in_flight
        # this is the way to pass new 'max-running' value to the class
        # this creates though, a tigh coupling with 'grid' and maybe
        # limits the flexibility of the class.
        # In this way we obsolete 'chunked_size' as part of the __init__ args
        # if self._grid:
        #     gc3libs.log.info("Updating %s chunk_size from %d to %d" %
        #                      (self.__class__, self.chunk_size, self._grid.max_in_flight))
        #     self.chunk_size =  self._grid.
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
                self.add(self.new_task(param, **kw))
            self._floor = top
            self.changed = True
        return ParallelTaskCollection.update_state(self, **kw)

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="dag",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
