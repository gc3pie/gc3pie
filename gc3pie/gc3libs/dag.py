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
# Copyright (C) 2009-2011 GC3, University of Zurich. All rights reserved.
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

    def submit(self, **kw):
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


    def stats(self):
        """
        Return a dictionary mapping each state name into the count of
        jobs in that state. In addition, the following keys are defined:
        
        * `ok`:  count of TERMINATED jobs with return code 0
        
        * `failed`: count of TERMINATED jobs with nonzero return code
        """
        result = gc3libs.utils.defaultdict(lambda: 0)
        for task in self.tasks:
            state = task.execution.state
            result[state] += 1
            if state == Run.State.TERMINATED:
                if task.execution.returncode == 0:
                    result['ok'] += 1
                else:
                    result['failed'] += 1
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

    def __init__(self, jobname, tasks, grid=None):
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

 
    def next(self, index):
        """
        Called by :meth:`progress` when a job is finished; if
        `Run.State.TERMINATED` is returned then no other jobs will be
        run; otherwise, the return value is assigned to
        `execution.state` and the next job in the `self.tasks` list is
        executed.

        The default implmentation runs tasks in the order they were
        given to the constructor, and sets the state to TERMINATED
        when all tasks have been run.  This method can (and should) be
        overridden in derived classes to implement policies for serial
        job execution.
        """
        if index == len(self.tasks) - 1:
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
        state = self.next(self._current_task)
        if state == Run.State.TERMINATED:
            self.execution.state = Run.State.TERMINATED
            self._current_task = None
        else:
            self.execution.state = state
            self._current_task += 1


    def submit(self, **kw):
        """
        Start the current task in the collection.
        """
        if self._current_task is None:
            self._current_task = 0
        task = self.tasks[self._current_task]
        task.submit(**kw)
        if task.execution.state == Run.State.NEW:
            # submission failed, state unchanged
            self.execution.state = Run.State.NEW
        elif task.execution.state == Run.State.SUBMITTED:
            self.execution.state = Run.State.SUBMITTED
        else:
            self.execution.state = Run.State.RUNNING


    def update_state(self, **kw):
        """
        Update state of the collection, based on the jobs' statuses.
        """
        gc3libs.log.debug("Updating state of task %d in collection %s ..."
                          % (self._current_task, self))
        if self._current_task is None:
            # it's either NEW or TERMINATED, no update
            assert self.execution.state in [ Run.State.NEW, Run.State.TERMINATED ]
            pass
        else:
            task = self.tasks[self._current_task]
            task.update_state(**kw)
            gc3libs.log.debug("Task #%d in state %s"
                              % (self._current_task, task.execution.state))
        if self._current_task == 0 and task.execution.state in [ Run.State.NEW, Run.State.SUBMITTED ]:
            self.execution.state = task.execution.state
        elif (task.execution.state == Run.State.TERMINATED
              and self._current_task == len(self.tasks)-1):
            self.execution.state = self.next(self._current_task)
            if self.execution.state not in [ Run.State.STOPPED,
                                             Run.State.TERMINATED ]:
                self._current_task += 1
        else:
            self.execution.state = Run.State.RUNNING
        return self.execution.state
        


class ParallelTaskCollection(TaskCollection):
    """
    A `ParallelTaskCollection` runs all of its tasks concurrently.

    The collection state is set to `TERMINATED` once all tasks have
    reached the same terminal status.
    """

    def __init__(self, jobname, tasks=None, grid=None):
        TaskCollection.__init__(self, jobname, tasks, grid)

        
    def _state(self):
        """
        Return the state of the collection.

        For a `ParallelTaskCollection`, the state of dependent jobs is
        computed by looping across the states NEW, SUBMITTED, RUNNING,
        STOPPED, TERMINATED, UNKNOWN in the order given: the first
        state for which there is at least one job in that state is
        returned as the global collection state.
        """
        stats = self.stats()
        for state in [ Run.State.NEW,
                       Run.State.SUBMITTED,
                       Run.State.RUNNING,
                       Run.State.STOPPED,
                       Run.State.TERMINATED,
                       Run.State.UNKNOWN,
                       ]:
            if stats[state] > 0:
                return state

    
    def kill(self, **kw):
        """
        Terminate all tasks in the collection, and set collection
        state to `TERMINATED`.
        """
        for task in self.tasks:
            task.kill(**kw)
        self.execution.state = TERMINATED
        self.execution.returncode = (Run.Signals.Cancelled, -1)


    def progress(self):
        """
        Try to advance all jobs in the collection to the next state in
        a normal lifecycle.  Return list of task execution states.
        """
        return [ task.progress() for task in self.tasks ]


    def submit(self, **kw):
        """
        Start all tasks in the collection.
        """
        for task in self.tasks:
            task.submit(**kw)
        self.execution.state = self._state()

        
    def update_state(self, **kw):
        """
        Update state of all tasks in the collection.
        """
        for task in self.tasks:
            gc3libs.log.debug("Updating state of %s in collection %s ..."
                              % (task, self))
            task.update_state(**kw)
        self.execution.state = self._state()
        

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="dag",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
