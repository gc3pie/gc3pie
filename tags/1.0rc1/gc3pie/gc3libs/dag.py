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
__version__ = '1.0rc1 (SVN $Revision$)'


from gc3libs import log, Run, Task
import gc3libs.exceptions


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
    
    def __init__(self, tasks=None, grid=None):
        Task.__init__(grid)
        if tasks is None:
            self._tasks = []
        for task in self._tasks:
            task.attach(self._grid)

    # manipulate the "grid" interface used to control the associated task
    def attach(self, grid):
        """
        Use the given Grid interface for operations on the job
        associated with this task.
        """
        self._grid = grid
        self._attached = True


    def add(self, task):
        """
        Add a task to the collection.
        """
        task.detach()
        self._tasks.append(task)
        if self._attached:
            task.attach(self._grid)

    def remove(self, task):
        """
        Remove a task from the collection.
        """
        self._tasks.remove(task)
        task.detach()


    # task execution manipulation -- these methods should be overriden
    # in derived classes, to implement the desired policy.

    def submit(self):
        raise NotImplementedError("Called abstract method TaskCollection.submit() - this should be overridden in derived classes.")

    def update(self):
        """
        Update the running state of all managed tasks.
        """
        for task in self._tasks:
            self._grid.update_job_state(task)

    def kill(self):
        # XXX: provide default implementation that kills all jobs?
        raise NotImplementedError("Called abstract method TaskCollection.kill() - this should be overridden in derived classes.")

    def fetch_output(self, output_dir=None, overwrite=False):
        # if `output_dir` is not None, it is interpreted as the base
        # directory where to download files; each task will get its
        # own subdir based on its `.persistent_id`
        for task in self._tasks:
            if output_dir is not None:
                self._grid.fetch_output(task, 
                                        os.path.join(output_dir, task.permanent_id),
                                        overwrite)
    def peek(self, what, offset=0, size=None):
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
                         for task in self._tasks ]
            time.sleep(interval)



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

    def __init__(self, tasks, grid=None):
        # XXX: check that `tasks` is a sequence type
        TaskCollection.__int__(tasks, grid)
        self._next_task = 0


    def next(self, index):
        """
        Called when a job is finished; if `Run.State.TERMINATED` is
        returned, then no other jobs will be run; otherwise, the
        return value is assigned to `execution.state` and the next job
        in the `self.tasks` list is executed.

        The default implmentation runs the tasks in the order they
        were given to the constructor, and sets the state to
        terminated when all tasks have been run.  This method can (and
        should) be overridden in derived classes to implement policies
        for serial job execution.
        """
        if index == len(self.tasks) - 1:
            return Run.State.TERMINATED
        else:
            return Run.State.RUNNING
    

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


    def submit(self):
        if self.tasks[self._current_task] == Run.State.TERMINATED:
            # submit next task
            self._next_step()
            self.tasks[self._current_task].submit()
        else:
            # ignore
            pass


    def kill(self):
        """
        Stop execution of this sequence.  Kill currently-running task
        (if any), then set collection state to TERMINATED.
        """
        if self._current_task is not None:
            self.tasks[self._current_task].kill()
        self.execution.state = Run.State.TERMINATED
        self.execution.returncode = (Run.Signals.Cancelled, -1)


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
        task = self.tasks[self._current_task]
        task.progress()
        if task.execution.state == Run.State.TERMINATED:
            self._next_step()



class ParallelTaskCollection(TaskCollection):
    """
    A `ParallelTaskCollection` runs all of its tasks concurrently.

    The collection state is set to `TERMINATED` once all tasks have
    reached the same terminal status.
    """
    
    def submit(self):
        """
        Start all tasks in the collection.
        """
        for task in self._tasks:
            self._grid.submit(task)
        for task in self.tasks:
            if task.execution.state in [ Run.State.SUBMITTED, Run.State.RUNNING ]:
                self.execution.state = Run.State.RUNNING
                return
        self.execution.state = Run.State.NEW

    def kill(self):
        """
        Terminate all tasks in the collection, and set collection
        state to `TERMINATED`.
        """
        for task in self._tasks:
            self._grid.kill(task)
        self.execution.state = TERMINATED
        self.execution.returncode = (Run.Signals.Cancelled, -1)

    def progress(self):
        """
        Try to advance all jobs in the collection to the next state in
        a normal lifecycle.  Return list of task execution states.
        """
        return [ task.progress() for task in self._tasks ]
        


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="dag",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
