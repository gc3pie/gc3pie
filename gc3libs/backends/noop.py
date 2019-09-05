#! /usr/bin/env python

"""
Fake running applications, only useful for testing.
"""
# Copyright (C) 2009-2019  University of Zurich. All rights reserved.
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import absolute_import, print_function, unicode_literals
__docformat__ = 'reStructuredText'


# stdlib imports
from random import random

# GC3Pie imports
import gc3libs
import gc3libs.exceptions
from gc3libs import log, Run
from gc3libs.utils import same_docstring_as
from gc3libs.backends import LRMS
from gc3libs.quantity import Memory


NORMAL_TRANSITION_GRAPH = {
    Run.State.SUBMITTED:   {1.0: Run.State.RUNNING},
    Run.State.RUNNING:     {1.0: Run.State.TERMINATING},
    Run.State.TERMINATING: {1.0: Run.State.TERMINATED},
}


class NoOpLrms(LRMS):
    """
    Simulate execution of an `Application`:class instance.

    Upon every invocation of `update_job_state`:meth: the application
    status is advanced to the next state (according to the normal
    progression SUBMITTED -> RUNNING -> TERMINATING).

    This progression can be altered by assigning a different
    transition graph to attribute `transition_graph` on an instance.
    The transition graph has a two-level structure:

    * keys are task execution states (e.g., `Run.State.SUBMITTED`)

    * values are dictionaries, mapping a probability (i.e., a floating
      point number between 0.0 and 1.0) to a new state.  All
      probabilities should sum to a number less then, or equal to, 1.0
      -- but this condition is not checked or enforced.  Likewise, it
      is not checked nor enforced that the new state is a valid target
      state given the source.

    Every invocation of :meth:`update_job_state` results in the task
    execution state possibly changing to one of the target states,
    according to the given transition probabilities.

    For example, the following transition graph specifies that a job
    in state ``SUBMITTED`` can change to ``RUNNING`` with 80%
    probability (and with 20% stay in ``SUBMITTED`` state); a job in
    state ``RUNNING`` has a 50% chance of transitioning to
    ``TERMINATING``, 10% chance of being ``STOPPED`` and 40% chance of
    staying in state ``RUNNING``; and a job in ``STOPPED`` state stays
    in ``STOPPED`` state forever::

    | transition_graph = {
    |   Run.State.SUBMITTED = {
    |     0.80: Run.State.RUNNING,
    |   },
    |   Run.State.RUNNING = {
    |     0.50: Run.State.TERMINATING,
    |     0.10: Run.State.STOPPED,
    |     0.40: Run.State.RUNNING, # implcit, could be omitted
    |   },
    |   Run.State.STOPPED = {
    |     1.00: Run.State.STOPPED,
    |   },
    | }

    All parameters taken by the base class `LRMS`:class: are
    understood by this class constructor, but they are actually
    ignored.
    """

    def __init__(self, name,
                 # these parameters are inherited from the `LRMS` class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime,
                 auth=None,
                 **extra_args):

        # init base class
        LRMS.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth, **extra_args)

        self.transition_graph = NORMAL_TRANSITION_GRAPH

        # initial resource status;
        # use `max_cores` as the max number of processes to allow
        self.user_queued = 0
        self.free_slots = self.max_cores
        self.queued = 0
        self.user_run = 0
        self.available_memory = self.max_cores * self.max_memory_per_core

    @same_docstring_as(LRMS.cancel_job)
    def cancel_job(self, app):
        """
        Simulate stopping a job's process.

        Effectively, this just returns any resources accounted to the
        job into the free pool.
        """
        if app.execution.state == Run.State.RUNNING:
            self.user_run -= 1
        elif app.execution.state == Run.State.SUBMITTED:
            self.user_queued -= 1
            self.queued -= 1
        self.free_slots += app.requested_cores
        if app.requested_memory:
            self.available_memory += app.requested_memory

    def close(self):
        """This is a no-op for this backend."""
        pass

    def free(self, app):
        """This is a no-op for this backend."""
        pass

    @same_docstring_as(LRMS.get_resource_status)
    def get_resource_status(self):
        # if we have been doing our own book-keeping well, then
        # there's no resource status to update
        self.updated = True
        return self

    @same_docstring_as(LRMS.get_results)
    def get_results(self, app, download_dir,
                    overwrite=False, changed_only=True):
        if app.outputs:
            raise gc3libs.exceptions.DataStagingError(
                "Retrieval of output files is not supported"
                " in the NoOp backend.")
        return

    def update_job_state(self, app):
        """
        Advance `app`'s status to the next one
        in the normal execution graph.
        """
        log.debug("No-Op backend updating state of Task %s ...", app)
        transitions = self.transition_graph[app.execution.state]
        log.debug("Task %s transitions: %s.", app, ", ".join([
            ("with probability %g to state %s" % (prob, state))
            for prob, state in list(transitions.items()) if prob > 0
        ]))
        dice = random()
        # log.debug("Rolled dice, got %g result", dice)
        for prob, to_state in sorted(transitions.items()):
            if dice < prob:
                log.debug(
                    "Task %s transitions to state '%s'", app, to_state)
                # update resource state based on old and new app state
                if app.execution.state == Run.State.SUBMITTED:
                    self.queued -= 1
                    self.user_queued -= 1
                if app.execution.state == Run.State.RUNNING:
                    self.user_run -= 1
                if to_state == Run.State.RUNNING:
                    self.user_run += 1
                if to_state == Run.State.TERMINATING:
                    self.free_slots += app.requested_cores
                    if app.requested_memory:
                        self.available_memory += app.requested_memory
                # set the new app state
                app.execution.state = to_state
                break
            else:
                dice -= prob
        return app.execution.state

    def submit_job(self, app):
        """
        Transition `app`'s status to `Run.State.SUBMITTED` if possible.

        Note that this method still checks that `app`'s requirements
        are compatible with what this resource was instanciated with,
        and that conversely the resource still has enough free
        cores/memory/etc to host a new application.  So, submission to
        a No-Op resource may still fail!
        """
        if app.requested_cores > self.free_slots:
            raise gc3libs.exceptions.MaximumCapacityReached(
                "Resource %s does not have enough free cores:"
                " %s requested, but %s available."
                " Increase 'max_cores' to raise."
                % (self.name, app.requested_cores, self.free_slots))

        if (app.requested_memory and
                 self.available_memory < app.requested_memory):
            raise gc3libs.exceptions.MaximumCapacityReached(
                "Resource %s does not have enough available memory:"
                " %s requested, but only %s available."
                % (self.name,
                   app.requested_memory.to_str('%g%s', unit=Memory.MB),
                   app.available_memory.to_str('%g%s', unit=Memory.MB),)
            )

        log.debug("Faking execution of command '%s' ...",
                  " ".join(app.arguments))

        # Update application and current resources
        app.execution.lrms_jobid = id(app)
        self.free_slots -= app.requested_cores
        if app.requested_memory:
            self.available_memory -= app.requested_memory
        self.queued += 1
        self.user_queued += 1

        return app

    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        """Not supported on this backend."""
        raise NotImplementedError(
            "The `peek` operation is not supported"
            " by the `NoOp` backend.")

    def validate_data(self, data_file_list=[]):
        """
        Return `False` if any of the URLs in `data_file_list` cannot
        be handled by this backend.

        The `noop`:mod: backend can not do *any* kind of I/O, so this
        method will only return `True` if the supplied list of files
        is empty.
        """
        for url in data_file_list:
            if url.scheme:
                return False
        return True


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
