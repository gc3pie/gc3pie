# test_engine.py
# -*- coding: utf-8 -*-
#
#  Copyright (C) 2015, 2018, 2019  University of Zurich. All rights reserved.
#
#
#  This program is free software; you can redistribute it and/or modify it
#  under the terms of the GNU General Public License as published by the
#  Free Software Foundation; either version 2 of the License, or (at your
#  option) any later version.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

# stdlib imports
from __future__ import absolute_import, print_function, unicode_literals
from future import standard_library
standard_library.install_aliases()
from builtins import range
from collections import defaultdict
from io import StringIO
import os
import shutil
import tempfile
import re

import pytest

# GC3Pie imports
from gc3libs import Run, Application, create_engine
import gc3libs.config
from gc3libs.core import Core, Engine, MatchMaker
from gc3libs.persistence.filesystem import FilesystemStore
from gc3libs.quantity import GB, GiB, hours

from gc3libs.testing.helpers import example_cfg_dict, SimpleParallelTaskCollection, SimpleSequentialTaskCollection, SuccessfulApp, temporary_config, temporary_config_file, temporary_core, temporary_directory, temporary_engine


def test_engine_resources():
    """
    Check that configured resources can be accessed through the `Engine` object.
    """
    with temporary_engine() as engine:
        resources = engine.resources
        assert len(resources) == 1
        assert 'test' in resources
        test_rsc = resources['test']
        # these should match the resource definition in `gc3libs.testing.helpers.temporary_core`
        assert test_rsc.max_cores_per_job == 123
        assert test_rsc.max_memory_per_core == 999*GB
        assert test_rsc.max_walltime == 7*hours


def test_engine_progress(num_jobs=5, transition_graph=None, max_iter=100):
    with temporary_engine() as engine:

        # generate some no-op tasks
        tasks = []
        for n in range(num_jobs):
            name = 'app{nr}'.format(nr=n+1)
            app = SuccessfulApp(name)
            engine.add(app)
            tasks.append(app)

        # run them all
        current_iter = 0
        done = engine.counts()[Run.State.TERMINATED]
        while done < num_jobs and current_iter < max_iter:
            engine.progress()
            done = engine.counts()[Run.State.TERMINATED]
            current_iter += 1

        # check state
        for task in tasks:
            assert task.execution.state == 'TERMINATED'


def test_engine_forget_terminated(num_jobs=3, transition_graph=None, max_iter=100):
    with temporary_engine() as engine:
        engine.forget_terminated = True

        # generate some no-op tasks
        tasks = []
        for n in range(num_jobs):
            name = 'app{nr}'.format(nr=n+1)
            app = SuccessfulApp(name)
            engine.add(app)
            tasks.append(app)

        # run them all
        current_iter = 0
        done = engine.counts()[Run.State.TERMINATED]
        while done < num_jobs and current_iter < max_iter:
            engine.progress()
            done = engine.counts()[Run.State.TERMINATED]
            current_iter += 1

        # check that they have been forgotten
        assert 0 == len(engine._managed.done)
        for task in tasks:
            assert task.execution.state == 'TERMINATED'
            assert not task._attached


def test_engine_progress_collection():
    with temporary_engine() as engine:
        seq = SimpleSequentialTaskCollection(3)
        engine.add(seq)

        # run through sequence
        while seq.execution.state != 'TERMINATED':
            engine.progress()
        assert seq.stage().jobname == 'stage2'
        assert seq.stage().execution.state == 'TERMINATED'


def test_engine_progress_collection_and_forget_terminated():
    with temporary_engine() as engine:
        engine.forget_terminated = True

        seq = SimpleSequentialTaskCollection(3)
        engine.add(seq)

        # run through sequence
        while seq.execution.state != 'TERMINATED':
            engine.progress()

        assert 0 == len(engine._managed.done)
        assert not seq._attached
        for task in seq.tasks:
            assert not task._attached


def test_engine_kill_SequentialTaskCollection():
    with temporary_engine() as engine:
        seq = SimpleSequentialTaskCollection(3)
        engine.add(seq)

        while seq.execution.state != 'RUNNING':
            engine.progress()

        # When the sequence is in RUNNING state, so must the first app
        assert seq.tasks[0].execution.state == 'RUNNING'
        assert seq.tasks[1].execution.state == 'NEW'
        assert seq.tasks[2].execution.state == 'NEW'

        # Killing a sequential should put all the applications in
        # TERMINATED state. However, we will need an extra run of
        # engine.progress() to update the status of all the jobs.
        engine.kill(seq)
        assert seq.tasks[0].execution.state == 'RUNNING'
        assert seq.tasks[1].execution.state == 'NEW'
        assert seq.tasks[2].execution.state == 'NEW'
        assert seq.execution.state == 'RUNNING'

        engine.progress()

        assert seq.tasks[0].execution.state == 'TERMINATED'
        assert seq.tasks[1].execution.state == 'TERMINATED'
        assert seq.tasks[2].execution.state == 'TERMINATED'
        assert seq.execution.state == 'TERMINATED'


def test_engine_kill_redo_SequentialTaskCollection():
    with temporary_engine() as engine:
        seq = SimpleSequentialTaskCollection(3)
        engine.add(seq)

        while seq.execution.state != 'RUNNING':
            engine.progress()

        # When the sequence is in RUNNING state, so must the first app
        assert seq.tasks[0].execution.state == 'RUNNING'
        assert seq.tasks[1].execution.state == 'NEW'
        assert seq.tasks[2].execution.state == 'NEW'

        # Killing a sequential should put all the applications in
        # TERMINATED state. However, we will need an extra run of
        # engine.progress() to update the status of all the jobs.
        engine.kill(seq)
        assert seq.tasks[0].execution.state == 'RUNNING'
        assert seq.tasks[1].execution.state == 'NEW'
        assert seq.tasks[2].execution.state == 'NEW'
        assert seq.execution.state == 'RUNNING'

        engine.progress()
        assert seq.tasks[0].execution.state == 'TERMINATED'
        assert seq.tasks[1].execution.state == 'TERMINATED'
        assert seq.tasks[2].execution.state == 'TERMINATED'
        assert seq.execution.state == 'TERMINATED'

        engine.redo(seq)
        assert seq.tasks[0].execution.state == 'NEW'
        assert seq.tasks[1].execution.state == 'NEW'
        assert seq.tasks[2].execution.state == 'NEW'
        assert seq.execution.state == 'NEW'

        engine.progress()
        assert seq.tasks[0].execution.state == 'SUBMITTED'
        assert seq.tasks[1].execution.state == 'NEW'
        assert seq.tasks[2].execution.state == 'NEW'


def test_engine_kill_ParallelTaskCollection():
    # Creates an engine with 2 cores.
    with temporary_engine(max_cores=2) as engine:
        par = SimpleParallelTaskCollection(3)
        engine.add(par)

        for _ in range(20):
            engine.progress()
            if par.execution.state == 'RUNNING':
                break

        # Because of our noop engine, as soon as the parallel is in
        # running we will have all jobs in SUBMITTED and the others in
        # NEW.
        assert (
            ['SUBMITTED', 'SUBMITTED', 'NEW'] ==
            [i.execution.state for i in par.tasks])

        # Killing a parallel should put all the applications in
        # TERMINATED state. However, we need two runs of
        # engine.progress() to update the status of all the jobs
        engine.kill(par)
        assert (
            ['SUBMITTED', 'SUBMITTED', 'NEW'] ==
            [i.execution.state for i in par.tasks])

        engine.progress()
        engine.progress()

        assert (
            ['TERMINATED', 'TERMINATED', 'TERMINATED'] ==
            [i.execution.state for i in par.tasks])
        assert par.execution.state == 'TERMINATED'


def test_engine_redo_SequentialTaskCollection():
    with temporary_engine() as engine:
        seq = SimpleSequentialTaskCollection(3)
        engine.add(seq)

        # run through sequence
        while seq.execution.state != 'TERMINATED':
            engine.progress()
        assert seq.stage().jobname == 'stage2'
        assert seq.stage().execution.state == 'TERMINATED'

        engine.redo(seq, 1)
        assert seq.stage().jobname == 'stage1'
        assert seq.stage().execution.state == 'NEW'

        # run through sequence again
        while seq.execution.state != 'TERMINATED':
            engine.progress()
        assert seq.stage().jobname == 'stage2'
        assert seq.stage().execution.state == 'TERMINATED'


def test_engine_redo_ParallelTaskCollection():
    with temporary_engine() as engine:
        par = SimpleParallelTaskCollection(5)
        engine.add(par)

        # run until terminated
        while par.execution.state != Run.State.TERMINATED:
            engine.progress()

        engine.redo(par)
        assert par.execution.state == Run.State.NEW
        for task in par.tasks:
            assert task.execution.state == Run.State.NEW


def test_engine_redo_Task1():
    """Test correct use of `Engine.redo()` with a `Task` instance."""
    with temporary_engine() as engine:
        task = SuccessfulApp()
        engine.add(task)

        # run until terminated
        while task.execution.state != Run.State.TERMINATED:
            engine.progress()
        assert task.execution.state == Run.State.TERMINATED

        # no do it all over again
        engine.redo(task)
        assert task.execution.state == Run.State.NEW

        engine.progress()
        assert task.execution.state != Run.State.NEW
        assert task.execution.state in [Run.State.SUBMITTED, Run.State.RUNNING]


def test_engine_redo_Task2():
    """Test that `Engine.redo()` raises if called on a Task that is not TERMINATED."""
    with temporary_engine() as engine:
        task = SuccessfulApp()
        engine.add(task)

        engine.progress()
        assert task.execution.state != Run.State.NEW

        # cannot redo a task that is not yet terminated
        with pytest.raises(AssertionError):
            task.redo()
            pytest.fail("`Task.redo()` succeeded on task not yet finished")


def test_engine_redo_Task3():
    """Test that `Engine.redo()` is a no-op if the Task is still NEW."""
    with temporary_engine() as engine:
        task = SuccessfulApp()
        engine.add(task)
        task.redo()


def test_engine_resubmit():
    with temporary_engine() as engine:
        app = SuccessfulApp()
        engine.add(app)

        # run through sequence
        while app.execution.state != 'TERMINATED':
            engine.progress()

        engine.submit(app, resubmit=True)
        assert app.execution.state == 'NEW'

        # run through sequence again
        while app.execution.state != 'TERMINATED':
            engine.progress()
        assert app.execution.state == 'TERMINATED'


def test_engine_submit1():
    """Engine.submit is equivalent to `add` if a task is not yet managed."""
    with temporary_engine() as engine:
        assert engine.counts()['NEW'] == 0

        app = SuccessfulApp()
        assert app.execution.state == 'NEW'
        engine.submit(app)
        assert app.execution.state == 'NEW'
        assert engine.counts()['NEW'] == 1

        engine.progress()
        assert app.execution.state in ['SUBMITTED', 'RUNNING']
        assert engine.counts()['NEW'] == 0
        assert engine.counts()[app.execution.state] == 1


def test_engine_submit2():
    """Engine.submit is a no-op if a task is already managed."""
    with temporary_engine() as engine:
        app = SuccessfulApp()
        engine.add(app)
        assert engine.counts()['NEW'] == 1

        engine.submit(app)
        assert engine.counts()['NEW'] == 1

        engine.progress()
        state = app.execution.state
        assert state in ['SUBMITTED', 'RUNNING']
        assert engine.counts()['NEW'] == 0
        assert engine.counts()[state] == 1

        engine.submit(app)
        assert app.execution.state == state
        assert engine.counts()[state] == 1


def test_engine_submit_to_multiple_resources(num_resources=3, num_jobs=50):
    """Test job spread across multiple resources."""
    # sanity check for parameters
    assert num_jobs > 10*(num_resources*(num_resources-1)/2), \
        "There must be enough jobs to fill the first N-1 resources"
    assert num_jobs < 10*(num_resources*(num_resources+1)/2), \
        "Too many jobs: would fill all resources"
    # set up
    cfg = gc3libs.config.Configuration()
    cfg.TYPE_CONSTRUCTOR_MAP['noop'] = ('gc3libs.backends.noop', 'NoOpLrms')
    for n in range(num_resources):
        name = 'test{nr}'.format(nr=n+1)
        cfg.resources[name].update(
            name=name,
            type='noop',
            auth='none',
            transport='local',
            max_cores_per_job=1,
            max_memory_per_core=1*GB,
            max_walltime=8*hours,
            max_cores=((n+1)*10),
            architecture=Run.Arch.X86_64,
        )
    core = Core(cfg)
    engine = Engine(core)
    # generate 50 no-op tasks
    for n in range(num_jobs):
        name = 'app{nr}'.format(nr=n)
        engine.add(SuccessfulApp(name))
    # submit them all
    engine.progress()
    # get handles to the actual backend objects
    rscs = [
        core.get_backend('test{nr}'.format(nr=n+1))
        for n in range(num_resources)
    ]
    num_jobs_per_resource = [
        len([task for task in engine._managed.to_update
             if task.execution.resource_name == rsc.name])
        for rsc in rscs
    ]
    # check that all jobs have been submitted and that each
    # resource got at least one job
    assert sum(num_jobs_per_resource) == num_jobs
    for num in num_jobs_per_resource:
        assert num > 0
    # since TYPE_CONSTRUCTOR_MAP is a class-level variable, we
    # need to clean up otherwise other tests will see the No-Op
    # backend
    del cfg.TYPE_CONSTRUCTOR_MAP['noop']


def test_create_engine_default():
    """Test `create_engine` with factory defaults."""
    with temporary_config_file() as cfgfile:
        # std factory params
        engine = create_engine(cfgfile.name)
        assert isinstance(engine, Engine)

def test_create_engine_with_cfg_dict():
    """
    Check that we can use a python dictionary in `create_engine` to configure resources.
    """
    engine = create_engine(cfg_dict=example_cfg_dict())
    resources = engine.resources
    assert len(resources) == 1
    assert 'test' in resources
    test_rsc = resources['test']
    assert test_rsc.max_cores_per_job == 4
    assert test_rsc.max_memory_per_core == 8*GiB
    assert test_rsc.max_walltime == 8*hours

def test_create_engine_non_default1():
    """Test `create_engine` with one non-default argument."""
    with temporary_config_file() as cfgfile:
        engine = create_engine(cfgfile.name, can_submit=False)
        assert engine.can_submit == False


def test_create_engine_non_default2():
    """Test `create_engine` with several non-default arguments."""
    with temporary_config_file() as cfgfile:
        engine = create_engine(cfgfile.name,
                               can_submit=False,
                               max_in_flight=1234)
        assert engine.can_submit == False
        assert engine.max_in_flight == 1234


def test_create_engine_with_core_options():
    """Test `create_engine` with a mix of Engine and Core options."""
    with temporary_config_file() as cfgfile:
        # use a specific MatchMaker instance for equality testing
        mm = MatchMaker()
        engine = create_engine(cfgfile.name,
                               can_submit=False,
                               matchmaker=mm,
                               auto_enable_auth=False)
        assert engine.can_submit == False
        assert engine._core.matchmaker == mm
        assert engine._core.auto_enable_auth == False


def test_engine_find_task_by_id():
    """
    Test that saved tasks are can be retrieved from the Engine given their ID only.
    """
    with temporary_core() as core:
        with temporary_directory() as tmpdir:
            store = FilesystemStore(tmpdir)
            engine = Engine(core, store=store)

            task = SuccessfulApp()
            store.save(task)
            engine.add(task)

            task_id = task.persistent_id
            assert engine.find_task_by_id(task_id) == task


def test_engine_cannot_find_task_by_id_if_not_saved():
    """
    Test that *unsaved* tasks are cannot be retrieved from the Engine given their ID only.
    """
    with temporary_core() as core:
        with temporary_directory() as tmpdir:
            store = FilesystemStore(tmpdir)
            engine = Engine(core, store=store)

            task = SuccessfulApp()
            engine.add(task)

            store.save(task)  # guarantee it has a `.persistent_id`
            task_id = task.persistent_id
            with pytest.raises(KeyError):
                engine.find_task_by_id(task_id)


def test_engine_cannot_find_task_by_id_if_no_store():
    """
    Test that `Engine.find_task_by_id` always raises `KeyError` if the Engine has no associated store.
    """
    with temporary_engine() as engine:
       with temporary_directory() as tmpdir:
            store = FilesystemStore(tmpdir)

            task = SuccessfulApp()
            engine.add(task)

            store.save(task)  # guarantee it has a `.persistent_id`
            task_id = task.persistent_id
            with pytest.raises(KeyError):
                engine.find_task_by_id(task_id)


@pytest.mark.parametrize("limit_submitted,limit_in_flight", [
    (2, 10),
    (10, 5),
])
def test_engine_limits(limit_submitted, limit_in_flight,
                       num_jobs=30, max_iter=100):
    """
    Test that `Engine.limit_in_flight` and `Engine.limit_submitted` are honored.
    """
    with temporary_engine(max_cores=50) as engine:
        # set limits
        engine.max_in_flight = 10
        engine.max_submitted = 2
        # populate with test apps
        apps = []
        for n in range(num_jobs):
            name = 'app{nr}'.format(nr=n)
            app = SuccessfulApp(name)
            engine.add(app)
            apps.append(app)
        # run all apps for (up to) a fixed nr of steps
        iter_nr = 0
        stats = engine.counts()
        while stats['TERMINATED'] < num_jobs and iter_nr < max_iter:
            iter_nr += 1
            engine.progress()
            stats = engine.counts()
            submitted = stats['SUBMITTED']
            assert submitted <= engine.max_submitted
            in_flight = (stats['SUBMITTED'] + stats['RUNNING'])
            assert in_flight <= engine.max_in_flight
        # catch errors in case of termination because of exceeded iter count
        assert stats["TERMINATED"] == num_jobs


@pytest.mark.parametrize("max_cores", [1, 2, 5, 12, 24])
def test_engine_counts1(max_cores, num_jobs=24, max_iter=1000):
    """
    Test that `Engine.count()` returns correct results with a plain list of tasks.
    """
    def populate(engine):
        apps = []
        for n in range(num_jobs):
            name = 'app{nr}'.format(nr=n)
            app = SuccessfulApp(name)
            apps.append(app)
            engine.add(app)
        return apps
    _test_engine_counts(populate, max_cores, num_jobs, max_iter=max_iter)

@pytest.mark.parametrize("max_cores", [1, 2, 5, 12, 24])
def test_engine_counts2(max_cores, n1=6, n2=6, max_iter=1000):
    """
    Test that `Engine.count()` returns correct results with a list of parallel collections.
    """
    def populate(engine):
        apps = []
        for n in range(n1):
            par = SimpleParallelTaskCollection(n2)
            par.jobname = 'par{nr}'.format(nr=n)
            engine.add(par)
            apps.append(par)
            for task in par.tasks:
                apps.append(task)
        return apps
    _test_engine_counts(populate, max_cores, n1 + n1*n2, max_iter=max_iter)

def _test_engine_counts(populate, max_cores, num_jobs,
                        num_new_jobs=None, max_iter=100):
    """
    Common code for the `test_engine_counts*` tests.
    """
    # make the `.progress()` call a little less deterministic
    transition_graph = {
        Run.State.SUBMITTED:   {0.8: Run.State.RUNNING},
        Run.State.RUNNING:     {0.8: Run.State.TERMINATING},
        Run.State.TERMINATING: {0.8: Run.State.TERMINATED},
    }
    with temporary_engine(transition_graph, max_cores=max_cores) as engine:
        # populate with test apps
        apps = populate(engine)

        # initial check on stats
        actual_counts = engine.counts()
        if num_new_jobs is None:
            num_new_jobs = num_jobs
        assert actual_counts['NEW'] == num_new_jobs

        # check
        iter_nr = 0
        while (actual_counts['TERMINATED'] < num_jobs
               and iter_nr < max_iter):
            iter_nr += 1
            engine.progress()
            actual_counts = engine.counts()
            expected_counts = _compute_counts(apps)
            _check_counts(actual_counts, expected_counts)

def _compute_counts(apps):
    """
    Helper method for `test_*_counts`.

    Explicitly compute the job/state counts by running through the
    entire list of jobs.
    """
    result = defaultdict(int)
    for app in apps:
        result['total'] += 1
        state = app.execution.state
        result[state] += 1
        if state == 'TERMINATED':
            if app.execution.returncode == 0:
                result['ok'] += 1
            else:
                result['failed'] += 1
    return result

def _check_counts(actual, expected):
    """
    Common code for `test_*_counts`.

    Check that `actual` is internally consistent and that it agrees
    with `expected` on every task state.
    """
    total = expected['total']
    # internal consistency checks
    assert actual['total'] == total
    assert total == sum([
        actual['NEW'],
        actual['SUBMITTED'],
        actual['RUNNING'],
        actual['TERMINATING'],
        actual['TERMINATED'],
    ])
    assert actual['TERMINATED'] == (
        actual['ok'] + actual['failed'])
    # compare with explicit counting
    for state in [
            'NEW', 'SUBMITTED', 'RUNNING', 'TERMINATING',
            'TERMINATED', 'ok', 'failed',
    ]:
        # need to make our own error message, otherwise
        # `pytest` just prints something like `assert 0 == 2`
        try:
            assert actual[state] == expected[state]
        except AssertionError:
            raise AssertionError(
                "Actual count for `{0}` is {1} but expected {2}"
                .format(state, actual[state], expected[state]))


if __name__ == "__main__":
    import pytest
    pytest.main(["-v", __file__])
