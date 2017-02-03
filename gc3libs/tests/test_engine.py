# test_engine.py
# -*- coding: utf-8 -*-
#
#  Copyright (C) 2015 S3IT, Zentrale Informatik, University of Zurich
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
from cStringIO import StringIO
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
from gc3libs.quantity import GB, hours

from gc3libs.testing.helpers import SimpleParallelTaskCollection, SimpleSequentialTaskCollection, SuccessfulApp, temporary_config, temporary_config_file, temporary_core, temporary_directory, temporary_engine


def test_engine_progress(num_jobs=1, transition_graph=None, max_iter=100):
    with temporary_engine() as engine:

        # generate some no-op tasks
        for n in range(num_jobs):
            name = 'app{nr}'.format(nr=n+1)
            engine.add(SuccessfulApp(name))

        # run them all
        current_iter = 0
        done = engine.stats()[Run.State.TERMINATED]
        while done < num_jobs and current_iter < max_iter:
            engine.progress()
            done = engine.stats()[Run.State.TERMINATED]
            current_iter += 1


def test_engine_progress_collection():
    with temporary_engine() as engine:
        seq = SimpleSequentialTaskCollection(3)
        engine.add(seq)

        # run through sequence
        while seq.execution.state != 'TERMINATED':
            engine.progress()
        assert seq.stage().jobname == 'stage2'
        assert seq.stage().execution.state == 'TERMINATED'


def test_engine_kill_SequentialTaskCollection():
    with temporary_engine() as engine:
        seq = SimpleSequentialTaskCollection(3)
        engine.add(seq)

        while seq.execution.state != 'RUNNING':
            engine.progress()

        # Because of our noop engine, as soon as the sequential is in
        # running we will have a job in TERMINATED and the others in
        # NEW.
        assert (
            ['TERMINATED', 'NEW', 'NEW'] ==
            [i.execution.state for i in seq.tasks])

        # Killing a sequential should put all the applications in
        # TERMINATED state. However, we will need an extra run of
        # engine.progress() to update the status of all the jobs.
        engine.kill(seq)
        assert (
            ['TERMINATED', 'NEW', 'NEW'] ==
            [i.execution.state for i in seq.tasks])

        engine.progress()

        assert (
            ['TERMINATED', 'TERMINATED', 'TERMINATED'] ==
            [i.execution.state for i in seq.tasks])
        assert seq.execution.state == 'TERMINATED'


def test_engine_kill_redo_SequentialTaskCollection():
    with temporary_engine() as engine:
        seq = SimpleSequentialTaskCollection(3)
        engine.add(seq)

        while seq.execution.state != 'RUNNING':
            engine.progress()

        # Because of our noop engine, as soon as the sequential is in
        # running we will have a job in TERMINATED and the others in
        # NEW.
        assert (
            ['TERMINATED', 'NEW', 'NEW'] ==
            [i.execution.state for i in seq.tasks])

        # Killing a sequential should put all the applications in
        # TERMINATED state. However, we will need an extra run of
        # engine.progress() to update the status of all the jobs.
        engine.kill(seq)
        assert (
            ['TERMINATED', 'NEW', 'NEW'] ==
            [i.execution.state for i in seq.tasks])

        engine.progress()

        assert (
            ['TERMINATED', 'TERMINATED', 'TERMINATED'] ==
            [i.execution.state for i in seq.tasks])
        assert seq.execution.state == 'TERMINATED'

        engine.redo(seq)
        assert (
            ['NEW', 'NEW', 'NEW'] ==
            [i.execution.state for i in seq.tasks])
        assert seq.execution.state == 'NEW'
        engine.progress()
        assert (
            ['SUBMITTED', 'NEW', 'NEW'] ==
            [i.execution.state for i in seq.tasks])

def test_engine_kill_ParallelTaskCollection():
    # Creates an engine with 2 cores.
    with temporary_engine(max_cores=2) as engine:
        par = SimpleParallelTaskCollection(3)
        engine.add(par)

        while par.execution.state != 'RUNNING':
            engine.progress()

        # Because of our noop engine, as soon as the parallel is in
        # running we will have all jobs in SUBMITTED and the others in
        # NEW.
        assert (
            ['TERMINATED', 'SUBMITTED', 'NEW'] ==
            [i.execution.state for i in par.tasks])

        # Killing a parallel should put all the applications in
        # TERMINATED state. However, we need a run of
        # engine.progress() to update the status of all the jobs
        engine.kill(par)

        assert (
            ['TERMINATED', 'SUBMITTED', 'NEW'] ==
            [i.execution.state for i in par.tasks])
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
        with pytest.raises(AssertionError,
                           message="`Task.redo()` succeeded on task not yet finished"):
            task.redo()


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
        assert engine.stats()['NEW'] == 0

        app = SuccessfulApp()
        assert app.execution.state == 'NEW'
        engine.submit(app)
        assert app.execution.state == 'NEW'
        assert engine.stats()['NEW'] == 1

        engine.progress()
        assert app.execution.state in ['SUBMITTED', 'RUNNING']
        assert engine.stats()['NEW'] == 0
        assert engine.stats()[app.execution.state] == 1


def test_engine_submit2():
    """Engine.submit is a no-op if a task is already managed."""
    with temporary_engine() as engine:
        app = SuccessfulApp()
        engine.add(app)
        assert engine.stats()['NEW'] == 1

        engine.submit(app)
        assert engine.stats()['NEW'] == 1

        engine.progress()
        state = app.execution.state
        assert state in ['SUBMITTED', 'RUNNING']
        assert engine.stats()['NEW'] == 0
        assert engine.stats()[state] == 1

        engine.submit(app)
        assert app.execution.state == state
        assert engine.stats()[state] == 1


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
        len([task for task in engine._in_flight
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
            assert_equal(task, engine.find_task_by_id(task_id))


@raises(KeyError)
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
            engine.find_task_by_id(task_id)


@raises(KeyError)
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
            engine.find_task_by_id(task_id)


if __name__ == "__main__":
    import pytest
    pytest.main(["-v", __file__])
