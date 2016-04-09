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

# nose
from nose.tools import raises, assert_equal
try:
    from nose.tools import assert_is_instance
except ImportError:
    # Python 2.6 does not support assert_is_instance()
    def assert_is_instance(obj, cls):
        assert (isinstance(obj, cls))

# GC3Pie imports
from gc3libs import Run, Application, create_engine
import gc3libs.config
from gc3libs.core import Core, Engine, MatchMaker
from gc3libs.quantity import GB, hours

from helpers import SimpleSequentialTaskCollection, SuccessfulApp, temporary_config, temporary_engine


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


def test_engine_redo():
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
    assert_equal(sum(num_jobs_per_resource), num_jobs)
    for num in num_jobs_per_resource:
        assert num > 0
    # since TYPE_CONSTRUCTOR_MAP is a class-level variable, we
    # need to clean up otherwise other tests will see the No-Op
    # backend
    del cfg.TYPE_CONSTRUCTOR_MAP['noop']


def test_create_engine_default():
    """Test `create_engine` with factory defaults."""
    with temporary_config() as cfgfile:
        # std factory params
        engine = create_engine(cfgfile.name)
        assert_is_instance(engine, Engine)


def test_create_engine_non_default1():
    """Test `create_engine` with one non-default argument."""
    with temporary_config() as cfgfile:
        engine = create_engine(cfgfile.name, can_submit=False)
        assert_equal(engine.can_submit, False)


def test_create_engine_non_default2():
    """Test `create_engine` with several non-default arguments."""
    with temporary_config() as cfgfile:
        engine = create_engine(cfgfile.name,
                               can_submit=False,
                               max_in_flight=1234)
        assert_equal(engine.can_submit, False)
        assert_equal(engine.max_in_flight, 1234)


def test_create_engine_with_core_options():
    """Test `create_engine` with a mix of Engine and Core options."""
    with temporary_config() as cfgfile:
        # use a specific MatchMaker instance for equality testing
        mm = MatchMaker()
        engine = create_engine(cfgfile.name,
                               can_submit=False,
                               matchmaker=mm,
                               auto_enable_auth=False)
        assert_equal(engine.can_submit, False)
        assert_equal(engine._core.matchmaker, mm)
        assert_equal(engine._core.auto_enable_auth, False)



if __name__ == "__main__":
    import nose
    nose.runmodule()
