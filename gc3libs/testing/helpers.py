# -*- coding: utf-8 -*-

"""
Utility functions for use in unit test code.
"""
#
#  Copyright (C) 2015, 2016, 2018, 2019  University of Zurich. All rights reserved.
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
from builtins import range
from contextlib import contextmanager
from tempfile import NamedTemporaryFile, mkdtemp
import shutil

# GC3Pie imports
from gc3libs import Application, Run
from gc3libs.config import Configuration
from gc3libs.core import Core, Engine
from gc3libs.quantity import GB, MB, hours
from gc3libs.workflow import ParallelTaskCollection, SequentialTaskCollection


@contextmanager
def test_resource(name='test', **params):
    """
    Yield a GC3Pie configuration containing a single resource,
    built using the given parameters.

    The only resource is named ``test`` (can be changed by passing
    keyword argument ``name``).

    .. note::

      The parameters *must* be given in the internal format expected
      by the backend "LRMS" constructors, not in the string format
      expected by the configuration file parser.
    """
    cfg = Configuration()
    cfg.TYPE_CONSTRUCTOR_MAP['noop'] = ('gc3libs.backends.noop', 'NoOpLrms')
    rsc = cfg.resources[name]
    # defaults (1) -- general
    rsc.update(
        name=name,
        type='noop',
        transport='local',
        auth='none',
        architecture=set([Run.Arch.X86_64]),
        enabled=True,
        # Use unusual values so that we can easily spot if the `override` option works
        large_file_chunk_size=1.78 * MB,
        large_file_threshold=1.414 * GB,
        max_cores=123,
        max_cores_per_job=123,
        max_memory_per_core=999 * GB,
        max_walltime=7 * hours,
    )
    # update with given parameters
    rsc.update(**params)
    # defaults (2) -- type-specific
    if rsc.type == 'shellcmd':
        rsc.setdefault('override', False)
        rsc.setdefault('time_cmd', '/usr/bin/time')

    yield cfg

    # since TYPE_CONSTRUCTOR_MAP is a class-level variable, we
    # need to clean up otherwise other tests will see the No-Op
    # backend
    del cfg.TYPE_CONSTRUCTOR_MAP['noop']


@contextmanager
def temporary_core(transition_graph=None, **params):
    with test_resource(**params) as cfg:
        name = params.get('name', 'test')
        core = Core(cfg)
        rsc = core.get_backend(name)
        # give each job a 50% chance of moving from one state to the
        # next one
        if transition_graph:
            rsc.transition_graph = transition_graph

        yield core


@contextmanager
def temporary_directory(*args, **kwargs):
    tmpdir = mkdtemp(*args, **kwargs)
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@contextmanager
def temporary_engine(transition_graph=None, **kw):
    with temporary_core(transition_graph, **kw) as core:
        yield Engine(core)


@contextmanager
def temporary_config_file(cfgtext=None, keep=False):
    """
    Write a GC3Pie configuration into a temporary file.

    Yields an open file object pointing to the configuration file.  Its
    ``.name`` attribute holds the file path in the filesystem.
    """
    if cfgtext is None:
        cfgtext = ("""
[resource/test]
enabled = yes
type = shellcmd
frontend = localhost
transport = local
max_cores_per_job = 4
max_memory_per_core = 8GiB
max_walltime = 8 hours
max_cores = 10
architecture = x86_64
auth = none
override = no
            """)
    with NamedTemporaryFile(prefix='gc3libs.test.',
                            suffix='.tmp',
                            mode='w+t',
                            delete=(not keep)) as cfgfile:
        cfgfile.write(cfgtext)
        cfgfile.flush()
        yield cfgfile
        # file is automatically deleted upon exit

def example_cfg_dict():
    """
    Write a GC3Pie configuration into a Python dictionary.
    """
    return {
        "resource/test": {
            'enabled': 'yes',
            'type': 'shellcmd',
            'frontend': 'localhost',
            'transport': 'local',
            'max_cores_per_job': '4',
            'max_memory_per_core': '8GiB',
            'max_walltime': '8 hours',
            'max_cores': '10',
            'architecture': 'x86_64',
            'auth': 'none',
            'override': 'no'
        }
    }

@contextmanager
def temporary_config(cfgtext=None):
    """
    Return a GC3Pie ``Configuration`` object.

    Optional argument `cfgtext` holds the contents of the configuration file to
    use. If not given, a default one will be used.

    """
    with temporary_config_file(cfgtext) as cfgfile:
        yield Configuration(cfgfile.name)


class SuccessfulApp(Application):
    """An application instance reporting always a zero exit code."""
    def __init__(self, name='success', **extra_args):
        Application.__init__(
            self,
            ['/bin/true'],
            inputs=[],
            outputs=[],
            output_dir='/tmp',
            jobname=name,
            requested_cores=1,
            **extra_args)

    def terminated(self):
        self.execution.returncode = 0


class UnsuccessfulApp(Application):
    """An application reporting always a non-zero exit code."""
    def __init__(self, name='fail', **extra_args):
        Application.__init__(
            self,
            ['/bin/false'],
            inputs=[],
            outputs=[],
            output_dir='/tmp',
            jobname=name,
            requested_cores=1,
            **extra_args)

    def terminated(self):
        self.execution.returncode = (0, 1)


class SimpleParallelTaskCollection(ParallelTaskCollection):
    def __init__(self, num_tasks, **extra_args):
        tasks = [SuccessfulApp('stage{n}'.format(n=n)) for n in range(num_tasks)]
        ParallelTaskCollection.__init__(self, tasks, **extra_args)


class SimpleSequentialTaskCollection(SequentialTaskCollection):
    def __init__(self, num_tasks, **extra_args):
        tasks = [SuccessfulApp('stage{n}'.format(n=n)) for n in range(num_tasks)]
        SequentialTaskCollection.__init__(self, tasks, **extra_args)
