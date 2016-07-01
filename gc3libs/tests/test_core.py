# test_core.py
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
import sys

# nose
from nose.tools import raises, assert_equal
try:
    from nose.tools import assert_is_instance
except ImportError:
    # Python 2.6 does not support assert_is_instance()
    def assert_is_instance(obj, cls):
        assert (isinstance(obj, cls))

# GC3Pie imports
from gc3libs import Run, Application, create_core
import gc3libs.config
from gc3libs.core import Core, MatchMaker
from gc3libs.quantity import GB, hours

from helpers import temporary_config


@raises(gc3libs.exceptions.NoResources)
def test_core_disable_resource_on_auth_init_failure():
    """Test that a resource is disabled if the auth cannot be initialized successfully."""
    # create "bad authentication" class
    class BadInitAuth(object):
        def __init__(self, **auth):
            raise RuntimeError("Bad authentication object!")

        def is_valid(self):
            raise AssertionError("This method should have never been called!")

        def check(self):
            raise AssertionError("This method should have never been called!")

        def enable(self):
            raise AssertionError("This method should have never been called!")

    _test_core_disable_resource_on_auth_failure(BadInitAuth)


@raises(gc3libs.exceptions.NoResources)
def test_core_disable_resource_on_auth_check_failure():
    """Test that a resource is disabled if the auth cannot be checked successfully."""
    # create "bad authentication" class
    class BadCheckAuth(object):

        def __init__(self, **auth):
            pass

        def is_valid(self):
            raise AssertionError("This method should have never been called!")

        def check(self):
            raise RuntimeError("Bad authentication object!")

        def enable(self):
            raise AssertionError("This method should have never been called!")

    _test_core_disable_resource_on_auth_failure(BadCheckAuth)


@raises(gc3libs.exceptions.NoResources)
def test_core_disable_resource_on_auth_enable_failure():
    """Test that a resource is disabled if the auth cannot be enabled successfully."""
    # create "bad authentication" class
    class BadEnableAuth(object):

        def __init__(self, **auth):
            pass

        def is_valid(self):
            raise AssertionError("This method should have never been called!")

        def check(self):
            return False  # so that `enable` is called next

        def enable(self):
            raise RuntimeError("Bad authentication object!")

    _test_core_disable_resource_on_auth_failure(BadEnableAuth)


def _test_core_disable_resource_on_auth_failure(auth_cls):
    """Common code for `test_core_disable_resource_on_auth_*_failure`."""
    gc3libs.authentication.Auth.register('bad', auth_cls)
    # set up
    cfg = gc3libs.config.Configuration()
    cfg.auths['bad_auth'].update(
        type='bad',
        username='fake',
    )
    cfg.resources['test'].update(
        name='test',
        type='shellcmd',
        transport='ssh',
        auth='bad_auth',
        max_cores_per_job=1,
        max_memory_per_core=1*GB,
        max_walltime=8*hours,
        max_cores=10,
        architecture=Run.Arch.X86_64,
    )
    core = Core(cfg)


def test_create_core_default():
    """Test `create_core` with factory defaults."""
    with temporary_config() as cfgfile:
        # std factory params
        core = create_core(cfgfile.name)
        assert_is_instance(core, Core)
        assert_equal(core.auto_enable_auth, True)


def test_create_core_non_default():
    """Test `create_core` with non-default arguments."""
    with temporary_config() as cfgfile:
        # use a specific MatchMaker instance for equality testing
        mm = MatchMaker()
        core = create_core(cfgfile.name, matchmaker=mm)
        assert_equal(core.auto_enable_auth, True)
        assert_equal(core.matchmaker, mm)


def test_create_core_no_auto_enable_auth():
    """Test `create_core` without the "auto enable" feature."""
    with temporary_config() as cfgfile:
        # std factory params
        core = create_core(cfgfile.name, auto_enable_auth=False)
        assert_equal(core.auto_enable_auth, False)
