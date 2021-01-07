# test_core.py
# -*- coding: utf-8 -*-
#
#  Copyright (C) 2015-2018  University of Zurich. All rights reserved.
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
from builtins import object
import os

import pytest

# GC3Pie imports
from gc3libs import Run, Application, create_core
import gc3libs.config
from gc3libs.core import Core, MatchMaker
from gc3libs.quantity import GB, GiB, hours
from gc3libs.utils import string_to_boolean

from gc3libs.testing.helpers import example_cfg_dict, temporary_config_file, temporary_core


def test_core_resources():
    """
    Check that configured resources can be accessed through the `Core` object.
    """
    with temporary_core() as core:
        resources = core.resources
        assert len(resources) == 1
        assert 'test' in resources
        test_rsc = resources['test']
        # these should match the resource definition in `gc3libs.testing.helpers.temporary_core`
        assert test_rsc.max_cores_per_job == 123
        assert test_rsc.max_memory_per_core == 999*GB
        assert test_rsc.max_walltime == 7*hours


@pytest.mark.skipif(
    string_to_boolean(os.environ.get('GC3PIE_RESOURCE_INIT_ERRORS_ARE_FATAL', 'no')),
    reason="Skipping test: not compatible with GC3PIE_RESOURCE_INIT_ERRORS_ARE_FATAL=yes")
def test_core_disable_resource_on_auth_init_failure():
    """Test that a resource is disabled if the auth cannot be initialized successfully."""

    # pylint: disable=no-self-use,unused-argument
    class BadInitAuth(object):
        """Fail all authentication methods."""
        def __init__(self, **auth):
            raise RuntimeError("Bad authentication object!")

        def is_valid(self):
            raise AssertionError("This method should have never been called!")

        def check(self):
            raise AssertionError("This method should have never been called!")

        def enable(self):
            raise AssertionError("This method should have never been called!")

    with pytest.raises(gc3libs.exceptions.NoResources):
        _test_core_disable_resource_on_auth_failure(BadInitAuth)


@pytest.mark.skipif(
    string_to_boolean(os.environ.get('GC3PIE_RESOURCE_INIT_ERRORS_ARE_FATAL', 'no')),
    reason="Skipping test: not compatible with GC3PIE_RESOURCE_INIT_ERRORS_ARE_FATAL=yes")
def test_core_disable_resource_on_auth_check_failure():
    """Test that a resource is disabled if the auth cannot be checked successfully."""

    # pylint: disable=no-self-use,unused-argument
    class BadCheckAuth(object):
        """Fail `Authentication.check()`"""

        def __init__(self, **auth):
            pass

        def is_valid(self):
            raise AssertionError("This method should have never been called!")

        def check(self):
            raise RuntimeError("Bad authentication object!")

        def enable(self):
            raise AssertionError("This method should have never been called!")

    with pytest.raises(gc3libs.exceptions.NoResources):
        _test_core_disable_resource_on_auth_failure(BadCheckAuth)


@pytest.mark.skipif(
    string_to_boolean(os.environ.get('GC3PIE_RESOURCE_INIT_ERRORS_ARE_FATAL', 'no')),
    reason="Skipping test: not compatible with GC3PIE_RESOURCE_INIT_ERRORS_ARE_FATAL=yes")
def test_core_disable_resource_on_auth_enable_failure():
    """Test that a resource is disabled if the auth cannot be enabled successfully."""

    # pylint: disable=no-self-use,unused-argument
    class BadEnableAuth(object):
        """Fail `Authentication.enable()`"""

        def __init__(self, **auth):
            pass

        def is_valid(self):
            raise AssertionError("This method should have never been called!")

        def check(self):
            return False  # so that `enable` is called next

        def enable(self):
            raise RuntimeError("Bad authentication object!")

    with pytest.raises(gc3libs.exceptions.NoResources):
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
    with temporary_config_file() as cfgfile:
        # std factory params
        core = create_core(cfgfile.name)
        assert isinstance(core, Core)
        assert core.auto_enable_auth == True


def test_create_core_non_default():
    """Test `create_core` with non-default arguments."""
    with temporary_config_file() as cfgfile:
        # use a specific MatchMaker instance for equality testing
        mm = MatchMaker()
        core = create_core(cfgfile.name, matchmaker=mm)
        assert core.auto_enable_auth == True
        assert core.matchmaker == mm

def test_create_core_with_cfg_dict():
    """
    Check that we can use a python dictionary in `create_core` to configure resources.
    """
    core = create_core(cfg_dict=example_cfg_dict())
    resources = core.resources
    assert len(resources) == 1
    assert 'test' in resources
    test_rsc = resources['test']
    assert test_rsc.max_cores_per_job == 4
    assert test_rsc.max_memory_per_core == 8*GiB
    assert test_rsc.max_walltime == 8*hours


def test_create_core_no_auto_enable_auth():
    """Test `create_core` without the "auto enable" feature."""
    with temporary_config_file() as cfgfile:
        # std factory params
        core = create_core(cfgfile.name, auto_enable_auth=False)
        assert core.auto_enable_auth == False
