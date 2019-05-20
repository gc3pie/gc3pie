#! /usr/bin/env python
#
"""
Unit tests for the OpenStack backend
"""
# Copyright (C) 2012-2013, 2015  University of Zurich. All rights reserved.
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
#
from __future__ import absolute_import, print_function, unicode_literals
from builtins import object
__docformat__ = 'reStructuredText'

# stdlib imports
from collections import namedtuple
import os

# 3rd party imports
from mock import MagicMock
import pytest

# local imports
from gc3libs import Application
import gc3libs.config
import gc3libs.exceptions
from gc3libs.quantity import MiB, GB

# The OpenStack backend might not be installed (it's currently marked as
# optional in `setup.py`), or we may be running Python 2.6 which is no longer
# supported so skip these tests altogether if there is any error
OpenStackLrms = pytest.importorskip('gc3libs.backends.openstack').OpenStackLrms


from gc3libs.testing.helpers import temporary_config, temporary_config_file

class _const(object):
    SSH_AUTH_STANZA = """
[auth/gc3user_ssh]
type = ssh
username = gc3-user

"""

    RESOURCE_STANZA = """
[resource/hobbes]
os_region=RegionOne
public_key=~/.ssh/id_dsa.pub
name=hobbes
enabled = yes
type=openstack+shellcmd
auth=hobbes
max_cores_per_job = 8
max_memory_per_core = 2
max_walltime = 8
max_cores = 32
architecture = x86_64
keypair_name=keypair
vm_auth=gc3user_ssh
image_id=a9e98055-5aa3-4636-8fc5-f3b2b4ea66bb
security_group_name=gc3pie_ssh
security_group_rules=tcp:22:22:0.0.0.0/0, icmp:-1:-1:0.0.0.0/0

"""

    CLOUD_AUTH_STANZA_FULL = """
[auth/hobbes]
type=openstack
os_username=USERNAME
os_password=PASSWORD
os_project_name=TENANT

"""

    CLOUD_AUTH_STANZA_USE_ENV = """
[auth/hobbes]
type=openstack
# other fields should be set via environment variables

"""

@pytest.mark.xfail
def test_openstack_variables_are_optional():
    with temporary_config_file(
        _const.SSH_AUTH_STANZA
        + _const.CLOUD_AUTH_STANZA_USE_ENV
        + _const.RESOURCE_STANZA
    ) as cfgfile:
        # set up fake environment
        env = ['username', 'password', 'tenant_name', 'auth_url']
        for name in env:
            os.environ['OS_' + name.upper()] = name

        # check that resource has been correctly created
        cfg = gc3libs.config.Configuration(cfgfile.name)
        resources = cfg.make_resources()
        assert 'hobbes' in resources

        cloud = resources['hobbes']
        assert isinstance(cloud, OpenStackLrms)

        # check that values have been inherited from the environment
        for name in env:
            attr = ('os_' + name)
            assert hasattr(cloud, attr)
            assert getattr(cloud, attr) == name


def _setup_flavor_selection_tests(extra_conf=''):
    # FIXME: the test does not run if this is not in the environment!
    os.environ['OS_AUTH_URL'] = 'http://localhost:5000/v2/'
    env = ['username', 'password', 'tenant_name', 'auth_url']
    for name in env:
        os.environ['OS_' + name.upper()] = name

    with temporary_config_file(
        _const.SSH_AUTH_STANZA
        + _const.CLOUD_AUTH_STANZA_FULL
        + _const.RESOURCE_STANZA + extra_conf
    ) as cfgfile:
        cfg = gc3libs.config.Configuration(cfgfile.name)
        resources = cfg.make_resources()
        cloud = resources['hobbes']

    # mock novaclient's `Flavor` type using a namedtuple
    _Flavor = namedtuple('_Flavor', 'name vcpus ram disk'.split())
    flavors = (
        _Flavor(name='1cpu-4ram-hpc', vcpus=1, ram=4000, disk=100),
        _Flavor(name='4cpu-16ram-hpc', vcpus=4, ram=16000, disk=100),
        _Flavor(name='8cpu-8ram-server', vcpus=8, ram=7680, disk=100),
    )

    cloud.client = MagicMock()
    cloud.client.flavors.list.return_value = flavors
    # `_connect` fills the flavor list
    cloud._connect()

    return cloud, flavors


@pytest.mark.xfail
def test_flavor_selection_at_init():
    """
    Test that the "largest" flavor is correctly identified.
    """
    cloud, flavors = _setup_flavor_selection_tests()

    # check selection of the largest flavor in lexicographic order
    # (cpus, ram, disk)
    assert cloud['max_cores'] == flavors[2].vcpus
    assert cloud['max_memory_per_core'] == flavors[2].ram * MiB


@pytest.mark.xfail
def test_flavor_selection_default():
    """
    Test flavor-selection logic when no "instance type" is specified.
    """
    cloud, flavors = _setup_flavor_selection_tests()

    # no instance type specified, should get smallest one
    app1 = Application(
        ['/bin/true'],
        inputs=[],
        outputs=[],
        output_dir='/tmp',
    )
    flv = cloud.get_instance_type_for_job(app1)
    assert flv == flavors[0]

    # require 10 GB, get 4cpu-16ram-hpc flavor
    app2 = Application(
        ['/bin/true'],
        inputs=[],
        outputs=[],
        output_dir='/tmp',
        requested_memory=10*GB,
    )
    flv = cloud.get_instance_type_for_job(app2)
    assert flv == flavors[1]

    # require 6 cores, get 8cpu-8ram-server flavor
    app3 = Application(
        ['/bin/true'],
        inputs=[],
        outputs=[],
        output_dir='/tmp',
        requested_cores=6,
    )
    flv = cloud.get_instance_type_for_job(app3)
    assert flv == flavors[2]


@pytest.mark.xfail
def test_flavor_selection_generic_instance_type():
    """
    Test flavor selection when a default flavor is prescribed by config.
    """
    # NOTE: `EXTRA_CONF` has to start with a blank line and with the first
    # non-whitespace character in column 0, otherwise `ConfigParser` will
    # consider it a continuation of the last key/value assignment in
    # `RESOURCE_STANZA` ...
    EXTRA_CONF = """

instance_type = 4cpu-16ram-hpc
small_instance_type = 1cpu-4ram-hpc
"""
    cloud, flavors = _setup_flavor_selection_tests(EXTRA_CONF)

    # no instance type specified, should get configured one
    app1 = Application(
        ['/bin/true'],
        inputs=[],
        outputs=[],
        output_dir='/tmp',
    )
    flv = cloud.get_instance_type_for_job(app1)
    assert flv == flavors[1]

    # application-specific instance type specified, should override default
    app2 = Application(
        ['/bin/true'],
        inputs=[],
        outputs=[],
        output_dir='/tmp',
    )
    app2.application_name = 'small'
    flv = cloud.get_instance_type_for_job(app2)
    assert flv == flavors[0]

    # requirements exclude default instance type
    app3 = Application(
        ['/bin/true'],
        inputs=[],
        outputs=[],
        output_dir='/tmp',
        requested_cores=6,
    )
    app3.application_name = 'large'
    flv = cloud.get_instance_type_for_job(app3)
    assert flv == flavors[2]


@pytest.mark.xfail
def test_flavor_selection_application_specific():
    """
    Test flavor selection when an application-specific flavor is prescribed by config.
    """
    # NOTE: `EXTRA_CONF` has to start with a blank line and with the first
    # non-whitespace character in column 0, otherwise `ConfigParser` will
    # consider it a continuation of the last key/value assignment in
    # `RESOURCE_STANZA` ...
    EXTRA_CONF = """

large_instance_type = 4cpu-16ram-hpc
"""
    cloud, flavors = _setup_flavor_selection_tests(EXTRA_CONF)

    # no instance type specified, should get smallest one
    app1 = Application(
        ['/bin/true'],
        inputs=[],
        outputs=[],
        output_dir='/tmp',
    )
    flv = cloud.get_instance_type_for_job(app1)
    assert flv == flavors[0]

    # application-specific instance type specified
    app2 = Application(
        ['/bin/true'],
        inputs=[],
        outputs=[],
        output_dir='/tmp',
    )
    app2.application_name = 'large'
    flv = cloud.get_instance_type_for_job(app2)
    assert flv == flavors[1]

    # application-specific instance type specified,
    # but requirements exclude it
    app3 = Application(
        ['/bin/true'],
        inputs=[],
        outputs=[],
        output_dir='/tmp',
        requested_cores=6,
    )
    app3.application_name = 'large'
    flv = cloud.get_instance_type_for_job(app3)
    assert flv == flavors[2]


@pytest.mark.xfail
def test_flavor_selection_application_specific_unavailable():
    """
    Test that application-specific "instance type" is ignored if unavailable.
    """
    # NOTE: `EXTRA_CONF` has to start with a blank line and with the first
    # non-whitespace character in column 0, otherwise `ConfigParser` will
    # consider it a continuation of the last key/value assignment in
    # `RESOURCE_STANZA` ...
    EXTRA_CONF = """

large_instance_type = 5cpu-11ram-server
"""
    cloud, flavors = _setup_flavor_selection_tests(EXTRA_CONF)

    # no instance type specified, should get smallest one
    app1 = Application(
        ['/bin/true'],
        inputs=[],
        outputs=[],
        output_dir='/tmp',
    )
    flv = cloud.get_instance_type_for_job(app1)
    assert flv == flavors[0]

    # application-specific instance type specified but unavailable:
    # should yield same result as before
    app2 = Application(
        ['/bin/true'],
        inputs=[],
        outputs=[],
        output_dir='/tmp',
    )
    app2.application_name = 'large'
    flv = cloud.get_instance_type_for_job(app2)
    assert flv == flavors[0]

    # application-specific instance type specified,
    # but requirements exclude it
    app3 = Application(
        ['/bin/true'],
        inputs=[],
        outputs=[],
        output_dir='/tmp',
        requested_cores=6,
    )
    app3.application_name = 'large'
    flv = cloud.get_instance_type_for_job(app3)
    assert flv == flavors[2]


# main: run tests

if "__main__" == __name__:
    import pytest
    pytest.main(["-v", __file__])
