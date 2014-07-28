#! /usr/bin/env python
#
"""
Unit tests for the OpenStack backend
"""
# Copyright (C) 2012-2013, GC3, University of Zurich. All rights reserved.
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
__version__ = '$Revision$'

# stdlib imports
import os
import tempfile

# 3rd party imports
from nose.tools import assert_equal, assert_false, assert_true, raises, set_trace

# local imports
import gc3libs.config
from gc3libs.backends.openstack import OpenStackLrms
import gc3libs.exceptions

def _setup_config_file(confstr):
    (fd, name) = tempfile.mkstemp()
    f = os.fdopen(fd, 'w+')
    f.write(confstr)
    f.close()
    return name

def test_openstack_variables_are_optionals():
    tmpfile = _setup_config_file("""

[auth/gc3user_ssh]
type = ssh
username = gc3-user

[auth/hobbes]
type=openstack

# Mandatory fields that are set via environment variable
# os_username=USERNAME
# os_password=PASSWORD
# os_project_name=TENANT

[resource/hobbes]
# Mandatory fields that are set via environment variable
# os_auth_url=http://cloud.gc3.uzh.ch:5000/v2.0
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
instance_type=m1.small
image_id=a9e98055-5aa3-4636-8fc5-f3b2b4ea66bb
security_group_name=gc3pie_ssh
security_group_rules=tcp:22:22:0.0.0.0/0, icmp:-1:-1:0.0.0.0/0
""")
    cfgvalues = ['username', 'password', 'tenant_name', 'auth_url']
    for name in cfgvalues:
        os.environ['OS_' + name.upper()] =  name

    try:
        cfg = gc3libs.config.Configuration(tmpfile)
        resources = cfg.make_resources()
        assert 'hobbes' in resources

        for name in cfgvalues:
            assert hasattr(resources['hobbes'], 'os_'+name)
            assert_equal(getattr(resources['hobbes'], 'os_'+name), name)
    finally:
        os.remove(tmpfile)


class TestOpenStackLrms(object):
    pass

## main: run tests

if "__main__" == __name__:
    import nose
    nose.runmodule()
