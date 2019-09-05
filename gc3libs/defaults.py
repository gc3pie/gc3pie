#! /usr/bin/env python
"""
A namespace for constants and default values
used in the GC3Libs package.
"""
# Copyright (C) 2019  University of Zurich. All rights reserved.
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

from __future__ import absolute_import, division, unicode_literals


import os

from gc3libs.quantity import MiB


RCDIR = os.path.join(os.path.expandvars('$HOME'), ".gc3")
"""
Default directory where all GC3Pie-related files are stored.
"""

CONFIG_FILE_LOCATIONS = [
    # system-wide config file
    "/etc/gc3/gc3pie.conf",
    # virtualenv config file
    os.path.expandvars("$VIRTUAL_ENV/etc/gc3/gc3pie.conf"),
    # user-private config file: first look into `$GC3PIE_CONF`, and
    # fall-back to `~/.gc3/gc3pie.conf`
    os.environ.get('GC3PIE_CONF', os.path.join(RCDIR, "gc3pie.conf"))
]
"""
List of filesystem locations where config files would be read from.
"""

JOBS_DIR = os.path.join(RCDIR, "jobs")
"""
Default session directory for GC3Utils.

.. warning::

  Use of this global default session is deprecated.
"""


# the ARC backends have been removed, but keep their names around
# so we can issue a warning if a user still has these resources in
# the configuration file
ARC0_LRMS = 'arc0'
ARC1_LRMS = 'arc1'

SGE_LRMS = 'sge'
PBS_LRMS = 'pbs'
LSF_LRMS = 'lsf'
SHELLCMD_LRMS = 'shellcmd'
SLURM_LRMS = 'slurm'
SUBPROCESS_LRMS = 'shellcmd'
EC2_LRMS = 'ec2'
OPENSTACK_LRMS = 'openstack'


# SSH transport information
SSH_CONFIG_FILE = '~/.ssh/config'
SSH_PORT = 22
SSH_CONNECT_TIMEOUT = 30


PEEK_FILE_SIZE = 120  # expressed in bytes


VM_OS_OVERHEAD = 512 * MiB
"""
Subtract this amount from the available total memory,
when creating resource configuration from cloud-based VMs.
"""

LSF_CACHE_TIME = 30
"""
Time (in seconds) to cache lshosts/bjobs information for.
"""

SPOOLDIR = "$HOME/.gc3pie_jobs"
"""
Top-level path for the working directory of jobs.

On batch systems, this should be visible from both
the frontend and the compute nodes.
"""
