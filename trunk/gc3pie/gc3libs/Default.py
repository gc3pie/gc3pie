#! /usr/bin/env python
"""
A namespace for all constants and default values used in the GC3Libs
package.
"""
# Copyright (C) 2009-2011 GC3, University of Zurich. All rights reserved.
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


import os
import os.path


RCDIR = os.path.join(os.path.expandvars('$HOME'), ".gc3")
CONFIG_FILE_LOCATIONS = [
    # system-wide config file
    "/etc/gc3/gc3pie.conf",
    # user-private config file
    os.path.join(RCDIR, "gc3pie.conf")
    ]
JOBS_DIR = os.path.join(RCDIR, "jobs")

ARC_LRMS = 'arc'
ARC_CACHE_TIME = 90 # only update ARC resources status every this seconds

SGE_LRMS = 'ssh_sge'
# Transport information
SSH_PORT = 22
SSH_CONNECT_TIMEOUT = 30

# Proxy
PROXY_VALIDITY_THRESHOLD = 600 # Proxy validity threshold in seconds. If proxy is expiring before the thresold, it will be marked as to be renewed.
