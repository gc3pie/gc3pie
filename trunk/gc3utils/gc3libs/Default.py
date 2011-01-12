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


HOMEDIR = os.path.expandvars('$HOME')
RCDIR = os.path.join(HOMEDIR, ".gc3")
CONFIG_FILE_LOCATION = os.path.join(RCDIR, "gc3pie.conf")
CONFIG_FILE_LOCATIONS = [ "/etc/gc3/gc3pie.conf", CONFIG_FILE_LOCATION ]
LOG_FILE_LOCATION = RCDIR + '/logging.conf'
JOBLIST_FILE = RCDIR + "/.joblist"
JOBLIST_LOCK = RCDIR + "/.joblist_lock"
JOB_FOLDER_LOCATION = os.getcwd()
AAI_CREDENTIAL_REPO = RCDIR + "/aai_credential"
GAMESS_XRSL_TEMPLATE = os.path.expandvars("$HOME/.gc3/gamess_template.xrsl")
JOB_FILE = ".lrms_id"
JOB_FINISHED_FILE = ".finished"
JOB_LOG = ".log"
DOWNLOAD_DIR = os.getcwd()
JOBS_DIR = os.path.join(RCDIR, "jobs")

ARC_CACHE_TIME = 90 # only update ARC resources status every this seconds

ARC_LRMS = 1
SGE_LRMS = 2

# Transport information
SSH_PORT = 22
SSH_CONNECT_TIMEOUT = 30
