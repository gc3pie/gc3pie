#! /usr/bin/env python
"""
Defaults for the GC3Libs package.
"""
# Copyright (C) 2009-2010 GC3, University of Zurich. All rights reserved.
#
# Includes parts adapted from the ``bzr`` code, which is
# copyright (C) 2005, 2006, 2007, 2008, 2009 Canonical Ltd
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


import types
import os
import os.path
from InformationContainer import *

# -----------------------------------------------------
# Default
#

HOMEDIR = os.path.expandvars('$HOME')
RCDIR = HOMEDIR + "/.gc3"
CONFIG_FILE_LOCATION = RCDIR + "/gc3pie.conf"
LOG_FILE_LOCATION = RCDIR + '/logging.conf'
JOBLIST_FILE = RCDIR + "/.joblist"
JOBLIST_LOCK = RCDIR + "/.joblist_lock"
JOB_FOLDER_LOCATION = os.getcwd()
AAI_CREDENTIAL_REPO = RCDIR + "/aai_credential"
GAMESS_XRSL_TEMPLATE = os.path.expandvars("$HOME/.gc3/gamess_template.xrsl")
JOBS_DIR = RCDIR + "/jobs"
JOB_FILE = ".lrms_id"
JOB_FINISHED_FILE = ".finished"
JOB_LOG = ".log"
CACHE_TIME = 90 # only update ARC resources status every this seconds

ARC_LRMS = 1
SGE_LRMS = 2

SMSCG_AUTHENTICATION = 1
SSH_AUTHENTICATION = 2
NONE_AUTHENTICATION = 3

# email notification information
NOTIFY_USER_EMAIL = "default_urename@gc3.uzh.ch"
NOTIFY_USERNAME = "sergio"
NOTIFY_GC3ADMIN = "sergio.maffioletti@gc3.uzh.ch"
NOTIFY_SUBJECTS = "Job notification"
NOTIFY_MSG = """This is an authomatic generated email."""
NOTIFY_DESTINATIONFOLDER = os.path.join('/tmp',NOTIFY_USERNAME)

class Default(InformationContainer):

    def __init__(self,initializer=None,**keywd):
        """
        Create a new Defaults object.

        Examples::

          >>> df = Default()
        """
        InformationContainer.__init__(self,initializer,**keywd)
#        super(InformationContainer)
        self.update(homedir=HOMEDIR)
        self.update(config_file_location=CONFIG_FILE_LOCATION)
        self.update(joblist_location=JOBLIST_FILE)
        self.update(joblist_lock=JOBLIST_LOCK)
        self.update(job_folder_location=JOB_FOLDER_LOCATION)

    def is_valid(self):
        return True


if __name__ == '__main__':
    import doctest
    doctest.testmod()
