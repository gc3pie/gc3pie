# utils.py
# -*- coding: utf-8 -*-
"""
Utility functions for use in unit test code.
"""
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
from contextlib import contextmanager
import sys
from tempfile import NamedTemporaryFile



@contextmanager
def temporary_config(cfgtext=None, keep=False):
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
                            suffix='.tmp', delete=(not keep)) as cfgfile:
        cfgfile.write(cfgtext)
        cfgfile.flush()
        yield cfgfile
        # file is automatically deleted upon exit
