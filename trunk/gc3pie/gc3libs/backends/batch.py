#! /usr/bin/env python
#
"""
This module provides a generic BatchSystem class from which all
batch-like backends should inherit. 
"""
# Copyright (C) 2009-2012 GC3, University of Zurich. All rights reserved.
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
__version__ = 'development version (SVN $Revision$)'

# Define some commonly used functions

def get_qsub_jobid(qsub_output, regexp):
    """Parse the ``qsub`` output for the local jobid."""
    for line in qsub_output.split('\n'):
        match = regexp.match(line)
        if match:
            return match.group('jobid')
    raise gc3libs.exceptions.InternalError("Could not extract jobid from qsub output '%s'" 
                        % qsub_output.rstrip())
    

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="sge",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
