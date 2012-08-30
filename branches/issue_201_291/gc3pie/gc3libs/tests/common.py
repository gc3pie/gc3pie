#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011, GC3, University of Zurich. All rights reserved.
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

def run_my_tests(local_variables):
    errors = 0
    tests = 0
    for i in sorted(local_variables):
        if not i.startswith('test_'): continue
        tests += 1
        try:
            print "%s ... " % i,
            local_variables[i]()
            print "ok"
        except Exception, e:
            print "FAIL"
            print "Error in function %s" % i
            print "Exception %s: %s" % (repr(e), e)
            errors +=1
            print
    print
    print "Run %d tests" % tests
    if errors: print "Errors: %d" % errors
    else: print "No errors! Good!"

