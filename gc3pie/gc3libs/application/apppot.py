#! /usr/bin/env python
#
"""
Support for AppPot-hosted applications.

For more details about AppPot, visit:
<http://apppot.googlecode.com>
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

import gc3libs


class AppPotApplication(gc3libs.Application):
    """
    Base class for AppPot-hosted applications.
    Provides the same interface as the base `Application`:class:
    and runs the specified command in an AppPot instance.

    In addition to the standard `Application`:class: keyword
    arguments, the following ones can be given to steer the AppPot
    execution:

    * `apppot_img`: Path or URL to the AppPot system image to use.
      If ``None`` (default), then the default AppPot system image
      on the remote system is used.
    """
    def __init__(self, executable, arguments, inputs, outputs, output_dir,
                 apppot_img=None, apppot_tag='ENV/APPPOT-0.21', **kw):
        # AppPot-specific setup
        apppot_start_args = [] 
        if apppot_img is not None:
            apppot_start_args += ['--apppot', 'apppot.img']
            # XXX: Need to deal with the two possibilities for
            # initializing the `inputs` list.  Can this be simplified
            # by making `inputs` a writeable property of an
            # `Application` object?
            if isinstance(inputs, dict):
                inputs[apppot_img] = 'apppot.img'
            elif isinstance(inputs, list):
                inputs.append( (apppot_img, 'apppot.img') )
            else:
                raise TypeError("Unexpected type for `inputs` parameter: need `dict` or `list`.")
        if kw.has_key('requested_memory'):
            apppot_start_args += ['--mem', ("%dM" % (int(kw['requested_memory']) * 1000))]
            # FIXME: we need to remove the memory limit because batch
            # systems miscompute the amount of memory actually used by
            # an UMLx process...
            del kw['requested_memory']
        apppot_start_args += [ executable ] + arguments

        kw.setdefault('tags', dict())
        kw['tags'].append(apppot_tag)
        
        # init base class
        gc3libs.Application.__init__(
            self,
            # FIXME: this is needed for ARC submissions,
            # because otherwise ARC insists that 'apppot-start.sh'
            # should be included in "inputFiles", but it obviously
            # breaks all other submission schemes...
            '/$APPPOT_STARTUP', #'apppot-start.sh', # executable
            apppot_start_args, # arguments
            inputs, outputs, output_dir, **kw)


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="apppot",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
