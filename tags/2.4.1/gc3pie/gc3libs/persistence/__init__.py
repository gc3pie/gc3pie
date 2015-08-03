#! /usr/bin/env python
#
"""
Facade to store and retrieve Job information from permanent storage.

A usage warning
~~~~~~~~~~~~~~~

This module saves Python objects using the `pickle` framework: thus,
the `Application` subclass corresponding to a job must be already
loaded (or at least ``import``-able) in the Python interpreter for
`pickle` to be able to 'undump' the object from its on-disk
representation.

In other words, if you create a custom `Application` subclass in some
client code, GC3Utils won't be able to read job files created by this
code, because the class definition is not available in GC3Utils.

The recommended simple workaround is for a stand-alone script to
'import self' and then use the fully qualified name to run the script.
In other words, start your script with this boilerplate code::

    if __name__ == '__main__':
        import myscriptname
        myscriptname.MyScript().run()

The rest of the script now runs as the ``myscript`` module, which does
the trick!

.. note::

  Of course, the ``myscript.py`` file must be in the search path of
  the Python interpreter, or GC3Utils will still complain!

"""
# Copyright (C) 2009-2012 S3IT, Zentrale Informatik, University of Zurich. All rights reserved.
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
__version__ = '2.4.1 version (SVN $Revision$)'


# Export the "public API" towards other modules, so that
# one can do ``import gc3libs.persistence`` and load whatever
# should be normally needed and supported.  Other modules in
# this package should be considered "internal use only".
from .filesystem import FilesystemStore
from .idfactory import IdFactory, JobIdFactory
from .store import make_store, Persistable

__all__ = ['make_store', 'Persistable', 'IdFactory',
           'JobIdFactory', 'FilesystemStore']

# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="persistence",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
