#! /usr/bin/env python
#
"""
The `gc3libs.compat` namespace collects 3rd party packages that we
provide ourselves to be compatible across Python versions.

A package is included here if the upstream does no longer support all
the Python versions that we support in GC3Pie, or, conversely, we
provide here updated versions of standard Python packages.

Note that each package in the `gc3libs.compat` namespace comes with
its own licence terms, which might be different from those of GC3Pie.
Copyright of the included packages is hereby acknowledged to the
respective authors: the GC3Pie developers make no copyright claim on
the packages in the `gc3libs.compat` namespace.
"""
# This file is only present so that the `compat/` directory is
# recognized as a Python package.
from __future__ import absolute_import, print_function, unicode_literals
__docformat__ = 'reStructuredText'


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
