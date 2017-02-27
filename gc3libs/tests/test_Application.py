#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011, 2012, University of Zurich. All rights reserved.
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


import pytest

from gc3libs import Application
import gc3libs.exceptions


@pytest.mark.xfail(raises=TypeError)
def test_invalid_invocation():
    Application()

app_mandatory_arguments = (
    'arguments',
    'inputs',
    'outputs',
    'output_dir',
)
    
@pytest.mark.parametrize("mandatory", app_mandatory_arguments)
@pytest.mark.xfail(raises=TypeError)
def test_mandatory_arguments(mandatory):
    # check for all mandatory arguments
    ma = {'arguments': ['/bin/true'],
          'inputs': [],
          'outputs': [],
          'output_dir': '/tmp',
          }

    # test *valid* invocation
    Application(**ma)

    del ma[mandatory]


app_wrong_arguments = (
    # 'inputs' : ['duplicated', 'duplicated'],
    # duplicated inputs doesnt raise an exception but just a warning
    ('outputs', ['/should/not/be/absolute']),
    # 'outputs' : ['duplicated', 'duplicated'],
    # duplicated outputs doesnt raise an exception but just a warning
    ('requested_architecture', 'FooBar'),
    ('requested_cores', 'one'),
)

@pytest.mark.parametrize("wrongargs", app_wrong_arguments)
@pytest.mark.xfail(raises=(gc3libs.exceptions.InvalidArgument, ValueError))
def test_wrong_type_arguments(wrongargs):
    # Things that will raise errors:
    # * unicode arguments
    # * unicode files in inputs or outputs
    # * remote paths (outputs) must not be absolute
    #
    # What happens when you request non-integer cores/memory/walltime?
    # what happens when you request non-existent architecture?

    ma = {'arguments': ['/bin/true'],
          'inputs': [],
          'outputs': [],
          'output_dir': '/tmp',
          'requested_cores': 1,
          }

    ma[wrongargs[0]] = wrongargs[1]
    Application(**ma)


def test_valid_invocation():
    ma = {'arguments': ['/bin/true'],
          'inputs': ['/tmp/a', 'b'],
          'outputs': ['o1', 'o2'],
          'output_dir': '/tmp',
          }
    Application(**ma)


@pytest.mark.xfail(raises=gc3libs.exceptions.InvalidValue)
def test_io_spec_to_dict_unicode():
    import gc3libs.url
    Application._io_spec_to_dict(
        gc3libs.url.UrlKeyDict, {
            u'/tmp/\u0246': u'\u0246',
            '/tmp/b/': 'b'},
        True)


# main: run tests

if "__main__" == __name__:
    pytest.main(["-v", __file__])
