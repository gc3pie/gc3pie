#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011, 2012, 2019,  University of Zurich. All rights reserved.
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
from __future__ import absolute_import, print_function, unicode_literals
__docformat__ = 'reStructuredText'


import pytest

from gc3libs import Application
import gc3libs.exceptions


def test_invalid_invocation():
    with pytest.raises(TypeError):
        Application()

app_mandatory_arguments = (
    'arguments',
    'inputs',
    'outputs',
    'output_dir',
)

@pytest.mark.parametrize("mandatory", app_mandatory_arguments)
def test_mandatory_arguments(mandatory):
    # check for all mandatory arguments
    args = {
        'arguments': ['/bin/true'],
        'inputs': [],
        'outputs': [],
        'output_dir': '/tmp',
    }

    # test *valid* invocation
    Application(**args)

    del args[mandatory]

    # now check that invocation is *invalid*
    with pytest.raises(TypeError):
        Application(**args)


app_wrong_arguments = (
    # 'inputs' : ['duplicated', 'duplicated'],
    # duplicated inputs doesnt raise an exception but just a warning
    ('outputs', ['/should/not/be/absolute']),
    # 'outputs' : ['duplicated', 'duplicated'],
    # duplicated outputs doesnt raise an exception but just a warning
    ('requested_architecture', 'FooBar'),
    ('requested_cores', 'one'),
)

@pytest.mark.parametrize("wrongarg", app_wrong_arguments)
def test_wrong_type_arguments(wrongarg):
    # Things that will raise errors:
    # * unicode arguments
    # * unicode files in inputs or outputs
    # * remote paths (outputs) must not be absolute
    #
    # What happens when you request non-integer cores/memory/walltime?
    # what happens when you request non-existent architecture?

    args = {
        'arguments': ['/bin/true'],
        'inputs': [],
        'outputs': [],
        'output_dir': '/tmp',
        'requested_cores': 1,
    }

    key, value = wrongarg

    args[key] = value
    with pytest.raises((gc3libs.exceptions.InvalidArgument, ValueError)):
        Application(**args)


def test_valid_invocation():
    ma = {'arguments': ['/bin/true'],
          'inputs': ['/tmp/a', 'b'],
          'outputs': ['o1', 'o2'],
          'output_dir': '/tmp',
          }
    Application(**ma)


def test_io_spec_to_dict_mixed():
    # pylint: disable=import-error,protected-access,redefined-outer-name
    from gc3libs.utils import text_str
    d = Application._io_spec_to_dict(
          gc3libs.url.UrlKeyDict, {
              u'/tmp/\u0246': u'\u0246',
              b'/tmp/b/': b'b'},
        True)
    for k, v in d.items():
        # Python 3+
        assert isinstance(v, text_str)
        assert isinstance(k.path, text_str)


# main: run tests

if "__main__" == __name__:
    pytest.main(["-v", __file__])
