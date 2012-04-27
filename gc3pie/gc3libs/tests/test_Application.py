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


from gc3libs import Application, Run

def test_invalid_invocation():
    try:
        Application()
    except TypeError, e:
        assert str(e) == '__init__() takes exactly 6 arguments (1 given)'

def test_mandatory_arguments():
    # check for all mandatory arguments
    ma = {'executable': '/bin/true',
          'arguments': [],
          'inputs': [],
          'outputs': [],
          'output_dir': '/tmp',
          }
    
    # test *valid* invocation
    Application(**ma)
    # from meliae import scanner,loader

    # out = open('meiae.dump','w')
    # scanner.dump_all_referenced(out, Application(**ma))
    # print loader.load('meiae.dump').summarize()
    # out.close()
    # test *invalid* invocation removing only one of the arguments
    for k in ma:
        _tmp = ma.copy()
        del _tmp[k]
        try:
            Application(**_tmp)
        except TypeError, e:
            assert "__init__() takes exactly" in str(e)

def test_wrong_type_arguments():
    # Things that will raise errors:
    # * unicode arguments
    # * unicode files in inputs or outputs
    # * remote paths (outputs) must not be absolute
    #
    # What happens when you request non-integer cores/memory/walltime?
    # what happens when you request non-existent architecture?

    ma = {'executable': '/bin/true',
          'arguments': [],
          'inputs': [],
          'outputs': [],
          'output_dir': '/tmp',
          'requested_cores': 1,          
          }
    for k,v  in {
        # 'inputs' : ['duplicated', 'duplicated'],
        # duplicated inputs doesnt raise an exception but just a warning
        'outputs' : ['/should/not/be/absolute'],
        # 'outputs' : ['duplicated', 'duplicated'],
        # duplicated outputs doesnt raise an exception but just a warning
        'requested_architecture' : 'FooBar',
        'requested_cores' : 'one',
        }.items():
        _tmpma = ma.copy()
        _tmpma[k] = v
        try:
            app = Application(**_tmpma)
        except:
            continue
        assert "We should have got an exception!" is False

def test_valid_invocation():
    ma = {'executable': '/bin/true',
          'arguments': [],
          'inputs': ['/tmp/a', 'b'],
          'outputs': ['o1', 'o2'],
          'output_dir': '/tmp',
          }
    app = Application(**ma)
    
def test_io_spec_to_dict_unicode():
    import gc3libs.url
    try:
        Application._io_spec_to_dict(gc3libs.url.UrlKeyDict, {u'/tmp/\u0246':u'\u0246', '/tmp/b/':'b'}, True)
    except UnicodeEncodeError, e:
        assert "UnicodeEncodeError in Application._io_spec_to_dict was expected" is False
## main: run tests





if "__main__" == __name__:
    test_invalid_invocation()
    test_mandatory_arguments()
    test_wrong_type_arguments()
    test_valid_invocation()
    test_io_spec_to_dict_unicode()
