#! /usr/bin/env python
#
from __future__ import absolute_import, print_function, unicode_literals
"""
"""
# Copyright (C) 2011-2012  University of Zurich. All rights reserved.
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
"""
Check if all the backends are implementing all the needed methods.
"""

__docformat__ = 'reStructuredText'

from gc3libs.backends import LRMS


def check_class(cls):
    for name in [  # list of abstract methods in class `LRMS`
            'cancel_job',
            'close',
            'free',
            'get_resource_status',
            'get_results',
            'peek',
            'submit_job',
            'update_job_state',
            'validate_data',
    ]:
        if getattr(cls, name) == getattr(LRMS, name):
            raise NotImplementedError(
                "Abstract method `%s` not implemented in class `%s`"
                % (name, cls.__name__))


def test_shellcmd_backends():
    from gc3libs.backends.shellcmd import ShellcmdLrms
    check_class(ShellcmdLrms)


def test_lsf_backends():
    from gc3libs.backends.lsf import LsfLrms
    check_class(LsfLrms)


def test_pbs_backends():
    from gc3libs.backends.pbs import PbsLrms
    check_class(PbsLrms)


def test_sge_backends():
    from gc3libs.backends.sge import SgeLrms
    check_class(SgeLrms)


if "__main__" == __name__:
    import pytest
    pytest.main(["-v", __file__])
