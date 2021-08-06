#! /usr/bin/env python
#
"""
Check that we don't run into Issue #670 again.
"""
# Copyright (C) 2021  Google LLC.
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

import logging
import warnings

from gc3libs import configure_logger
from gc3libs.compat._inspect import getargspec


loglevel = logging.ERROR
configure_logger(loglevel, "test_issue_670")


def test_issue_670():
    "Check that we don't use a deprecated `getargspec`."
    with warnings.catch_warnings(record=True) as w:
        # Cause all warnings to always be triggered.
        warnings.simplefilter("always")
        # Call our interface to the `inspect` module
        getargspec(test_issue_670)
        # No `DeprecationWarning` has been raised
        assert len(w) == 0


if "__main__" == __name__:
    import pytest
    pytest.main(["-v", __file__])
