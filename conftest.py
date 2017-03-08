# pylint: disable=invalid-name,line-too-long
"""
Dynamically configure test collection for `py.test`

See: http://doc.pytest.org/en/latest/example/pythoncollection.html#customizing-test-collection-to-find-all-py-files
"""
#
# Copyright (C) 2017 University of Zurich.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import sys

# always ignore `setup.py` and other aux files
collect_ignore = [
    'conftest.py',
    'scripts/install.py',
    'setup.py',
]

# OpenStack-related modules cause an `ImportError` on Python 2.6
if sys.version_info < (2, 7):
    collect_ignore.append("gc3libs/backends/openstack.py")
    collect_ignore.append("gc3libs/backends/tests/test_openstack.py")
