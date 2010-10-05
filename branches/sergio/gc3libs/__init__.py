#!/usr/bin/python

# NG's default packages install arclib into /opt/nordugrid/lib/pythonX.Y/site-packages;
# add this anyway in case users did not set their PYTHONPATH correctly
import sys
import os

nordugrid_location = os.path.join(os.path.expandvars('$NORDUGRID_LOCATION'))
if not os.path.isdir(nordugrid_location):
    nordugrid_location = '/opt/nordugrid'

sys.path.append(os.path.join(nordugrid_location,'lib/python%d.%d/site-packages' % sys.version_info[:2]))

import logging
log = logging.getLogger("gc3libs")
