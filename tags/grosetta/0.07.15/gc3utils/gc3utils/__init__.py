#!/usr/bin/python

# NG's default packages install arclib into /opt/nordugrid/lib/pythonX.Y/site-packages;
# add this anyway in case users did not set their PYTHONPATH correctly
import sys
sys.path.append('/opt/nordugrid/lib/python%d.%d/site-packages' 
                % sys.version_info[:2])

import logging
log = logging.getLogger("gc3utils")
