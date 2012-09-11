#! /usr/bin/env python
#
"""
  Minimal example to illustrate global optimization using gc3pie. 
  
  This example is meant as a starting point for other optimizations
  within the gc3pie framework. 
"""

# Copyright (C) 2011, 2012 University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
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
__version__ = '$Revision$'
__author__ = 'Benjamin Jonen <benjamin.jonen@bf.uzh.ch>'
# summary of user-visible changes
__changelog__ = """

"""
__docformat__ = 'reStructuredText'

import os


def compute_target_rosenbrock(pop_location_tuple):
    '''
      Given a list of (population, location), compute and return list of target 
      values. 
    '''
    fxVals = []
    for (pop, loc) in pop_location_tuple:
        outputDir = os.path.join(loc, 'output')
        f = open(os.path.join(outputDir, 'rosenbrock.out'))
        line = f.readline().strip()
        fxVal = float(line)
        fxVals.append(fxVal)
    return fxVals


if __name__ == '__main__':
    import sys
    import gc3libs
    import time    

    print 'Starting: \n%s' % ' '.join(sys.argv)
    # clean up
    os.system('rm -r /tmp/rosenbrock')

    # create an instance globalObt
    from gc3libs.optimizer.global_opt import GlobalOptimizer
    globalOptObj = GlobalOptimizer(y_crit=0.1)
    app = globalOptObj
    
    # create an instance of Core. Read configuration from your default
    # configuration file
    cfg = gc3libs.config.Configuration(*gc3libs.Default.CONFIG_FILE_LOCATIONS,
                                       **{'auto_enable_auth': True})
    g = gc3libs.core.Core(cfg)
    engine = gc3libs.core.Engine(g)
    engine.add(app)
    
    # Periodically check the status of your application.
    while app.execution.state != gc3libs.Run.State.TERMINATED:
        try:
            print "Job in status %s " % app.execution.state
            time.sleep(5)
            engine.progress()
        except:
            raise
    
    print "Job is now in state %s. Fetching output." % app.execution.state
    
    print 'main done'