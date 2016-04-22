#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012-2013, GC3, University of Zurich. All rights reserved.
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


import os
import gc3libs
from gc3libs.cmdline import SessionBasedDaemon
from gc3libs.workflow import SequentialTaskCollection
import inotifyx


class ParticleLocator(gc3libs.Application):
    application = 'plocator'
    
    def __init__(self, videofile, **extra):
        videofilename = os.path.basename(videofile)
        extra['jobname'] = "%s.%s" % (self.application, extra['videoname'])
        extra['output_dir'] = os.path.join(extra['base_output_dir'], self.application)
        
        self.ijoutbase = extra['videoname']+'.ijout.txt'
        outputfile = os.path.join('data',
                                  '2-particle',
                                  self.ijoutbase)
        gc3libs.Application.__init__(
            self,
            arguments = ["./plocator.sh", "locator"],
            inputs = {
                'plocator.sh': 'plocator.sh',
                'ParticleLocator.R': 'ParticleLocator.R',
                videofile: os.path.join('data', '1-raw', videofilename)},
            outputs = {outputfile : self.ijoutbase},
            stdout = 'plocator.log',
            join = True,
            **extra)

    @property
    def ijout(self):
        try:
            return os.path.join(self.output_dir, self.ijoutbase)
        except AttributeError:
            return ""

class ParticleLinker(gc3libs.Application):
    application = 'plinker'

    def __init__(self, particlefile, **extra):
        extra['jobname'] = "%s.%s" % (self.application, particlefile.replace('.ijout.txt', ''))
        extra['output_dir'] = os.path.join(extra['base_output_dir'], self.application)

        ijoutname = os.path.basename(particlefile)

        gc3libs.Application.__init__(
            self,
            arguments = ["Rscript", "ParticleLocator.R", "linker"],
            inputs = {
                particlefile: os.path.join("data",
                                           "2-particle",
                                           os.path.basename(particlefile)),
                "ParticleLocator.R": "ParticleLocator.R",
            },
            outputs = {os.path.join('data',
                                    '3-trajectory',
                                    'ParticleLinker_' + ijoutname)},
            stdout = 'plinker.log',
            join = True,
            **extra)
        
class BemoviWorkflow(SequentialTaskCollection):
    def __init__(self, videofile, **extra):
        self.videofile = videofile
        videofilename = os.path.basename(videofile)
        videoname = videofilename.rsplit('.', 1)[0]

        self.extra = extra
        extra['jobname'] = 'BemoviWorkflow_%s' % videoname
        extra['videoname'] = videoname
        extra['base_output_dir'] = extra['output_dir'].replace('NAME', extra['jobname'])

        plocator = ParticleLocator(videofile, **extra)

        SequentialTaskCollection.__init__(self, [plocator], **extra)

    def next(self, done):
        app = self.tasks[done]
        if isinstance(app, ParticleLocator):
            extra = self.extra.copy()
            plinker = ParticleLinker(app.ijout, **extra)
            self.add(plinker)
            return gc3libs.Run.State.RUNNING
        else:
            # We are probably done
            return gc3libs.Run.State.TERMINATED
    

class GBemoviDaemon(SessionBasedDaemon):
    """Daemon to run bemovi workflow"""
    version = '1.0'

    def new_tasks(self, extra, epath=None, emask=0):
        if not epath:
            # At startup we don't create any app.
            return []

        if not emask & inotifyx.IN_CLOSE_WRITE:
            # Only trigger a new run if a new file has been written
            return []

        return [BemoviWorkflow(epath, **self.extra)]

if "__main__" == __name__:
    from gbemovi import GBemoviDaemon
    GBemoviDaemon().run()
