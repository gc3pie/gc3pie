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

import ConfigParser
import os
import gc3libs
from gc3libs.cmdline import SessionBasedDaemon
from gc3libs.workflow import SequentialTaskCollection
import inotifyx
import time
import csv


class ParticleLocator(gc3libs.Application):
    application = 'plocator'

    def __init__(self, videofile, **extra):
        videofilename = os.path.basename(videofile)
        extra['jobname'] = "%s.%s" % (self.application, extra['videoname'])
        extra['output_dir'] = os.path.join(extra['base_output_dir'], self.application)

        scriptdir = os.path.dirname(__file__)

        self.ijoutbase = extra['videoname']+'.ijout.txt'
        outputfile = os.path.join('data',
                                  '2-particle',
                                  self.ijoutbase)
        rdatafile = os.path.join('data',
                                 '2-particle',
                                 'particle.RData')

        self.fps = extra['rparams']['fps']
        self.pixel_to_scale = extra['rparams']['pixel_to_scale']
        self.difference_lag = extra['rparams']['difference_lag']
        self.threshold1 = extra['rparams']['threshold1']
        self.threshold2 = extra['rparams']['threshold2']

        extra_params = [
            extra['rparams']['memory'],
            extra['rparams']['fps'],
            extra['rparams']['pixel_to_scale'],
            extra['rparams']['difference_lag'],
            extra['rparams']['threshold1'],
            extra['rparams']['threshold2'],
        ]
        gc3libs.Application.__init__(
            self,
            arguments = ["./plocator.sh", "locator"] + extra_params,
            inputs = {
                os.path.join(scriptdir, 'plocator.sh'): 'plocator.sh',
                os.path.join(scriptdir, 'bemovi.R'): 'bemovi.R',
                videofile: os.path.join('data', '1-raw', videofilename)},
            outputs = {outputfile : self.ijoutbase,
                       rdatafile: 'particle.RData'},
            stdout = 'plocator.log',
            join = True,
            **extra)


class ParticleLinker(gc3libs.Application):
    application = 'plinker'

    def __init__(self, particlefile, **extra):
        ijoutname = os.path.basename(particlefile)
        extra['jobname'] = "%s.%s" % (self.application, extra['videoname'])
        extra['output_dir'] = os.path.join(extra['base_output_dir'], self.application)
        scriptdir = os.path.dirname(__file__)

        self.fps = extra['rparams']['fps']
        self.pixel_to_scale = extra['rparams']['pixel_to_scale']
        self.difference_lag = extra['rparams']['difference_lag']
        self.threshold1 = extra['rparams']['threshold1']
        self.threshold2 = extra['rparams']['threshold2']

        extra_params = [
            extra['rparams']['memory'],
            extra['rparams']['fps'],
            extra['rparams']['pixel_to_scale'],
            extra['rparams']['difference_lag'],
            extra['rparams']['threshold1'],
            extra['rparams']['threshold2'],
        ]
        gc3libs.Application.__init__(
            self,
            arguments = ["./plocator.sh", "linker"] + extra_params,
            inputs = {
                particlefile: os.path.join("data",
                                           "2-particle",
                                           os.path.basename(particlefile)),
                os.path.join(scriptdir, 'plocator.sh'): 'plocator.sh',
                os.path.join(scriptdir, 'bemovi.R'): 'bemovi.R',
            },
            outputs = {os.path.join('data',
                                    '3-trajectory',
                                    'ParticleLinker_' + ijoutname),
                       os.path.join('data',
                                    '3-trajectory',
                                    'trajectory.RData')},
            stdout = 'plinker.log',
            join = True,
            **extra)

class BemoviWorkflow(SequentialTaskCollection):
    def __init__(self, videofile,  **extra):
        self.videofile = videofile
        videofilename = os.path.basename(videofile)
        inboxdir = os.path.dirname(videofile)
        # Look for a configuration file in the inboxdir to override
        # bemovi parameters
        cfgfile = os.path.join(inboxdir, 'gbemovi.conf')
        cfg = ConfigParser.RawConfigParser(defaults=extra['rparams'])
        try:
            cfg.read(cfgfile)
            gc3libs.log.debug("Reading configuration file %s for video file %s",
                              cfgfile, videofile)
            extra['rparams'].update(cfg.defaults())
            if videofilename in cfg.sections():
                for key in cfg.options(videofilename):
                    extra['rparams'][key] = cfg.get(videofilename, key)

        except Exception as ex:
            gc3libs.log.warning("Error while reading configuration file %s: %s. Ignoring",
                             cfgfile, ex)

        # As requested by Frank and Owne, also provide the ability to
        # read a CSV file
        csvcfgfile = os.path.join(inboxdir, 'gbemovi.csv')
        csvdata = {}
        try:
            with open(csvcfgfile) as fd:
                gc3libs.log.debug(
                    "Reading CSV configuration file %s for video file %s",
                    csvcfgfile, videofile)
                cr = csv.reader(fd)
                for line in cr:
                    if len(line) != 6:
                        gc3libs.log.warning(
                            "Ignoring line '%s' in csv configuration file %s",
                            line, csvcfgfile)
                    elif line[0] in csvdata:
                        gc3libs.log.warning(
                            "Ignoring dupliacate key in '%s' csv configuration file: '%s'",
                            csvcfgfile, line[0])
                    elif line[0].lower() == "default" or line[0] == videofilename:
                        gc3libs.log.debug(
                            "Matching line '%s' inc CSV config file %s for videofile %s",
                            line, csvcfgfile, videofilename)
                        extra['rparams']['fps'] = line[1]
                        extra['rparams']['pixel_to_scale'] = line[2]
                        extra['rparams']['difference_lag'] = line[3]
                        extra['rparams']['threshold1'] = line[4]
                        extra['rparams']['threshold2'] = line[5]
        except IOError:
            # File not found, ignore
            pass
        except Exception as ex:
            gc3libs.log.warning(
                "Error while reading CSV configuration file %s: Ignoring."
                " Error was: %s",
                csvcfgfile, ex)
        videoname = videofilename.rsplit('.', 1)[0]

        self.extra = extra
        extra['jobname'] = 'BemoviWorkflow_%s' % videoname
        extra['videoname'] = videoname

        extra['base_output_dir'] = os.path.join(
            inboxdir + '.out',
            extra['jobname'])

        plocator = ParticleLocator(videofile, **extra)
        plinker = ParticleLinker(
            os.path.join(extra['base_output_dir'],
                         ParticleLocator.application,
                         extra['videoname'] + '.ijout.txt',),
            **extra)
        SequentialTaskCollection.__init__(self, [plocator, plinker], **extra)


class GBemoviDaemon(SessionBasedDaemon):
    """Daemon to run bemovi workflow"""
    version = '1.0'

    def setup_options(self):
        self.add_param('--fps', default='25', help="Video FPS. Default: %(default)s")
        self.add_param('--pixel-to-scale', default='1000/240', help="Default: %(default)s")
        self.add_param('--difference-lag', default='10', help="Default: %(default)s")
        self.add_param('--threshold1', default='5', help="Default: %(default)s")
        self.add_param('--threshold2', default='255', help="Default: %(default)s")
        self.add_param('--valid-extensions', default='avi,cxd,raw',
                       help="Comma separated list of valid extensions for video file. Files ending with an extension non listed here will be ignored. Default: %(default)s")

    def setup_args(self):
        SessionBasedDaemon.setup_args(self)
        self.actions['notify_state'].default = "CLOSE_WRITE,CREATE"

    def parse_args(self):
        self.valid_extensions = [i.strip() for i in self.params.valid_extensions.split(',')]

        if not self.params.inbox:
            # Add a default inbox if not passed from command line
            self.params.inbox = [os.path.join(self.params.working_dir, 'inbox')]
        if self.params.output == self.actions['output'].default:
            # Use directory 'output' as output directory by default
            self.params.output = os.path.join(self.params.working_dir, 'output')

    def before_main_loop(self):
        # Setup new command
        self.comm.server.register_function(self.merge_data, "merge")

    def merge_data(self):
        # Scan all the output directories. Find the plocator.Rdata
        # files, add a merger for each input directory.
        scriptdir = os.path.dirname(__file__)
        msg = ""
        for inbox in self.params.inbox:
            # The corresponding directory ends with '.out'
            outdir = inbox + '.out'
            # Walk and find any `particle.RData` file, then submit an app
            infiles = {}
            index = 0
            for dirpath, dirnames, fnames in os.walk(outdir):
                # Stop when we have `plinker` and `plocator` directory
                if 'plinker' in dirnames and 'plocator' in dirnames:
                    pdata = os.path.join(dirpath, 'plocator', 'particle.RData')
                    tdata = os.path.join(dirpath, 'plinker', 'trajectory.RData')
                    if not os.path.exists(pdata) or not os.path.exists(tdata):
                        self.log.warning("Ignoring output directory %s as either the trajectory or the particle files are missing", dirpath)
                    else:
                        infiles[pdata] = "data/2-particle/particle-%05d.RData" % index
                        infiles[tdata] = "data/3-trajectory/trajectory-%05d.RData" % index
                        index += 1
            if infiles:
                # We also need a video description file. This is
                # likely to be in the outdir directory
                vdescrfile = os.path.join(inbox, "video.description.txt")
                infiles[vdescrfile] = "data/1-raw/video.description.txt"
                infiles[os.path.join(scriptdir, "plocator.sh")] = "plocator.sh"
                infiles[os.path.join(scriptdir, 'bemovi.R')] = 'bemovi.R'

                extra = self.extra.copy()
                extra['jobname'] = "Merger_%s" % time.strftime("%Y-%m-%d_%H.%M", time.localtime())
                extra['output_dir'] = os.path.join(outdir, extra['jobname'])
                # FIXME: Add the output
                app = gc3libs.Application(
                    arguments = ["./plocator.sh", "merger"],
                    inputs = infiles,
                    outputs = {"data/5-merged/Master.RData": "Master.RData"},
                    # outputs = gc3libs.ANY_OUTPUT,
                    stdout = 'merger.log',
                    join = True,
                    **extra)
                self._controller.add(app)
                self.session.add(app)
                # FIXME: Submit merger application
                return "Merging data from %d input videos" % index
        return "No data to merge"

    def new_tasks(self, extra, epath=None, emask=0):
        extra['rparams'] = {
            'memory': str(self.params.memory_per_core.amount(unit=gc3libs.quantity.MB)),
            'fps': self.params.fps,
            'pixel_to_scale': self.params.pixel_to_scale,
            'difference_lag': self.params.difference_lag,
            'threshold1': self.params.threshold1,
            'threshold2': self.params.threshold2,
        }

        if not epath:
            # At startup, scan all the input directories and check if
            # there is a file which is not processed yet.

            # First, check which files we already did
            known_videos = [i.get('videofile', None) for i in self.session]
            new_jobs = []
            for inbox in self.params.inbox:
                for dirpath, dirnames, fnames in os.walk(inbox):
                    for fname in fnames:
                        filename = os.path.join(dirpath, fname)
                        if filename.rsplit('.', 1)[-1] not in self.valid_extensions:
                            continue
                        if filename.startswith('._'):
                            self.log.warning("Ignoring file %s as it starts with '._'", filename)
                            continue
                        if filename not in known_videos:
                            new_jobs.append(BemoviWorkflow(filename, **extra))
                            known_videos.append(filename)


            return new_jobs

        # FIXME: for some reason emask & IN_CREATE & IN_ISDIR does not
        # work as expected. Using os.path.isdir() instead
        if emask & inotifyx.IN_CREATE and os.path.isdir(epath):
            # Creation of a directory or a file.  For each directory,
            # we need to add a new inotify watch to allow users to
            # organize files into directories.
            if epath.endswith('.out'):
                self.log.warning("NOT adding directory %s to the list of input folders as it looks like an OUTPUT folder!" % epath)
            else:
                self.log.debug("Adding directory %s to the list of input folders" % epath)
                self.add_inotify_watch(epath)

        elif emask & inotifyx.IN_CLOSE_WRITE:
            if epath.rsplit('.', 1)[-1] not in self.valid_extensions:
                self.log.info("Ignoring file %s as it does not end with a valid extension (%s)",
                              epath, str.join(',', self.valid_extensions))
                return []

            return [BemoviWorkflow(epath, **extra)]
        return []


if "__main__" == __name__:
    from gbemovi import GBemoviDaemon
    GBemoviDaemon().run()
