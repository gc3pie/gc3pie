#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012-2013,  University of Zurich. All rights reserved.
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

from __future__ import absolute_import, print_function
import csv
import logging
import os
import pwd
import smtplib
import socket
import time

from collections import defaultdict
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import gc3libs
import gc3libs.poller as plr
from gc3libs.cmdline import SessionBasedDaemon
from gc3libs.workflow import SequentialTaskCollection
from gc3libs import Run
from gc3libs.url import Url
from gc3libs.quantity import Duration

log = logging.getLogger('gc3.gc3utils')


class BemoviCSVConf(object):
    def __init__(self, path, default_walltime='3h'):
        self.path = path
        self.cfg = {}
        self.defaults = {}
        with open(path, 'rU') as fd:
            log.debug("Reading CSV configuration file %s", path)
            cr = csv.reader(fd)
            lineno = 0
            for line in cr:
                lineno += 1
                if len(line) != 9:
                    log.warning("Ignoring line '%d' in csv configuration file %s: wrong number of fields (%d != 9)",
                    lineno, path, len(line))
                    continue
                if line[0] in self.cfg:
                    log.warning(
                        "Overwriting dupliacate key in '%s' csv configuration file: '%s'",
                        csvcfgfile, line[0])

                try:
                    # Check if this is an header line. These values
                    # should always be integers.
                    int(line[1])
                    int(line[3])
                    int(line[4])
                    int(line[5])
                except ValueError:
                    log.debug("Ignoring line '%d' of file %s, some values do not convert to integer as expected.",
                              lineno, path)
                    continue
                data = {
                    'fps': line[1],
                    'pixel_to_scale': line[2],
                    'difference_lag': line[3],
                    'threshold1': line[4],
                    'threshold2': line[5],
                    'video_is_needed': False if line[7].lower() == 'optional' else True,
                    'email_to': line[8],
                }
                try:
                    data['requested_walltime'] = Duration(line[6])
                except ValueError as ex:
                    log.error("Unable to parse walltime '%s' for key %s in file"
                              " %s: %s. Using default value of %s",
                              line[6], line[0], path, ex, default_walltime)
                    data['requested_walltime'] = Duration(default_walltime)
                key = line[0]
                if key.lower() == "default":
                    self.defaults = data
                else:
                    self.cfg[key] = data

    def get(self, key):
        """Return configuration key for `path` entry, or default value (if any"""
        if key in self.cfg:
            return self.cfg[key].copy()
        elif os.path.basename(key) in self.cfg:
            return self.cfg[os.path.basename(key)].copy()
        else:
            return self.defaults.copy()

    def keys(self):
        return ['default'] + self.cfg.keys()

    def __iter__(self):
        return iter(self.keys())

class BemoviApp(gc3libs.Application):
    def update_configuration(self, extra):
        self.rparams.update(extra['rparams'])
        self.requested_walltime = extra['requested_walltime']
        extra_params = [
            self.rparams['memory'],
            self.rparams['fps'],
            self.rparams['pixel_to_scale'],
            self.rparams['difference_lag'],
            self.rparams['threshold1'],
            self.rparams['threshold2'],
        ]
        self.arguments[2:] = extra_params


class ParticleLocator(BemoviApp):
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
        self.rdatafile = os.path.join(extra['output_dir'],
                                      'particle.RData')
        self.logfile = os.path.join(extra['output_dir'],
                                    'plocator.log')
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


class ParticleLinker(BemoviApp):
    application = 'plinker'

    def __init__(self, particlefile, **extra):
        ijoutname = os.path.basename(particlefile)
        extra['jobname'] = "%s.%s" % (self.application, extra['videoname'])
        extra['output_dir'] = os.path.join(extra['base_output_dir'], self.application)
        scriptdir = os.path.dirname(__file__)
        rdatafile = os.path.join('data',
                                 '3-trajectory',
                                 'trajectory.RData')
        self.rdatafile = os.path.join(extra['output_dir'],
                                      'trajectory.RData')
        self.logfile = os.path.join(extra['output_dir'],
                                    'plinker.log')
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
                       rdatafile},
            stdout = 'plinker.log',
            join = True,
            **extra)


class BemoviWorkflow(SequentialTaskCollection):
    appname = 'bemoviworkflow'
    def __init__(self, videofile, email_from, smtp_server, **extra):
        self.videofile = videofile
        videofilename = os.path.basename(videofile)

        inboxdir = os.path.dirname(videofile)
        self.vdescrfile = os.path.join(inboxdir, 'video.description.txt')
        self.inboxdir = inboxdir
        self.email_to = ''
        self.email_from = email_from
        self.smtp_server = smtp_server
        self.video_is_needed = True

        # Look for a 'configuration' file in the inboxdir to override
        # bemovi parameters

        videoname = videofilename.rsplit('.', 1)[0]

        self.extra = extra
        extra['jobname'] = 'BemoviWorkflow_%s' % videoname
        extra['videoname'] = videoname

        extra['base_output_dir'] = os.path.join(
            inboxdir + '.out',
            extra['jobname'])

        self.update_configuration()

        plocator = ParticleLocator(videofile, **extra)
        plinker = ParticleLinker(
            os.path.join(extra['base_output_dir'],
                         ParticleLocator.application,
                         extra['videoname'] + '.ijout.txt',),
            **extra)
        SequentialTaskCollection.__init__(self, [plocator, plinker], **extra)

    def update_configuration(self):
        csvcfgfile = os.path.join(self.inboxdir, 'gbemovi.csv')
        try:
            cfg = BemoviCSVConf(csvcfgfile).get(self.videofile)
            self.extra['rparams']['fps'] = cfg.get('fps')
            self.extra['rparams']['pixel_to_scale'] = cfg.get('pixel_to_scale')
            self.extra['rparams']['difference_lag'] = cfg.get('difference_lag')
            self.extra['rparams']['threshold1'] = cfg.get('threshold1')
            self.extra['rparams']['threshold2'] = cfg.get('threshold2')
            self.extra['requested_walltime'] = cfg.get('requested_walltime')
            self.video_is_needed = cfg.get('video_is_needed')
            self.email_to = cfg.get('email_to')
            for task in self.tasks:
                task.update_configuration(self.extra)
        except IOError:
            pass
        except Exception as ex:
            log.warning(
                "Error while reading CSV configuration file %s: Ignoring."
                " Error was: %s",
                csvcfgfile, ex)


    def should_resubmit(self):
        """Return True or False if the job should be resubmitted or not"""
        for task in self.tasks:
            if task.execution.state == Run.State.TERMINATED and \
               task.execution.exitcode != 0:
                return True
        return False

    def _send_notification(self, subject, body, attachments=None, debug=False):
        log.info("Sending notification to %s with subject \"%s\".", self.email_to, subject)
        if debug:
            log.debug("Subject: \"%s\"", subject)
            log.debug("Body: \"%s\"", body)
        else:
            try:
                msg = MIMEMultipart()
                msg['Subject'] = subject
                msg['From'] = self.email_from
                msg['To'] = self.email_to
                msg.attach(MIMEText(body))
                if attachments:
                    for attachment in attachments:
                        with open(attachment) as fd:
                            msg.attach(
                                MIMEApplication(fd.read(),
                                                name=attachment)
                            )
                s = smtplib.SMTP(self.smtp_server)
                s.sendmail(self.email_from, [self.email_to], msg.as_string())
                s.quit()
                log.info("Successfully sent email to %s", self.email_to)
            except Exception as ex:
                log.error("Error while sending an email to %s via %s: %s",
                          self.email_to, self.smtp_server, ex)


    def terminated(self):
        """Check if the processing went fine, otherwise send an email."""
        plocator, plinker = self.tasks
        if plocator.execution.exitcode != 0 or \
           plinker.execution.exitcode != 0:
            # Send notification

            # path of self.videofile should be relative to the inbox dir.
            shortvideofile = self.videofile.replace(os.path.dirname(self.inboxdir), '<inbox>')
            subject = "GBemovi: Failed processing file %s" % shortvideofile
            msg = """File {} failed processing.
ParticleLocator exited with status {}
ParticleLinker exited with status {}
""".format(shortvideofile,
           plocator.execution.exitcode,
           plinker.execution.exitcode)
            self._send_notification(
                subject,
                msg,
                attachments=[plocator.logfile,
                             plinker.logfile]
            )


class Merger(gc3libs.Application):
    appname = "merger"
    def __init__(self, vdescrfile, inputs, **extra):
        # Rename files in destination folder
        scriptdir = os.path.dirname(__file__)
        self.vdescrfile = vdescrfile
        self.inboxdir = os.path.dirname(vdescrfile)
        infiles = {
            os.path.join(scriptdir, 'plocator.sh'): 'plocator.sh',
            os.path.join(scriptdir, 'bemovi.R'): 'bemovi.R',
            vdescrfile: 'data/1-raw/video.description.txt',
        }
        for index, (ldata, tdata) in enumerate(inputs):
            infiles[ldata] = 'data/2-particle/particle-%05d.RData' % index
            infiles[tdata] = 'data/3-trajectory/trajectory-%05d.RData' % index
        extra['jobname'] = "Merger_%d_%s" % (index, time.strftime("%Y-%m-%d_%H.%M", time.localtime()))
        extra['output_dir'] = os.path.join(self.inboxdir + '.out', extra['jobname'])
        gc3libs.Application.__init__(
            self,
            arguments = ["./plocator.sh", "merger"],
            inputs = infiles,
            outputs = {"data/5-merged/Master.RData": "Master.RData"},
            # outputs = gc3libs.ANY_OUTPUT,
            stdout = 'merger.log',
            join = True,
            **extra)


class FinalMerger(Merger):
    """This application is exactly like Merger, but when it's terminated
    it will delete its input files"""
    appname = "final_merger"
    def __init__(self, vdescrfile, tasks, email_from, email_to, smtp_server, stats, session, **extra):
        self.email_from = email_from
        self.email_to = email_to
        self.smtp_server = smtp_server
        self.stats = stats
        self.videotasks = tasks
        self.videofiles = [t.videofile for t in tasks]
        self.session = session
        inputs = []
        for task in tasks:
            plinker, ptracker = task.tasks
            if plinker.execution.exitcode == ptracker.execution.exitcode == 0:
                inputs.append((plinker.rdatafile, ptracker.rdatafile))
        Merger.__init__(self, vdescrfile, inputs, **extra)

    def terminated(self):
        # if terminated with success, delete input files and send
        # notification
        if self.execution.exitcode != 0:
            # nothing to do
            del self.videotasks
            return
        try:
            text = """Report from experiment in inbox {}

Path to merged file: {}

Files processed:
{}

Statistics:
{}
""".format(os.path.dirname(self.vdescrfile),
           os.path.join(self.output_dir, 'merger.log'),
           str.join('\n', self.videofiles),
           self.stats)
            msg = MIMEText(text)
            msg['Subject'] = "GBemovi: Experiment in %s ended successfully." % os.path.dirname(self.vdescrfile)
            msg['From'] = self.email_from
            msg['To'] = str.join(', ', self.email_to)
            s = smtplib.SMTP(self.smtp_server)
            s.sendmail(self.email_from, self.email_to, msg.as_string())
            s.quit()
            log.info("Successfully sent email to %s", str.join(', ', self.email_to))
        except Exception as ex:
            log.error("Error while sending an email to %s via %s: %s",
                      self.email_to, self.smtp_server, ex)

        for videofile in self.videofiles:
            try:
                os.remove(videofile)
                log.info("Removed input file %s", videofile)
            except Exception as ex:
                log.warning("Ignorinig error while removing input file %s: %s",
                            videofile, ex)

        for task in self.videotasks:
            try:
                self.session.remove(task.persistent_id)
                task._controller.remove(task)
            except Exception as ex:
                log.warning("Ignoring error while removing task %s from session: %s", task.jobname, ex)
        # This is useful when the daemon is restarted...
        del self.videotasks

class GBemoviDaemon(SessionBasedDaemon):
    """Daemon to run bemovi workflow"""
    version = '1.0'

    def setup_options(self):
        self.add_param(
            '--fps',
            default='25',
            help="Framerate of the video in seconds (frame per second). Default: %(default)s")
        self.add_param(
            '--pixel-to-scale',
            default='1000/240',
            help="A conversion factor to scale videos (in pixel) to real"
            " dimensions (e.g.  micrometer). The conversion factor needs to"
            " be determined by the experimenter by measuring an object of"
            " known size (e.g. micrometer) with the used microscope/video"
            " settings (magnification, resolution etc.)."
            " Default: %(default)s")
        self.add_param(
            '--difference-lag',
            default='10',
            help="Lag between two frames, the former being the target frame"
            " to be segmented, the latter the frame subtracted from the former"
            " to create the difference image. Lag is specified in frames, but"
            " can be converted into time (difference-lag of 25 of a video"
            " taken with 25 fps translates into a difference-lag of 1 second)."
            " Default: %(default)s")
        self.add_param(
            '--threshold1',
            default='5',
            help="Threshold applied to the difference image. Threshold has to"
            " be in the range of 0 to 255. The lower the threshold, the more"
            " greyish pixels will be considered when the image is binarized."
            " Default: %(default)s")
        self.add_param(
            '--threshold2',
            default='255',
            help="Upper threshold applied to the difference image. Value of"
            " `--threshold2` has to be larger than `--threshold1` (lower"
            " threshold). Default: %(default)s")
        self.add_param(
            '--valid-extensions',
            default='avi,cxd,raw',
            help="Comma separated list of valid extensions for video file."
            " Files ending with an extension non listed here will be ignored."
            " Default: %(default)s")
        hostname = socket.gethostname()
        user = pwd.getpwuid(os.getuid()).pw_name
        email_from = '%s+gbemovi@%s' % (user, hostname)

        self.add_param(
            '--email-from',
            default=email_from,
            help="Email to use when sending notifications. Default: %(default)s")
        self.add_param(
            '--smtp-server',
            default='localhost',
            help="SMTP server to use to send notifications. Default: %(default)s")

    def setup_args(self):
        SessionBasedDaemon.setup_args(self)
        self.actions['notify_state'].default = "CLOSE_WRITE,CREATE"

    def parse_args(self):
        self.valid_extensions = [i.strip() for i in self.params.valid_extensions.split(',')]

        if not self.params.inbox:
            # Add a default inbox if not passed from command line
            self.params.inbox = [Url(os.path.join(self.params.working_dir, 'inbox'))]
        if self.params.output == self.actions['output'].default:
            # Use directory 'output' as output directory by default
            self.params.output = os.path.join(self.params.working_dir, 'output')

        if int(self.params.threshold2) <= int(self.params.threshold1):
            gc3libs.exceptions.InvalidUsage(
                "Value of `--threshold2` should be greater than `--threshold1`")
    def before_main_loop(self):
        # Setup new command
        self.comm.server.register_function(self.merge_data, "merge")
        self.comm.server.register_function(self.cleanup_session, "cleanup")

    def merge_data(self):
        # Scan all the output directories. Find the plocator.Rdata
        # files, add a merger for each input directory.
        scriptdir = os.path.dirname(__file__)

        # Walk through our jobs, group "experiments" and run a merger for each of them
        mergers = defaultdict(list)
        newapps = []
        for task in self.session:
            try:
                if task.appname != 'bemoviworkflow':
                    continue
            except AttributeError:
                # Some class might not have appname attribute
                continue
            if task.execution.state != Run.State.TERMINATED:
                # only merge completed jobs
                continue
            # FIXME: We should also avoid running the merger twice for
            # the same job

            # find plinker radata files
            plinker = task.tasks[0]
            ptracker = task.tasks[1]
            # If linker or tracker failed, skip the rdata file, as
            # it's likely to be empty and bemove will not like it.
            if plinker.execution.exitcode != 0 or \
               ptracker.execution.exitcode != 0:
                continue
            linkerdata = plinker.rdatafile
            trackerdata = ptracker.rdatafile
            mergers[task.vdescrfile].append((linkerdata, trackerdata))

        for vdescrfile, inputs in mergers.items():
            if not os.path.exists(vdescrfile):
                self.log.warning("Ignoring inbox directory %s as there is no video.description.txt file", os.path.dirname(vdescrfile))
                continue
            app = Merger(vdescrfile, inputs, **self.extra)
            newapps.append(app)
            self._controller.add(app)
            self.session.add(app)
            # FIXME: Submit merger application
            self.log.info("Creating Merger application from %d input videos" % len(inputs))

        msg = []
        for app in newapps:
            msg.append("Merger application created from directory %s" % app.inboxdir)
        if msg:
            return "Merging data for %d experiments.\n%s" % (len(msg), str.join('\n', msg))

    def cleanup_session(self):
        # for each experiment:
        #   => run "final" merger
        #   => delete input files
        #   => cleanup session
        #   => send report ("Experiment XXX final report")
        #
        # This is implemented via the FinalMerger application, which
        # is similar to the Merger application but is also responsible
        # for deleting the input files and sending a report after
        # termination.

        # group running jobs by their vdescrfile attribute
        experiments = defaultdict(list)
        not_completed = set()
        newapps = []
        for task in self.session:
            try:
                if task.appname != 'bemoviworkflow':
                    continue
            except AttributeError:
                continue
            if task.execution.state != Run.State.TERMINATED:
                continue
            experiments[task.vdescrfile].append(task)

        # Now we must filter non-completed experiments. There are two kinds:
        # 1) experiments with mandatory video files that failed
        # 2) experiments with files not yet processed.
        #
        # not_completed currently contains jobs of the first kind
        for vdescrfile, tasks in experiments.items():
            inboxdir = os.path.dirname(vdescrfile)
            try:
                bemovicfg = BemoviCSVConf(os.path.join(inboxdir, 'gbemovi.csv'))
            except Exception as ex:
                # Ignoring
                self.log.error(
                    "Unable to run FinalMerger application when gbemovi.csv is"
                    " missing or invalid. Ignoring directory %s: %s", inboxdir, ex)
                not_completed.add(vdescrfile)
                continue
            for task in tasks:
                if task.should_resubmit():
                    # We could access task.video_is_needed but the csv
                    # file might have been changed in the meantime.
                    if bemovicfg.get(task.videofile).get('video_is_needed'):
                        # Cannot merge this, experiment is not finished.
                        not_completed.add(task.vdescrfile)
                        continue
            if vdescrfile in not_completed:
                self.log.warning("Ignoring directory %s: some mandatory video were not correctly processed.", inboxdir)
                continue

            if os.path.exists(vdescrfile):
                # We need now to check if all the videos in the
                # experiment were processed
                videofiles = [os.path.basename(t.videofile) for t in tasks]
                fd = open(vdescrfile)
                data = fd.read().replace('\r', '\n')
                fd.close()
                # Note: we ignore the header line
                descriptions = [i.split()[0] for i in data.splitlines()[1:]]

                # Descriptions could miss the extension.
                intersection = set(descriptions).intersection(videofiles)
                fileext = ''
                if not intersection:
                    # description file are probably missing the extension.
                    for descr in descriptions:
                        for key in videofiles:
                            if key.startswith(descr):
                                fileext = key[len(descr):]
                                break
                        if fileext:
                            break
                descriptions = [i+fileext for i in descriptions]

                # Now we know the experiment is complete iff
                # descriptions is exactly the list of videofiles.
                if set(descriptions) != set(videofiles):
                    self.log.info("Inbox %s not yet completed",
                                  os.path.dirname(vdescrfile))
                    continue

                self.log.info("Creating FinalMerger for inbox %s",
                              os.path.dirname(vdescrfile))
                inputs = []
                email_to = set()
                stats = []
                for task in tasks:
                    plinker, ptracker = task.tasks
                    email_to.add(bemovicfg.get(task.videofile).get('email_to'))
                    stats.append("""Video file {}
  Linker
    duration: {}
    exitcode: {}

  Tracker
    duration: {}
    exitcode: {}
""".format(task.videofile,
           plinker.execution.duration, plinker.execution.exitcode,
           ptracker.execution.duration, ptracker.execution.exitcode))

                app = FinalMerger(vdescrfile,
                                  tasks,
                                  self.params.email_from,
                                  email_to,
                                  self.params.smtp_server,
                                  str.join('\n', stats),
                                  self.session,
                                  **self.extra)
                newapps.append(app)
                self._controller.add(app)
                self.session.add(app)
        if newapps:
            return "Running FinalMerger for the following inboxes:\n%s" % str.join('\n', [app.inboxdir for app in newapps])
        else:
            return "No experiment is done yet"

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
            known_videos = {i.get('videofile', None): i for i in self.session}
            new_jobs = []
            for inboxurl in self.params.inbox:
                # WARNING: we assume this is a filesystem directory
                try:
                    inbox = inboxurl.path
                except:
                    inboxurl = Url(inboxurl)
                    inbox = inboxurl.path
                for dirpath, dirnames, fnames in os.walk(inbox):
                    for fname in fnames:
                        filename = os.path.join(dirpath, fname)
                        if filename.rsplit('.', 1)[-1] not in self.valid_extensions:
                            continue
                        if filename.startswith('._'):
                            self.log.warning("Ignoring file %s as it starts with '._'", filename)
                            continue
                        if filename not in known_videos:
                            app = BemoviWorkflow(filename,
                                                 self.params.email_from,
                                                 self.params.smtp_server,
                                                 **extra)
                            new_jobs.append(app)
                            known_videos[filename] = app
                        else:
                            # In case it exists, but the application
                            # termianted with an exit code, then we
                            # want to resubmit the job anyway
                            job = known_videos[filename]
                            if job.should_resubmit():
                                self.log.info("File %s might have been overwritten. Resubmitting job %s",
                                              filename, job.persistent_id)
                                # self._controller.kill(job)
                                # self._controller.progress()
                                # self._controller.redo(job, from_stage=0)
            return new_jobs

        fpath = epath.path
        if emask & plr.events['IN_CLOSE_WRITE']:
            if fpath.rsplit('.', 1)[-1] not in self.valid_extensions:
                self.log.info("Ignoring file %s as it does not end with a valid extension (%s)",
                              fpath, str.join(',', self.valid_extensions))
                return []

            # Only resubmit the job if it failed
            for job in self.session.tasks.values():
                try:
                    job.videofile
                except AttributeError:
                    # Not a GBemoviWorkflow application
                    continue
                if job.videofile == fpath:
                    if job.should_resubmit():
                        self.log.info("Re-submitting job %s as file %s has been overwritten",
                                      job.persistent_id, fpath)
                        job.update_configuration()
                        self._controller.kill(job)
                        self._controller.progress()
                        self._controller.redo(job, from_stage=0)

                    else:
                        self.log.info("Ignoring already successfully processed file %s", fpath)
                    # In both case, do not return any new job
                    return []
            return [BemoviWorkflow(fpath,
                                   self.params.email_from,
                                   self.params.smtp_server,
                                   **extra)]
        return []


if "__main__" == __name__:
    from gbemovi import GBemoviDaemon
    GBemoviDaemon().run()
