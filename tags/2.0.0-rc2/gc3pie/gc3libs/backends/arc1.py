#! /usr/bin/env python
#
"""
Job control using ``libarcclient``.  (Which can submit to all
EMI-supported resources.)
"""
# Copyright (C) 2009-2012 GC3, University of Zurich. All rights reserved.
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
__version__ = '2.0.0-rc2 version (SVN $Revision$)'


import sys

import itertools
import os
import shutil
import time
import tempfile

sys.path.append('/usr/share/pyshared')
try:
    import arc
    have_arc_module = True
except ImportError:
    have_arc_module = False

from gc3libs import log, Run
from gc3libs.backends import LRMS
import gc3libs.exceptions
from gc3libs.quantity import kB, GB, MB, hours, minutes, seconds
from gc3libs.utils import *



class Arc1Lrms(LRMS):
    """
    Manage jobs through ARC's ``libarcclient``.
    """
    arc_logger_set = False

    def __init__(self, name,
                 # this are inherited from the base LRMS class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime, auth,
                 # these are specific to the ARC0 backend
                 arc_ldap,
                 frontend=None,
                 lost_job_timeout=gc3libs.Default.ARC_LOST_JOB_TIMEOUT,
                 **extra_args):

        log.warning(
            "The ARC1 backend (used in resource '%s') is deprecated"
            " and will be removed in a future release."
            " Consider changing your configuration.",
            name)

        # check if arc module has been imported
        if not have_arc_module:
            raise gc3libs.exceptions.LRMSError(
                "Could not import `arc` module, disable ARC1 resources.")

        # init base class
        LRMS.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth)

        # ARC1-specific setup
        self.lost_job_timeout = lost_job_timeout
        self.arc_ldap = arc_ldap
        if frontend is None:
            if self.arc_ldap is not None:
                # extract frontend information from arc_ldap entry
                try:
                    resource_url = gc3libs.url.Url(arc_ldap)
                    self.frontend = resource_url.hostname
                except Exception, err:
                    raise gc3libs.exceptions.ConfigurationError(
                        "Configuration error: resource '%s' has no valid 'arc_ldap' setting: %s: %s"
                        % (name, err.__class__.__name__, err.message))
            else:
                self.frontend = None

        # XXX: do we need a way to setup non-default UserConfig?
        self._usercfg = arc.UserConfig("", "")
        self._usercfg.ClearSelectedServices()

        # setting default service timeout
        # XXX: should this be configurable?
        self._usercfg.Timeout(gc3libs.Default.ARC1_DEFAULT_SERVICE_TIMEOUT)

        gc3libs.log.debug("Adding ARC1 service %s for resource '%s' ...",
                          self.arc_ldap, self.name)

        service, arc_version, ldap_host_endpoint = self.arc_ldap.split(':',2)
        if service == "INDEX":
            # add index service
            if not self._usercfg.AddServices(["%s:%s" % (arc_version,ldap_host_endpoint)], arc.INDEX):
                gc3libs.log.error("Could not add INDEX service '%s'", ldap_host_endpoint)
        elif service == "COMPUTING":
            # add computing service
            if not self._usercfg.AddServices(["%s:%s" % (arc_version, ldap_host_endpoint)], arc.COMPUTING):
                gc3libs.log.error(
                    "Could not add COMPUTING service '%s:%s'", arc_version, ldap_host_endpoint)
        else:
            gc3libs.log.error(
                "Unknown ARC Service type '%s'. Valid prefixes are: 'INDEX' and 'COMPUTING'.",
                self.arc_ldap)

        # XXX: have to check whether and how to handle the arc
        # logging: shall we simply disable it?  some internal logging
        # information could be useful for debugging as the new arc
        # libraries do not provide exceptions nor detailed information
        # about failures...

        # set up libarcclient logging
        gc3libs.backends.arc1.Arc1Lrms.init_arc_logger()

        # DEBUG: use the following to enable fully verbose ARC1 logging:
        # arc_rootlogger = arc.Logger_getRootLogger()
        # arc_logger = arc.Logger(arc_rootlogger, self.name)
        # arc_logger_dest = arc.LogStream(sys.stderr) # or open(os.devnull, 'w')
        # arc_rootlogger.addDestination(arc_logger_dest)
        # arc_rootlogger.setThreshold(arc.DEBUG) # or .VERBOSE, .INFO, .WARNING, .ERROR

        # # Initialize the required ARC1 components
        # log.debug('Invoking arc.JobSupervisor')
        # self._jobsupervisor = arc.JobSupervisor(self._usercfg, [])
        # # XXX: we need to get what 'middleware' each controller can control
        # log.debug('Invoking arc.JobSupervisor.GetJobControllers')
        # self._controllers = self._jobsupervisor.GetJobControllers()
        # # XXX: can we also create the target ?
        # log.debug('Invoking arc.TargetGenerator')
        # self._target_generator = arc.TargetGenerator(self._usercfg, 0)

        gc3libs.log.info("ARC1 resource '%s' init: OK", self.name)


    def _get_JobSupervisor_and_JobController(self):
        # Initialize the required ARC1 components
        log.debug('Invoking arc.JobSupervisor')
        self._jobsupervisor = arc.JobSupervisor(self._usercfg, [])
        # XXX: we need to get what 'middleware' each controller can control
        log.debug('Invoking arc.JobSupervisor.GetJobControllers')
        self._controllers = self._jobsupervisor.GetJobControllers()
        # XXX: can we also create the target ?
        log.debug('Invoking arc.TargetGenerator')
        self._target_generator = arc.TargetGenerator(self._usercfg, 0)




    @same_docstring_as(LRMS.cancel_job)
    @LRMS.authenticated
    def cancel_job(self, app):
        controller, job = self._get_job_and_controller(app.execution.lrms_jobid)
        try:
            log.debug("Calling arc.JobController.Cancel(job)")
            if not controller.CancelJob(job):
                raise gc3libs.exceptions.LRMSError('arc.JobController.Cancel returned False')
        except Exception, ex:
            gc3libs.log.error('Failed while killing job. Error type %s, message %s' % (ex.__class__,str(ex)))
            raise gc3libs.exceptions.LRMSError('Failed while killing job. Error type %s, message %s' % (ex.__class__,str(ex)))




    # ARC refreshes the InfoSys every 30 seconds by default;
    # there's no point in querying it more often than this...
    @cache_for(gc3libs.Default.ARC_CACHE_TIME)
    def _get_targets(self):
        """
        Wrapper around `arc.TargetGenerator.GetTargets()`.
        """
        # tg = arc.TargetGenerator(self._usercfg, 1)
        # return tg.FoundTargets()
        # This methodd should spawn the ldapsearch to update the ExecutionTager information
        log.debug('Calling arc.TargetGenerator.RetrieveExecutionTargets')

        self._get_JobSupervisor_and_JobController()

        self._target_generator.RetrieveExecutionTargets()

        log.debug('Calling arc.TargetGenerator.GetExecutionTargets()')
        return self._target_generator.GetExecutionTargets()
        # execution_targets = self._target_generator.GetExecutionTargets()
        # for target in execution_targets:
        #     self._execution_targets{target.GridFlavour} = target
        # return self._execution_targets


    # ARC refreshes the InfoSys every 30 seconds by default;
    # there's no point in querying it more often than this...
    # @cache_for(gc3libs.Default.ARC_CACHE_TIME)
    @cache_for(60)
    def _iterjobs(self):
        """
        Iterate over all jobs.
        """

        self._get_JobSupervisor_and_JobController()

        for c in self._controllers:
            log.debug("Calling JobController.GetJobInformation() ...")
            c.GetJobInformation()
            log.debug('... controller returned %d jobs' % len(c.GetJobs()))
        return itertools.chain(* [c.GetJobs() for c in self._controllers])


    def _get_job(self, jobid):
        """Return the ARC `Job` object given its ID (connection URL)."""
        for job in self._iterjobs():
            if job.JobID.str() == jobid:
                return job
        raise IndexError("No job with ID '%s'" % jobid)


    def _get_job_and_controller(self, jobid):
        """
        Return a pair `(c, j)` where `j` is the `arc.Job` object
        corresponding to the given `jobid` and `c` is the
        corresponding `arc.JobController`.
        """

        """
        jobmaster = arc.JobSupervisor(usercfg, []);
        jobcontrollers = jobmaster.GetJobControllers();
        """

        self._iterjobs()

        for c in self._controllers:
            log.debug("Calling JobController.GetJobs in get_job_and_controller")
            jl = c.GetJobs()
            for j in jl:
                if j.JobID.str() == jobid:
                    # found, clean remote job sessiondir
                    return (c, j)
        raise KeyError("No job found with job ID '%s'" % jobid)


    @same_docstring_as(LRMS.submit_job)
    @LRMS.authenticated
    def submit_job(self, app):
        job = app.execution

        # Initialize xrsl
        xrsl = app.xrsl(self)
        log.debug("Application provided XRSL: %s" % xrsl)

        jd = arc.JobDescription()
        jobdesclang = "nordugrid:xrsl"
        log.debug("Calling arc.JobDescription.Parse")
        arc.JobDescription.Parse(jd, xrsl, jobdesclang)
        # JobDescription::Parse(const std::string&, std::list<JobDescription>&, const std::string&, const std::string&) method instead.


        # perform brokering
        log.debug("Calling arc.BrokerLoader")
        ld = arc.BrokerLoader()
        broker = ld.load("Random", self._usercfg)
        # broker.PreFilterTargets(tg.GetExecutionTargets(), jd)
        log.debug("Calling arc.Broker.PreFilterTargets")
        broker.PreFilterTargets(self._get_targets(), jd)

        submitted = False
        tried = 0
        j = arc.Job()
        while True:
            log.debug("Calling arc.Broker.GetBestTarget")
            target = broker.GetBestTarget()
            if not target:
                break
            log.debug("Calling arc.ExecutionTarget.Submit")
            submitted = target.Submit(self._usercfg, jd, j)
            if not submitted:
                continue
            # XXX: this is necessary as the other component of arc library seems to refer to the job.xml file
            # hopefully will be fixed soon
            j.WriteJobsToFile(gc3libs.Default.ARC_JOBLIST_LOCATION, [j])
            # save job ID for future reference
            job.lrms_jobid = j.JobID.str()
            # state is known at this point, so mark this as a successful update
            job._arc1_state_last_checked = time.time()
            return jd

        if not submitted and tried == 0:
            raise gc3libs.exceptions.LRMSSubmitError('No ARC targets found')


    @staticmethod
    def _map_arc1_status_to_gc3pie_state(status):
        """
        Return the GC3Pie state corresponding to the given ARC status.

        See `update_job_state`:meth: for a complete table of the
        correspondence.

        :param status: ARC's `arc.JobState` object.

        :raise gc3libs.exceptions.UnknownJobState: If there is no
        mapping of `status` to a GC3Pie state.
        """
        # we cannot use a dictionary lookup because the `arc.JobState`
        # objects are not strings and won't compare equal in
        # dictionary key lookup... (bug in ARC1 SWIG wrappers?)
        if arc.JobState.ACCEPTED == status:
            return Run.State.SUBMITTED
        elif arc.JobState.PREPARING == status:
            return Run.State.SUBMITTED
        elif arc.JobState.SUBMITTING == status:
            return Run.State.SUBMITTED
        elif arc.JobState.QUEUING == status:
            return Run.State.SUBMITTED
        elif arc.JobState.RUNNING == status:
            return Run.State.RUNNING
        elif arc.JobState.FINISHING == status:
            return Run.State.RUNNING
        elif arc.JobState.FINISHED == status:
            return Run.State.TERMINATING
        elif arc.JobState.FAILED == status:
            return Run.State.TERMINATING
        elif arc.JobState.KILLED == status:
            return Run.State.TERMINATED
        elif arc.JobState.DELETED == status:
            return Run.State.TERMINATED
        elif arc.JobState.HOLD == status:
            return Run.State.STOPPED
        elif arc.JobState.OTHER == status:
            return Run.State.UNKNOWN
        elif arc.JobState.UNDEFINED == status:
            return Run.State.UNKNOWN
        else:
            raise gc3libs.exceptions.UnknownJobState(
                "Unknown ARC1 job state '%s'" % status.GetGeneralState())


    # ARC refreshes the InfoSys every 30 seconds by default;
    # there's no point in querying it more often than this...
    @cache_for(gc3libs.Default.ARC_CACHE_TIME)
    @LRMS.authenticated
    def update_job_state(self, app):
        """
        Query the state of the ARC job associated with `app` and
        update `app.execution.state` accordingly.  Return the
        corresponding `Run.State`; see `Run.State` for more details.

        The mapping of ARC job statuses to `Run.State` is as follows:

                    ==============  ===========
                    ARC job status  `Run.State`
                    ==============  ===========
                    ACCEPTED        SUBMITTED
                    SUBMITTING      SUBMITTED
                    PREPARING       SUBMITTED
                    QUEUING         SUBMITTED
                    RUNNING         RUNNING
                    FINISHING       RUNNING
                    FINISHED        TERMINATING
                    FAILED          TERMINATING
                    KILLED          TERMINATED
                    DELETED         TERMINATED
                    HOLD            STOPPED
                    OTHER           UNKNOWN
                    ==============  ===========

        Any other ARC job status is mapped to `Run.State.UNKNOWN`.
        """
        job = app.execution

        # try to intercept error conditions and translate them into
        # meaningful exceptions
        try:
            arc_job = self._get_job(job.lrms_jobid)
        except AttributeError, ex:
            # `job` has no `lrms_jobid`: object is invalid
            raise gc3libs.exceptions.InvalidArgument(
                "Job object is invalid: %s" % str(ex))
        except IndexError, ex:
            # No job found.  This could be caused by the
            # information system not yet updated with the information
            # of the newly submitted job.
            if not hasattr(job, 'state_last_changed'):
                # XXX: compatibility with running sessions, remove before release
                job.state_last_changed = time.time()
            if not hasattr(job, '_arc1_state_last_checked'):
                # XXX: compatibility with running sessions, remove before release
                job._arc1_state_last_checked = time.time()
            now = time.time()
            if (job.state == Run.State.SUBMITTED
                and now - job.state_last_changed < self.lost_job_timeout):
                # assume the job was recently submitted, hence the
                # information system knows nothing about it; just
                # ignore the error and return the object unchanged
                return job.state
            elif (job.state in [ Run.State.SUBMITTED, Run.State.RUNNING ]
                  and now - job._arc1_state_last_checked < self.lost_job_timeout):
                # assume transient information system failure;
                # ignore the error and return object unchanged
                return job.state
            else:
                raise  gc3libs.exceptions.UnknownJob(
                    "No job found corresponding to the ID '%s'" % job.lrms_jobid)
        job._arc1_state_last_checked = time.time()

        # update status
        state = self._map_arc1_status_to_gc3pie_state(arc_job.State)
        log.debug("ARC1 job status '%s' mapped to GC3Pie state '%s'",
                  arc_job.State.GetGeneralState(), state)
        if arc_job.ExitCode != -1:
            job.returncode = gc3libs.Run.shellexit_to_returncode(arc_job.ExitCode)
        elif state in [Run.State.TERMINATING, Run.State.TERMINATING] and job.returncode is None:
            # XXX: it seems that ARC does not report the job exit code
            # (at least in some cases); let's make one up based on
            # some crude heuristics
            if len(arc_job.Error) > 0:
                job.log("ARC reported error: %s" % str.join(arc_job.Error))
                job.returncode = (Run.Signals.RemoteError, -1)
            # FIXME: we should introduce a kind of "wrong requirements" error
            elif (arc_job.RequestedTotalWallTime is not None
                  and arc_job.RequestedTotalWallTime.GetPeriod() != -1
                  and arc_job.UsedTotalWallTime.GetPeriod() != -1
                  and arc_job.UsedTotalWallTime.GetPeriod() > arc_job.RequestedTotalWallTime.GetPeriod()):
                job.log("Job exceeded requested wall-clock time (%d s),"
                        " killed by remote batch system"
                        % arc_job.RequestedTotalWallTime.GetPeriod())
                job.returncode = (Run.Signals.RemoteError, -1)
            elif (arc_job.RequestedTotalCPUTime is not None
                  and arc_job.RequestedTotalCPUTime.GetPeriod() != -1
                  and arc_job.UsedTotalCPUTime.GetPeriod() != -1
                  and arc_job.UsedTotalCPUTime.GetPeriod() > arc_job.RequestedTotalCPUTime.GetPeriod()):
                job.log("Job exceeded requested CPU time (%d s),"
                        " killed by remote batch system"
                        % arc_job.RequestedTotalWallTime.GetPeriod())
                job.returncode = (Run.Signals.RemoteError, -1)
            # note: arc_job.used_memory is in KiB (!)
            elif (app.requested_memory is not None
                  and arc_job.UsedMainMemory > -1
                  and arc_job.UsedMainMemory > app.requested_memory.amount(kB)):
                job.log("Job used more memory (%d MB) than requested (%s),"
                        " killed by remote batch system"
                        % (arc_job.UsedMainMemory / 1024, app.requested_memory.amount(MB)))
                job.returncode = (Run.Signals.RemoteError, -1)
            else:
                # presume everything went well...
                job.returncode = (0, 0)
            # pass

        # common job reporting info, see Issue #78 and `Task.update_state`
        job.duration = gc3libs.utils.ifelse(arc_job.UsedTotalWallTime.GetPeriod() != -1,
                                            arc_job.UsedTotalWallTime.GetPeriod() * seconds,
                                            None)
        job.max_used_memory = gc3libs.utils.ifelse(arc_job.UsedMainMemory != -1,
                                                   arc_job.UsedMainMemory * kB,
                                                   None)
        job.used_cpu_time = gc3libs.utils.ifelse(arc_job.UsedTotalCPUTime.GetPeriod() != -1,
                                                 arc_job.UsedTotalCPUTime.GetPeriod() * seconds,
                                                 None)

        # additional info
        job.arc_original_exitcode = arc_job.ExitCode
        job.arc_jobname = arc_job.Name

        job.state = state
        return state


    @same_docstring_as(LRMS.get_results)
    @LRMS.authenticated
    def get_results(self, app, download_dir, overwrite=False):
        jobid = app.execution.lrms_jobid

        # XXX: can raise encoding/decoding error if `download_dir`
        # is not ASCII, but the ARClib bindings don't accept
        # Python `unicode` strings.
        download_dir = str(download_dir)

        c, j = self._get_job_and_controller(jobid)

        # as ARC complains when downloading to an already-existing
        # directory, make a temporary directory for downloading files;
        # then move files to their final destination and delete the
        # temporary location.
        tmp_download_dir = tempfile.mkdtemp(suffix='.d', dir=download_dir)

        log.debug("Downloading %s output into temporary location '%s' ...", app, tmp_download_dir)

        # Get a list of downloadable files
        download_file_list = c.GetDownloadFiles(j.JobID);

        source_url = arc.URL(j.JobID.str())
        destination_url = arc.URL(tmp_download_dir)

        source_path_prefix = source_url.Path()
        destination_path_prefix = destination_url.Path()

        errors = 0
        for remote_file in download_file_list:
            source_url.ChangePath(os.path.join(source_path_prefix,remote_file))
            destination_url.ChangePath(os.path.join(destination_path_prefix,remote_file))
            if not c.ARCCopyFile(source_url,destination_url):
                log.warning("Failed downloading '%s' to '%s'",
                            source_url.str(), destination_url.str())
                errors += 1
        if errors > 0:
            # remove temporary download location
            shutil.rmtree(tmp_download_dir, ignore_errors=True)
            raise gc3libs.exceptions.UnrecoverableDataStagingError(
                "Failed downloading remote folder of job '%s' into '%s'."
                " There were %d errors, reported at the WARNING level in log files."
                % (jobid, download_dir, errors))

        log.debug("Moving %s output into download location '%s' ...", app, download_dir)
        entries = os.listdir(tmp_download_dir)
        if not overwrite:
            # raise an early error before we start mixing files from
            # the old and new download directories
            for entry in entries:
                dst = os.path.join(download_dir, entry)
                if os.path.exists(entry):
                    # remove temporary download location
                    shutil.rmtree(tmp_download_dir, ignore_errors=True)
                    raise gc3libs.exceptions.UnrecoverableDataStagingError(
                        "Entry '%s' in download directory '%s' already exists,"
                        " and no overwriting was requested."
                        % (entry, download_dir))
        # move all entries to the final destination
        for entry in entries:
            src = os.path.join(tmp_download_dir, entry)
            dst = os.path.join(download_dir, entry)
            if os.path.isdir(dst):
                shutil.rmtree(dst)
            os.rename(src, dst)

        # remove temporary download location (XXX: is it correct to ignore errors here?)
        shutil.rmtree(tmp_download_dir, ignore_errors=True)

        app.execution.download_dir = download_dir
        return


    @same_docstring_as(LRMS.free)
    @LRMS.authenticated
    def free(self, app):
        controller, job = self._get_job_and_controller(app.execution.lrms_jobid)
        log.debug("Calling JobController.CleanJob")
        if not controller.CleanJob(job):
            log.error("arc1.JobController.CleanJob returned False for ARC job ID '%s'",
                      app.execution.lrms_jobid)
        # XXX: this is necessary as the other component of arc library seems to refer to the job.xml file
        # remove Job from job.xml file
        log.debug("Removing job '%s' from jobfile '%s'",
                  app, gc3libs.Default.ARC_JOBLIST_LOCATION)
        job.RemoveJobsFromFile(gc3libs.Default.ARC_JOBLIST_LOCATION, [job.IDFromEndpoint])


    @cache_for(gc3libs.Default.ARC_CACHE_TIME)
    @LRMS.authenticated
    def get_resource_status(self):
        """
        Get dynamic information from the ARC infosystem and set
        attributes on the current object accordingly.

        The following attributes are set:

        * total_queued
        * free_slots
        * user_running
        * user_queued
        """
        total_queued = 0
        free_slots = 0
        user_running = 0
        user_queued = 0

        targets = self._get_targets()

        def cap_to_0(n):
            if n < 0:
                return 0
            else:
                return n

        for t in targets:
            # total_queued
            total_queued += (
                cap_to_0(t.PreLRMSWaitingJobs)
                + cap_to_0(t.WaitingJobs)
                + cap_to_0(t.RunningJobs)
                + cap_to_0(t.StagingJobs)
                + cap_to_0(t.SuspendedJobs)
                )

            # free_slots
            free_slots += min(cap_to_0(t.FreeSlots),
                              cap_to_0(t.TotalSlots - t.UsedSlots))

        # user_running and user_queued
        for job in self._iterjobs():
            if job.State == arc.JobState.RUNNING:
                user_running = user_running + 1
            elif job.State == arc.JobState.QUEUING:
                user_queued = user_queued + 1

        self.queued = total_queued
        self.free_slots = free_slots
        self.user_queued = user_queued
        self.user_run = user_running
        self.used_quota = -1

        log.debug("Updated resource '%s' status:"
                          " free slots: %d,"
                          " own running jobs: %d,"
                          " own queued jobs: %d,"
                          " total queued jobs: %d",
                          self.name,
                          self.free_slots,
                          self.user_run,
                          self.user_queued,
                          self.queued,
                          )
        return self


    @same_docstring_as(LRMS.peek)
    @LRMS.authenticated
    def peek(self, app, remote_filename, local_file, offset=0, size=None):

        job = app.execution

        assert job.has_key('lrms_jobid'), \
            "Missing attribute `lrms_jobid` on `Job` instance passed to `ArcLrms.peek`."

        controller, j = self._get_job_and_controller(job.lrms_jobid)

        if size is None:
            size = sys.maxint

        # `local_file` could be a file name (string) or a file-like
        # object, as per function docstring; ensure `local_file_name`
        # is the local path
        try:
           local_file_name = local_file.name
        except AttributeError:
           local_file_name = local_file

        # `local_file` could be a file name (string) or a file-like
        # object, as per function docstring; ensure `local_file_name`
        # is the local path
        try:
            local_file_name = local_file.name
        except AttributeError:
            local_file_name = local_file

        source_url = arc.URL(job.lrms_jobid + '/' + remote_filename)
        destination_url = arc.URL(local_file_name)

        # download file
        log.debug("Arc1Lrms.peek(): Downloading remote file '%s' into local file '%s' ..."
                  % (remote_filename, local_file_name))
        if not controller.ARCCopyFile(source_url, destination_url):
            log.warning("Failed downloading '%s' to '%s'"
                        % (source_url.str(), destination_url.str()))
        log.debug("Arc1LRMS.peek(): arc.JobController.ARCCopyFile: completed")


    @same_docstring_as(LRMS.validate_data)
    def validate_data(self, data_file_list):
        """
        Supported protocols: file, gsiftp, srm, http, https
        """
        for url in data_file_list:
            log.debug("Resource %s: checking URL '%s' ..." % (self.name, url))
            if not url.scheme in ['srm', 'lfc', 'file', 'http', 'gsiftp', 'https']:
                return False
        return True


    @staticmethod
    def init_arc_logger():
        if not gc3libs.backends.arc1.Arc1Lrms.arc_logger_set:
            arc_rootlogger = arc.Logger_getRootLogger()
            arc_logger = arc.Logger(arc_rootlogger, "gc3pie")
            # arc_logger_dest = arc.LogStream(sys.stderr) # or open(os.devnull, 'w')
            arc_logger_dest = arc.LogStream(open(gc3libs.Default.ARC1_LOGFILE, 'w'))
            arc_rootlogger.addDestination(arc_logger_dest)
            arc_rootlogger.setThreshold(arc.DEBUG) # or .VERBOSE, .INFO, .WARNING, .ERROR
            gc3libs.backends.arc1.Arc1Lrms.arc_logger_set = True

    @same_docstring_as(LRMS.validate_data)
    def close(self):
        pass

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="arc",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
