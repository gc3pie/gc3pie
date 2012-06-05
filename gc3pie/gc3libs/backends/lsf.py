#! /usr/bin/env python
#
"""
Job control on SGE clusters (possibly connecting to the front-end via SSH).
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
__version__ = 'development version (SVN $Revision$)'


import os
import posixpath
import random
import re
import sys
import tempfile
import time

from gc3libs.compat.collections import defaultdict

from gc3libs import log, Run
from gc3libs.backends import LRMS
import gc3libs.exceptions
import gc3libs.utils as utils # first, to_bytes
from gc3libs.utils import *

import transport


# Examples of LSF commands output used to build this backend:
# $ bsub -W 00:10 -n 1 -R "rusage[mem=1800]" < script.sh
# Generic job.
# Job <473713> is submitted to queue <pub.1h>.
#
# where:
#   -W HH:MM is the wall-clock time
#   -n is the number of CPUs
#   -R sets resource limits
# note: script must be fed as STDIN
#
# [gloessa@brutus2 test_queue]$ bjobs 473713
# JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
# 473713  gloessa PEND  pub.1h     brutus2                 TM-T       Oct 19 17:10
#
# $ bjobs 473713
# JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
# 473713  gloessa RUN   pub.1h     brutus2     a6128       TM-T       Oct 19 17:10
# [gloessa@brutus2 test_queue]$ bjobs -l 473713
#
# Job <473713>, Job Name <TM-T>, User <gloessa>, Project <default>, Status <RUN>,
#                       Queue <pub.1h>, Job Priority <50>, Command <#!/bin/sh;#;#
#                      ;echo 'sequential test job';echo $OMP_NUM_THREADS;echo '$T
#                      URBODIR: '$TURBODIR;sysname;which dscf;echo '$TMPDIR: '$TM
#                      PDIR; echo 'parallel test job';export OMP_NUM_THREADS=2;ec
#                      ho $OMP_NUM_THREADS;echo '$TURBODIR: '$TURBODIR;sysname;wh
#                      ich dscf;echo '$TMPDIR: '$TMPDIR; sleep 300>, Share group
#                      charged </lsf_cfour/gloessa>
# Wed Oct 19 17:10:27: Submitted from host <brutus2>, CWD <$HOME/test_queue>, Out
#                      put File <lsf.o%J>, Requested Resources <order[-r1m] span[
#                      ptile=1] same[model] rusage[mem=1800,xs=1]>, Specified Hos
#                      ts <thin+7>, <single+5>, <shared+3>, <parallel+1>;
#
#  RUNLIMIT
#  10.0 min of a6128
# Wed Oct 19 17:11:02: Started on <a6128>, Execution Home </cluster/home/chab/glo
#                      essa>, Execution CWD </cluster/home/chab/gloessa/test_queu
#                      e>;
# Wed Oct 19 17:12:10: Resource usage collected.
#                      MEM: 5 Mbytes;  SWAP: 201 Mbytes;  NTHREAD: 5
#                      PGID: 23177;  PIDs: 23177 23178 23182 23259
#
#
#  SCHEDULING PARAMETERS:
#            r15s   r1m  r15m   ut      pg    io   ls    it    tmp    swp    mem
#  loadSched   -     -     -     -       -     -    -     -     -      -      -
#  loadStop    -     -     -     -       -     -    -     -     -      -      -
#
#           scratch      xs       s       m       l      xl      sp
#  loadSched     -       -       -       -       -       -       -
#  loadStop      -       -       -       -       -       -       -
#
# $ bkill 473713
# Job <473713> is being terminated
#
# $ bjobs -W -w 473713
# JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME  PROJ_NAME CPU_USED MEM SWAP PIDS START_TIME FINISH_TIME
# 473713  gloessa EXIT  pub.1h     brutus2     a6128       TM-T       10/19-17:10:27 default    000:00:00.12 5208   206312 23177,23178,23182,23259 10/19-17:11:02 10/19-17:14:49
#
# # STAT would be DONE if not killed
# $ bjobs 473713
# JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
# 473713  gloessa EXIT  pub.1h     brutus2     a6128       TM-T       Oct 19 17:10
#
# $ bacct 473713
#
# Accounting information about jobs that are:
#   - submitted by all users.
#   - accounted on all projects.
#   - completed normally or exited
#   - executed on all hosts.
#   - submitted to all queues.
#   - accounted on all service classes.
# ------------------------------------------------------------------------------
#
# SUMMARY:      ( time unit: second )
#  Total number of done jobs:       0      Total number of exited jobs:     1
#  Total CPU time consumed:       0.1      Average CPU time consumed:     0.1
#  Maximum CPU time of a job:     0.1      Minimum CPU time of a job:     0.1
#  Total wait time in queues:    35.0
#  Average wait time in queue:   35.0
#  Maximum wait time in queue:   35.0      Minimum wait time in queue:   35.0
#  Average turnaround time:       262 (seconds/job)
#  Maximum turnaround time:       262      Minimum turnaround time:       262
#  Average hog factor of a job:  0.00 ( cpu time / turnaround time )
#  Maximum hog factor of a job:  0.00      Minimum hog factor of a job:  0.00
#
# $ lsinfo
# RESOURCE_NAME   TYPE   ORDER  DESCRIPTION
# r15s          Numeric   Inc   15-second CPU run queue length
# r1m           Numeric   Inc   1-minute CPU run queue length (alias: cpu)
# r15m          Numeric   Inc   15-minute CPU run queue length
# ut            Numeric   Inc   1-minute CPU utilization (0.0 to 1.0)
# pg            Numeric   Inc   Paging rate (pages/second)
# io            Numeric   Inc   Disk IO rate (Kbytes/second)
# ls            Numeric   Inc   Number of login sessions (alias: login)
# it            Numeric   Dec   Idle time (minutes) (alias: idle)
# tmp           Numeric   Dec   Disk space in /tmp (Mbytes)
# swp           Numeric   Dec   Available swap space (Mbytes) (alias: swap)
# mem           Numeric   Dec   Available memory (Mbytes)
# scratch       Numeric   Dec   Disk space available in /scratch in Mbytes
# xs            Numeric   Dec   number of slots available for express jobs
# s             Numeric   Dec   number of slots available for 1h jobs
# m             Numeric   Dec   number of slots available for 8h jobs
# l             Numeric   Dec   number of slots available for 36h jobs
# xl            Numeric   Dec   number of slots available for 7d jobs
# sp            Numeric   Dec   number of slots available for unlimited jobs
# ncpus         Numeric   Dec   Number of CPUs
# ndisks        Numeric   Dec   Number of local disks
# maxmem        Numeric   Dec   Maximum memory (Mbytes)
# maxswp        Numeric   Dec   Maximum swap space (Mbytes)
# maxtmp        Numeric   Dec   Maximum /tmp space (Mbytes)
# cpuf          Numeric   Dec   CPU factor
# rexpri        Numeric   N/A   Remote execution priority
# openclversion Numeric   Dec   e.g. 1.1
# nprocs        Numeric   Dec   Number of physical processors
# ncores        Numeric   Dec   Number of cores per physical processor
# nthreads      Numeric   Dec   Number of threads per processor core
# server        Boolean   N/A   LSF server host
# LSF_Base      Boolean   N/A   Base product
# lsf_base      Boolean   N/A   Base product
# LSF_Manager   Boolean   N/A   LSF Manager product
# lsf_manager   Boolean   N/A   LSF Manager product
# LSF_JobSchedu Boolean   N/A   JobScheduler product
# lsf_js        Boolean   N/A   JobScheduler product
# LSF_Make      Boolean   N/A   Make product
# lsf_make      Boolean   N/A   Make product
# LSF_Parallel  Boolean   N/A   Parallel product
# lsf_parallel  Boolean   N/A   Parallel product
# LSF_Analyzer  Boolean   N/A   Analyzer product
# lsf_analyzer  Boolean   N/A   Analyzer product
# mips          Boolean   N/A   MIPS architecture
# sparc         Boolean   N/A   SUN SPARC
# hpux          Boolean   N/A   HP-UX UNIX
# aix           Boolean   N/A   AIX UNIX
# irix          Boolean   N/A   IRIX UNIX
# rms           Boolean   N/A   RMS
# pset          Boolean   N/A   PSET
# dist          Boolean   N/A   DIST
# slurm         Boolean   N/A   SLURM
# cpuset        Boolean   N/A   CPUSET
# solaris       Boolean   N/A   SUN SOLARIS
# fs            Boolean   N/A   File server
# cs            Boolean   N/A   Compute server
# frame         Boolean   N/A   Hosts with FrameMaker licence
# bigmem        Boolean   N/A   Hosts with very big memory
# diskless      Boolean   N/A   Diskless hosts
# alpha         Boolean   N/A   DEC alpha
# linux         Boolean   N/A   LINUX UNIX
# nt            Boolean   N/A   Windows NT
# mpich_gm      Boolean   N/A   MPICH GM MPI
# lammpi        Boolean   N/A   LAM MPI
# mpichp4       Boolean   N/A   MPICH P4 MPI
# mvapich       Boolean   N/A   Infiniband MPI
# sca_mpimon    Boolean   N/A   SCALI MPI
# ibmmpi        Boolean   N/A   IBM POE MPI
# hpmpi         Boolean   N/A   HP MPI
# sgimpi        Boolean   N/A   SGI MPI
# intelmpi      Boolean   N/A   Intel MPI
# crayxt3       Boolean   N/A   Cray XT3 MPI
# crayx1        Boolean   N/A   Cray X1 MPI
# mpich_mx      Boolean   N/A   MPICH MX MPI
# mpichsharemem Boolean   N/A   MPICH Shared Memory
# mpich2        Boolean   N/A   MPICH2
# openmpi       Boolean   N/A   OPENMPI
# Platform_HPC  Boolean   N/A   platform hpc license
# platform_hpc  Boolean   N/A   platform hpc license
# fluent        Boolean   N/A   fluent availability
# ls_dyna       Boolean   N/A   ls_dyna availability
# nastran       Boolean   N/A   nastran availability
# pvm           Boolean   N/A   pvm availability
# openmp        Boolean   N/A   openmp availability
# ansys         Boolean   N/A   ansys availability
# blast         Boolean   N/A   blast availability
# gaussian      Boolean   N/A   gaussian availability
# lion          Boolean   N/A   lion availability
# scitegic      Boolean   N/A   scitegic availability
# schroedinger  Boolean   N/A   schroedinger availability
# hmmer         Boolean   N/A   hmmer availability
# ib            Boolean   N/A   InfiniBand QDR
# panfs         Boolean   N/A   Panasas file system
# c4            Boolean   N/A   C4 node with huge memory and scratch
# lustre        Boolean   N/A   Lustre file system
# define_ncpus_ Boolean   N/A   ncpus := procs
# define_ncpus_ Boolean   N/A   ncpus := cores
# define_ncpus_ Boolean   N/A   ncpus := threads
# mg            Boolean   N/A   Management hosts
# bluegene      Boolean   N/A   BLUEGENE
# cuda          Boolean   N/A   nodes supporting Nvidia CUDA
# cuda3         Boolean   N/A   nodes supporting Nvidia CUDA
# opencl        Boolean   N/A   nodes supporting OpenCL
# opencl3       Boolean   N/A   nodes supporting OpenCL
# nas           Boolean   N/A   all NAS shares are available
# imsb          Boolean   N/A   nodes supporting IMSB-user NAS shares
# perfctr       Boolean   N/A   nodes exclusive for perfctr, no panfs running
# type           String   N/A   Host type
# model          String   N/A   Host model
# status         String   N/A   Host status
# hname          String   N/A   Host name
# gpuvendor      String   N/A   e.g. Nvidia, AMD
# gputype        String   N/A   e.g. Tesla, Fermi, FireStream
# gpumodel       String   N/A   e.g. C1060, M2050, 9350
# cudaversion   Numeric   Dec   e.g. 3.2
# lic_fluent    Numeric   Dec   fluent base licenses
# lic_fluent_pa Numeric   Dec   fluent parallel licenses
# lic_cfx       Numeric   Dec   cfx base licenses
# lic_cfx_par   Numeric   Dec   cfx parallel licenses
# aa_r          Numeric   Dec   ansys academic research licenses
# aa_r_hpc      Numeric   Dec   ansys academic hpc licenses
# msi_tokenr    Numeric   Dec   msi tokens
# gpu           Numeric   Dec   nodes with 1,2,3... GPUs
# ddadb         Numeric   Dec   number of connections that ddadb.ethz.ch can handle
#
# TYPE_NAME
# UNKNOWN_AUTO_DETECT
# DEFAULT
# DEFAULT
# CRAYJ
# CRAYC
# CRAYT
# CRAYSV1
# CRAYT3E
# CRAYX1
# DigitalUNIX
# ALPHA
# ALPHA5
# ALPHASC
# HPPA
# IBMAIX4
# IBMAIX532
# IBMAIX564
# LINUX
# LINUX2
# LINUXAXP
# LINUX86
# LINUXPPC
# LINUX64
# DLINUX
# DLINUX64
# DLINUXAXP
# SCYLD
# SLINUX
# SLINUX64
# NECSX4
# NECSX5
# NECSX6
# NECSX8
# NTX86
# NTX64
# NTIA64
# NTALPHA
# SGI5
# SGI6
# SUNSOL
# SOL732
# SOL64
# SGI64
# SGI65
# SGI658
# SOLX86
# SOLX8664
# HPPA11
# HPUXIA64
# MACOSX
# LNXS39032
# LNXS390X64
# LINUXPPC64
# BPROC
# BPROC4
# LINUX_ARM
# X86_64
# SX86_64
# IA64
# DIA64
# SIA64
#
# MODEL_NAME      CPU_FACTOR      ARCHITECTURE
# Opteron2216           8.00      x15_3604_AMDOpterontmProcessor2216
# Opteron2220           8.00      x15_3604_AMDOpterontmProcessor2220
# Opteron2435           8.20      x15_3604_AMDOpterontmProcessor2435
# Opteron8220           8.00      x15_3604_AMDOpterontmProcessor8220
# Opteron8380           7.50      x15_3604_AMDOpterontmProcessor8380
# Opteron8384           8.10      x15_3604_AMDOpterontmProcessor8384
# Opteron6174           7.00      x15_3604_AMDOpterontmProcessor6174
# [gloessa@brutus2 test_queue]$ lsid
# Platform LSF HPC 7 Update 6, Sep 04 2009
# Copyright 1992-2009 Platform Computing Corporation
#
# My cluster name is brutus
# My master name is hpcadm2


_bsub_jobid_re = re.compile(r'^Job <(?P<jobid>\d+)> is submitted', re.I)

def get_bsub_jobid(bsub_output):
    """Parse the ``bsub`` output for the local jobid."""
    for line in bsub_output.split('\n'):
        match = _bsub_jobid_re.match(line)
        if match:
            return match.group('jobid')
    raise gc3libs.exceptions.InternalError("Could not extract jobid from bsub output '%s'"
                        % bsub_output.rstrip())


def _make_remote_and_local_path_pair(transport, job, remote_relpath, local_root_dir, local_relpath):
    """
    Return list of (remote_path, local_path) pairs corresponding to
    """
    # see https://github.com/fabric/fabric/issues/306 about why it is
    # correct to use `posixpath.join` for remote paths (instead of `os.path.join`)
    remote_path = posixpath.join(job.ssh_remote_folder, remote_relpath)
    local_path = os.path.join(local_root_dir, local_relpath)
    if transport.isdir(remote_path):
        # recurse, accumulating results
        result = [ ]
        for entry in transport.listdir(remote_path):
            result += _make_remote_and_local_path_pair(
                transport, job,
                posixpath.join(remote_relpath, entry),
                local_path, entry)
        return result
    else:
        return [(remote_path, local_path)]



class LsfLrms(LRMS):
    """
    Job control on LSF clusters (possibly by connecting via SSH to a submit node).
    """
    def __init__(self, resource, auths):
        """
        Create an `LsfLRMS` instance from a `Resource` object.

        For a `Resource` object `r` to be a valid `SgeLRMS` construction
        parameter, the following conditions must be met:
          * `r.type` must have value `Default.SGE_LRMS`;
          * `r.frontend` must be a string, containing the FQDN of an SGE cluster submit node;
          * `r.auth` must be a valid key to pass to `Auth.get()`.
        """
        # XXX: should these be `InternalError` instead?
        assert resource.type == gc3libs.Default.LSF_LRMS, \
            "LsfLRMS.__init__(): Failed. Resource type expected 'lsf'. Received '%s'" \
            % resource.type

        # checking mandatory resource attributes
        resource.frontend
        resource.transport

        self._resource = resource

        # set defaults
        auth = auths.get(resource.auth)
        self._ssh_username = auth.username

        if resource.transport == 'local':
            self.transport = transport.LocalTransport()
        elif resource.transport == 'ssh':
            self.transport = transport.SshTransport(self._resource.frontend,
                                                    username=self._ssh_username)
        else:
            raise gc3libs.exceptions.TransportError("Unknown transport '%s'", resource.transport)

        # XXX: does Ssh really needs this ?
        self._resource.max_cores = int(self._resource.max_cores)
        self._resource.max_memory_per_core = int(self._resource.max_memory_per_core) * 1000
        self._resource.max_walltime = int(self._resource.max_walltime)
        if self._resource.max_walltime > 0:
            # Convert from hours to minutes
            self._resource.max_walltime = self._resource.max_walltime * 60

        self.isValid = 1


    def is_valid(self):
        return self.isValid


    @same_docstring_as(LRMS.submit_job)
    def submit_job(self, app):
        job = app.execution
        # Create the remote directory.
        try:
            self.transport.connect()

            _command = 'mkdir -p $HOME/.gc3pie_jobs; mktemp -p $HOME/.gc3pie_jobs -d lrms_job.XXXXXXXXXX'
            log.info("Creating remote temporary folder: command '%s' " % _command)
            exit_code, stdout, stderr = self.transport.execute_command(_command)
            if exit_code == 0:
                ssh_remote_folder = stdout.split('\n')[0]
            else:
                raise gc3libs.exceptions.LRMSError("Failed while executing command '%s' on resource '%s';"
                                " exit code: %d, stderr: '%s'."
                                % (_command, self._resource, exit_code, stderr))
        except gc3libs.exceptions.TransportError, x:
            raise
        # except:
        #     self.transport.close()
        #     raise

        # Copy the input file to remote directory.
        for local_path,remote_path in app.inputs.items():
            remote_path = os.path.join(ssh_remote_folder, remote_path)
            remote_parent = os.path.dirname(remote_path)
            try:
                if remote_parent not in ['', '.']:
                    log.debug("Making remote directory '%s'" % remote_parent)
                    self.transport.makedirs(remote_parent)
                log.debug("Transferring file '%s' to '%s'" % (local_path.path, remote_path))
                self.transport.put(local_path.path, remote_path)
                # preserve execute permission on input files
                if os.access(local_path.path, os.X_OK):
                    self.transport.chmod(remote_path, 0755)
            except:
                log.critical("Copying input file '%s' to remote cluster '%s' failed",
                                      local_path.path, self._resource.frontend)
                # self.transport.close()
                raise

        if app.executable.startswith('./'):
            gc3libs.log.debug("Making remote path '%s' executable.",
                              app.executable)
            self.transport.chmod(os.path.join(ssh_remote_folder,
                                              app.executable), 0755)

        try:
            # Try to submit it to the local queueing system.
            bsub, script = app.bsub(self._resource)
            if script is not None:
                # save script to a temporary file and submit that one instead
                local_script_file = tempfile.NamedTemporaryFile()
                local_script_file.write(script)
                local_script_file.flush()
                script_name = '%s.%x.sh' % (app.get('application_tag', 'script'),
                                            random.randint(0, sys.maxint))
                # upload script to remote location
                self.transport.put(local_script_file.name,
                                   os.path.join(ssh_remote_folder, script_name))
                # cleanup
                local_script_file.close()
                if os.path.exists(local_script_file.name):
                    os.unlink(local_script_file.name)
            # submit it
            exitcode, stdout, stderr = self.transport.execute_command(
                "/bin/sh -c 'cd %s && %s < %s'"
                % (ssh_remote_folder, bsub, script_name))
            jobid = get_bsub_jobid(stdout)
            log.debug('Job submitted with jobid: %s', jobid)
            # self.transport.close()

            job.lrms_jobid = jobid
            if app.has_key('stdout'):
                job.stdout_filename = app.stdout
            else:
                job.stdout_filename = '%s.o%s' % (jobname, jobid)
            if app.join:
                job.stderr_filename = job.stdout_filename
            else:
                if app.has_key('stderr'):
                    job.stderr_filename = app.stderr
                else:
                    job.stderr_filename = '%s.e%s' % (jobname, jobid)
            job.log.append('Submitted to LSF @ %s with jobid %s'
                           % (self._resource.name, jobid))
            job.log.append("LSF `bsub` output:\n"
                           "  === stdout ===\n%s"
                           "  === stderr ===\n%s"
                           "  === end ===\n"
                           % (stdout, stderr), 'lsf', 'bsub')
            job.ssh_remote_folder = ssh_remote_folder

            return job

        except:
            # self.transport.close()
            log.critical("Failure submitting job to resource '%s' - see log file for errors"
                                  % self._resource.name)
            raise


    @same_docstring_as(LRMS.update_job_state)
    def update_job_state(self, app):
        # check that passed object obeys contract
        try:
            job = app.execution
            job.lrms_jobid
        except AttributeError, ex:
            # `job` has no `lrms_jobid`: object is invalid
            raise gc3libs.exceptions.InvalidArgument("Job object is invalid: %s" % str(ex))

        def map_lsf_names_to_local_ones(name):
            return 'lsf_' + name

        try:
            self.transport.connect()

            # check the lrms_jobid with qstat
            _command = "bjobs -w -W %s" % job.lrms_jobid
            log.debug("checking remote job status with '%s'" % _command)
            exit_code, stdout, stderr = self.transport.execute_command(_command)
            if exit_code == 0 and stdout:
                # 1st line in STDOUT is header line, 2nd one is real info
                status_line = stdout.split('\n')[1]
                #
                fields = status_line.split()
                assert fields[0] == job.lrms_jobid, \
                       "First field in `bjobs` output is not JobID!"
                stat = fields[2]
                log.debug("translating LSF's `bjobs` STAT '%s' to gc3libs.Run.State" % stat)
                if 'PEND' == stat:
                    state = Run.State.SUBMITTED
                elif 'RUN' == stat:
                    return Run.State.RUNNING
                elif stat in [
                    'DONE', # successful termination
                    'EXIT'  # job was killed / exit forced
                    ]:
                    return Run.State.TERMINATING
                else:
                    log.warning("unknown LSF job status '%s', returning `UNKNOWN`", stat)
                    state = Run.State.UNKNOWN
            else:
                # only good test for job termination is the presence of the "lsf.o<JobID>"
                # file on the filesystem; it is created after the job has finished
                # XXX: is this systematic ? or depends on the LSF directives for stdout ?
                found = False
                for entry in self.transport.listdir(job.ssh_remote_folder):
                    if entry == app.stdout:
                        found = True
                        state = Run.State.TERMINATING
                        # XXX: assume returncode 0. need to
                        job.returncode = (0, 0)
                        break
                if not found:
                    # job was lost??
                    state = Run.State.UNKNOWN

        except Exception, ex:
            # self.transport.close()
            log.error("Error in querying LSF resource '%s': %s: %s",
                              self._resource.name, ex.__class__.__name__, str(ex))
            raise

        # self.transport.close()

        job.state = state
        return state


    @same_docstring_as(LRMS.cancel_job)
    def cancel_job(self, app):
        job = app.execution
        try:
            self.transport.connect()

            _command = ('bkill %s' % job.lrms_jobid)
            exit_code, stdout, stderr = self.transport.execute_command(_command)
            if exit_code != 0 and ('Job has already finished' not in stderr):
                log.error("Failed executing remote LSF command '%s'; exit status: %d" % (_command,exit_code))
                log.debug("Remote LSF command returned stdout: %s" % stdout)
                log.debug("Remote LSF command returned stderr: %s" % stderr)
                if exit_code == 127:
                    # failed executing remote command
                    raise gc3libs.exceptions.LRMSError('Failed executing remote command')

            # self.transport.close()
            return job

        except:
            # self.transport.close()
            log.critical('Failure in checking status')
            raise



    @same_docstring_as(LRMS.free)
    def free(self, app):

        job = app.execution
        try:
            log.debug("Connecting to cluster frontend '%s' as user '%s' via SSH ...",
                           self._resource.frontend, self._ssh_username)
            self.transport.connect()
            self.transport.remove_tree(job.ssh_remote_folder)
        except:
            log.warning("Failed removing remote folder '%s': %s: %s"
                        % (job.ssh_remote_folder, sys.exc_info()[0], sys.exc_info()[1]))
        return


    @same_docstring_as(LRMS.get_results)
    def get_results(self, app, download_dir, overwrite=False):
        if app.output_base_url is not None:
            raise gc3libs.exceptions.UnrecoverableDataStagingError(
                "Retrieval of output files to non-local destinations"
                " is not supported in the LSF backend (yet).")

        job = app.execution
        try:
            self.transport.connect()

            # Make list of files to copy, in the form of (remote_path, local_path) pairs.
            # This entails walking the `Application.outputs` list to expand wildcards
            # and directory references.
            stageout = [ ]
            for remote_relpath, local_url in app.outputs.iteritems():
                local_relpath = local_url.path
                if remote_relpath == gc3libs.ANY_OUTPUT:
                    remote_relpath = ''
                    local_relpath = ''
                stageout += _make_remote_and_local_path_pair(
                    self.transport, job, remote_relpath, download_dir, local_relpath)

            # copy back all files, renaming them to adhere to the ArcLRMS convention
            log.debug("Downloading job output into '%s' ...", download_dir)
            for remote_path, local_path in stageout:
                log.debug("Downloading remote file '%s' to local file '%s'",
                          remote_path, local_path)
                if (overwrite
                    or not os.path.exists(local_path)
                    or os.path.isdir(local_path)):
                    log.debug("Copying remote '%s' to local '%s'"
                              % (remote_path, local_path))
                    # ignore missing files (this is what ARC does too)
                    self.transport.get(remote_path, local_path,
                                       ignore_nonexisting=True)
                else:
                    log.info("Local file '%s' already exists;"
                             " will not be overwritten!",
                             local_path)

            # self.transport.close()
            return # XXX: should we return list of downloaded files?

        except:
            # self.transport.close()
            raise


    @same_docstring_as(LRMS.peek)
    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        job = app.execution
        assert job.has_key('ssh_remote_folder'), \
            "Missing attribute `ssh_remote_folder` on `Job` instance passed to `LsfLrms.peek`."

        if size is None:
            size = sys.maxint

        _filename_mapping = _lsf_filename_mapping(job.lrms_jobname, job.lrms_jobid, remote_filename)
        _remote_filename = os.path.join(job.ssh_remote_folder, _filename_mapping)

        try:
            self.transport.connect()
            remote_handler = self.transport.open(_remote_filename, mode='r', bufsize=-1)
            remote_handler.seek(offset)
            data = remote_handler.read(size)
            # self.transport.close()
        except Exception, ex:
            # self.transport.close()
            log.error("Could not read remote file '%s': %s: %s",
                              _remote_filename, ex.__class__.__name__, str(ex))

        try:
            local_file.write(data)
        except (TypeError, AttributeError):
            output_file = open(local_file, 'w+b')
            output_file.write(data)
            output_file.close()
        log.debug('... Done.')

    @cache_for(gc3libs.Default.ARC_CACHE_TIME)
    def get_resource_status(self):
        """
        Get dynamic information out of the LSF subsystem.

        return self._resource

        dynamic information required (at least those):
        total_queued
        free_slots
        user_running
        user_queued
        """

        try:
            self.transport.connect()

            # username = self._ssh_username

            # Run lhosts to get the list of available nodes and their
            # related number of cores
            # used to compute self._resource.total_slots
            # lhost output format:
            # ($nodeid,$OStype,$model,$cpuf,$ncpus,$maxmem,$maxswp)
            log.debug("Running `lshosts -w`... ")
            _command = "lshosts -w"
            exit_code, stdout, stderr = self.transport.execute_command(_command)
            if exit_code != 0:
                # Stop and do not continue
                # XXX: raise LRMSError here
                raise gc3libs.exceptions.LRMSError("LSF backend failed while executing [%s]."
                                    "Exit code: [%d]. Stdout: [%s]. Stderr: [%s]" %
                                    (_command, exit_code, stdout, stderr))

            lhosts_output = []
            if stdout:
                # Remove Header
                lhosts_output = stdout.strip().split('\n')
                lhosts_output.pop(0)

            # Run bhosts to get information about the number of
            # occupied slots for each node
            # used to compute self._resource.free_slots
            # bhosts output format:
            # HOST_NAME          STATUS          JL/U    MAX  NJOBS    RUN  SSUSP  USUSP    RSV
            # a3000              closed_Full     -      4      4      4      0      0      0
            # log.debug("Running `bhosts -w`... ")
            # _command = "bhosts -w"
            # exit_code, stdout, stderr = self.transport.execute_command(_command)

            # if exit_code != 0:
            #     # Stop and do not continue
            #     # XXX: raise LRMSError here
            #     raise gc3libs.exceptions.LRMSError("LSF backend failed while executing [%s]."
            #                         "Exit code: [%d]. Stdout: [%s]. Stderr: [%s]" %
            #                         (_command, exit_code, stdout, stderr))

            # bhosts_output = []
            # if stdout:
            #     # Remove Header
            #     bhosts_output = stdout.strip().split('\n')
            #     bhosts_output.pop(0)


            # Run bqueues to get information about the status of system queues
            # used to build running_jobs and queued
            log.debug("Running `bqueues`... ")
            _command = "bqueues"
            exit_code, stdout, stderr = self.transport.execute_command(_command)

            if exit_code != 0:
                # Stop and do not continue
                # XXX: raise LRMSError here
                raise gc3libs.exceptions.LRMSError("LSF backend failed while executing [%s]."
                                    "Exit code: [%d]. Stdout: [%s]. Stderr: [%s]" %
                                    (_command, exit_code, stdout, stderr))

            bqueues_output = []
            if stdout:
                bqueues_output = stdout.strip().split('\n')
                bqueues_output.pop(0)

            # Run bjobs to get information about the jobs for a given user
            # used to compute  self._resource.user_run and self._resource.user_queued
            # bjobs output format:
            # JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
            log.debug("Runing 'bjobs '... ")
            _command = "bjobs"
            exit_code, stdout, stderr = self.transport.execute_command(_command)

            if exit_code != 0:
                # Stop and do not continue
                # XXX: raise LRMSError here
                raise gc3libs.exceptions.LRMSError("LSF backend failed while executing [%s]."
                                    "Exit code: [%d]. Stdout: [%s]. Stderr: [%s]" %
                                    (_command, exit_code, stdout, stderr))

            bjobs_output = []
            if stdout:
                # Remove Header
                bjobs_output = stdout.strip().split('\n')
                bjobs_output.pop(0)

            # self.transport.close()

            # compute self._resource.total_slots
            self._resource.max_cores = 0
            for line in lhosts_output:
                # HOST_NAME      type    model  cpuf ncpus maxmem maxswp server RESOURCES
                (hostname, h_type, h_model, h_cpuf, h_ncpus) = line.strip().split()[0:5]
                try:
                    self._resource.max_cores +=  int(h_ncpus)
                except ValueError:
                    # h_ncpus == '-'
                    pass

            # compute total queued
            self._resource.queued = 0
            running_jobs = 0
            for line in bqueues_output:
                # QUEUE_NAME      PRIO STATUS          MAX JL/U JL/P JL/H NJOBS  PEND   RUN  SUSP
                (queue_name, priority, status, max_j, jlu, jlp, jlh, n_jobs, j_pend, j_run, j_susp) = line.split()
                self._resource.queued += int(j_pend)
                running_jobs += int(j_run)

            self._resource.free_slots = self._resource.max_cores - running_jobs

            # # compute self._resource.free_slots
            # total_jobs = 0

            # for line in bhosts_output:
            #     # HOST_NAME          STATUS          JL/U    MAX  NJOBS    RUN  SSUSP  USUSP    RSV
            #     # a3000              closed_Full     -      4      4      4      0      0      0
            #     njobs = line.strip().split()[4]
            #     try:
            #         total_jobs += int(njobs)
            #     except ValueError:
            #         # njobs == '-'
            #         pass


            # self._resource.free_slots = self._resource.max_cores - total_jobs

            # user runing/queued
            self._resource.user_run = 0
            self._resource.user_queued = 0

            queued_status = ['PEND', 'PSUSP', 'USUSP', 'SSUSP', 'WAIT', 'ZOMBI']

            for line in bjobs_output:
                # JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
                (jobid, user, stat, queue, from_h, exec_h) = line.strip().split()[0:6]
                # to compute the number of cores allocated per each job
                # we use the output format of EXEC_HOST field
                # e.g.: 1*cpt178:2*cpt151
                per_node_allocated_cores_list = exec_h.split(':')
                for node in per_node_allocated_cores_list:
                    try:
                        # mutli core
                        (cores, n_name) = node.split('*')
                    except ValueError:
                        # single core
                        cores = 1
                        n_name = node
                try:
                    if stat in queued_status:
                        self._resource.user_queued += int(cores)
                    else:
                        self._resource.user_run += int(cores)
                except ValueError:
                    # core == '-'
                    pass

            # log.info("Not updated resource '%s' status (see `backends/lsf.py`),"
            #          "using hardcoded defaults!!", self._resource.name)
            # self._resource.user_run = 0
            # self._resource.user_queued = 0
            # self._resource.free_slots = 800
            # self._resource.used_quota = -1
            # self._resource.queued = 0

            return self._resource

        except Exception, ex:
            # self.transport.close()
            log.error("Error querying remote LRMS, see debug log for details.")
            log.debug("Error querying LRMS: %s: %s",
                      ex.__class__.__name__, str(ex))
            raise

    @same_docstring_as(LRMS.validate_data)
    def close(self):
        self.transport.close()

    @same_docstring_as(LRMS.validate_data)
    def validate_data(self, data_file_list):
        """
        Supported protocols: file
        """
        for url in data_file_list:
            if not url.scheme in ['file']:
                return False
        return True


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="lsf",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
