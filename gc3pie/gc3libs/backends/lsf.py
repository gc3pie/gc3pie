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
__version__ = '2.0.0-rc3 version (SVN $Revision$)'


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

import batch

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

# Job <850088>, Job Name <GdemoSimpleApp>, User <smaffiol>, Project <default>, Status <EXIT>, Queue <normal>, Command <./script.62b626ca7ad5acaf_01.sh>, Share group charged </smaffiol>
# Wed Jul 11 14:11:10: Submitted from host <globus.vital-it.ch>, CWD <$HOME>, Output File (overwrite) <stdout.log>, Re-runnable, Login Shell </bin/sh>;
# Wed Jul 11 14:11:47: Started on <cpt157>, Execution Home </home/smaffiol>, Execution CWD </home/smaffiol>;
# Wed Jul 11 14:11:48: Exited with exit code 127. The CPU time used is 0.1 seconds.
# Wed Jul 11 14:11:48: Completed <exit>.

_bjobs_long_re = re.compile(
    '(?P<end_time>[a-zA-Z]+\s+[a-zA-Z]+\s+\d+\s+\d+:\d+:\d+):\s+'
    'Exited with exit code (?P<exit_status>\d+)[^0-9]+'
    'The CPU time used is (?P<used_cpu_time>[0-9\.]+)\s+'
    )


class LsfLrms(batch.BatchSystem):
    """
    Job control on LSF clusters (possibly by connecting via SSH to a submit node).
    """

    _batchsys_name = 'LSF'

    def __init__(self, name,
                 # this are inherited from the base LRMS class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime,
                 auth, # ignored if `transport` is 'local'
                 # these are inherited from `BatchSystem`
                 frontend, transport,
                 accounting_delay = 15,
                 # these are specific to this backend
                 **extra_args):

        # init base class
        batch.BatchSystem.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth,
            frontend, transport, accounting_delay, **extra_args)

        self.bsub = self._get_command_argv('bsub')

        # LSF commands
        self._bjobs = self._get_command('bjobs')
        self._bkill = self._get_command('bkill')
        self._bqueues = self._get_command('bqueues')
        self._lshosts = self._get_command('lshosts')


    def _submit_command(self, app):
        # LSF's `bsub` allows one to submit scripts and binaries with
        # the same syntax, so we do not need to create an auxiliary
        # submission script and can just specify the command on the
        # command-line
        sub_argv, app_argv = app.bsub(self)
        return (str.join(' ', sub_argv + app_argv), '')

    def _parse_submit_output(self, bsub_output):
        """Parse the ``bsub`` output for the local jobid."""
        return self.get_jobid_from_submit_output(bsub_output, _bsub_jobid_re)

    def _stat_command(self, job):
        return ("%s -l %s" % (self._bjobs, job.lrms_jobid))

    def _acct_command(self, job):
        return ("%s -l %s" % (self._bjobs, job.lrms_jobid))

    @staticmethod
    def _lsf_state_to_gc3pie_state(stat):
        log.debug("Translating LSF's `bjobs` status '%s' to gc3libs.Run.State ...", stat)
        try:
            return {
            # LSF 'stat' mapping:
                'PEND'  : Run.State.SUBMITTED,
                'RUN'   : Run.State.RUNNING,
                'PSUSP' : Run.State.STOPPED,
                'USUSP' : Run.State.STOPPED,
                'SSUSP' : Run.State.STOPPED,
                # DONE = successful termination
                'DONE'  : Run.State.TERMINATING,
                # EXIT = job was killed / exit forced
                'EXIT'  : Run.State.TERMINATING,
                # ZOMBI = job "killed" and unreachable
                'ZOMBI' : Run.State.TERMINATING,
                'UNKWN' : Run.State.UNKNOWN,
                }[stat]
        except KeyError:
            log.warning("Unknown LSF job status '%s', returning `UNKNOWN`", stat)
            return Run.State.UNKNOWN

    _status_re = re.compile(r'Status <(?P<state>[A-Z]+)>', re.M)
    _unsuccessful_exit_re = re.compile(r'Exited with exit code (?P<exit_status>[0-9]+).', re.M)
    _cpu_time_re = re.compile(r'The CPU time used is (?P<cputime>[0-9]+(\.[0-9]+)?) seconds', re.M)

    _CONTINUATION_LINE_START = '                     '

    @staticmethod
    def _parse_stat_output(stdout):
        # LSF `bjobs -l` uses a LDIF-style continuation lines, wherein
        # a line is truncated at 79 characters and continues upon the
        # next one; continuation lines start with a fixed amount of
        # whitespace.  Join continuation lines, so that we can work on
        # a single block of text.
        lines = [ ]
        for line in stdout.split('\n'):
            if len(line) == 0:
                continue
            if line.startswith(LsfLrms._CONTINUATION_LINE_START):
                lines[-1] += line[len(LsfLrms._CONTINUATION_LINE_START):]
            else:
                lines.append(line)

        # now rebuild stdout by joining the reconstructed lines
        stdout = str.join('\n', lines)

        jobstatus = gc3libs.utils.Struct()
        # XXX: this only works if the current status is the first one
        # reported in STDOUT ...
        match = LsfLrms._status_re.search(stdout)
        if match:
            stat = match.group('state')
            jobstatus.state = LsfLrms._lsf_state_to_gc3pie_state(stat)
            if stat == 'DONE':
                # DONE = success
                jobstatus.exit_status = 0
            elif stat == 'EXIT':
                # EXIT = job exited with exit code != 0
                match = LsfLrms._unsuccessful_exit_re.search(stdout)
                if match:
                    log.debug("LSF says: '%s'", match.group(0))
                    jobstatus.exit_status = int(match.group('exit_status'))
        assert 'state' in jobstatus
        return jobstatus

    # The same command is used in LSF to get the status and the accouting info
    _parse_acct_output = _parse_stat_output

    def _cancel_command(self, jobid):
        return ("%s %s" % (self._bkill, jobid))


    @cache_for(gc3libs.Default.ARC_CACHE_TIME)
    @LRMS.authenticated
    def get_resource_status(self):
        """
        Get dynamic information out of the LSF subsystem.

        return self

        dynamic information required (at least those):
        total_queued
        free_slots
        user_running
        user_queued
        """

        try:
            self.transport.connect()

            # Run lhosts to get the list of available nodes and their
            # related number of cores
            # used to compute self.total_slots
            # lhost output format:
            # ($nodeid,$OStype,$model,$cpuf,$ncpus,$maxmem,$maxswp)
            _command = ('%s -w' % self._lshosts)
            log.debug("Running `%s`... ", _command)
            exit_code, stdout, stderr = self.transport.execute_command(_command)
            if exit_code != 0:
                # cannot continue
                raise gc3libs.exceptions.LRMSError(
                    "LSF backend failed executing '%s':"
                    "exit code: %d; stdout: '%s'; stderr: '%s'." %
                    (_command, exit_code, stdout, stderr))

            if stdout:
                lhosts_output = stdout.strip().split('\n')
                # Remove Header
                lhosts_output.pop(0)
            else:
                lhosts_output = [ ]

            # Run bqueues to get information about the status of system queues
            # used to build running_jobs and queued
            _command = self._bqueues
            log.debug("Running `%s`... ", _command)
            exit_code, stdout, stderr = self.transport.execute_command(_command)
            if exit_code != 0:
                # cannot continue
                raise gc3libs.exceptions.LRMSError(
                    "LSF backend failed executing '%s':"
                    "exit code: %d; stdout: '%s'; stderr: '%s'." %
                    (_command, exit_code, stdout, stderr))

            if stdout:
                bqueues_output = stdout.strip().split('\n')
                bqueues_output.pop(0)
            else:
                bqueues_output = [ ]

            # Run bjobs to get information about the jobs for a given user
            # used to compute  self.user_run and self.user_queued
            # bjobs output format:
            # JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
            _command = self._bjobs
            log.debug("Runing `%s`... ", _command)
            exit_code, stdout, stderr = self.transport.execute_command(_command)
            if exit_code != 0:
                # cannot continue
                raise gc3libs.exceptions.LRMSError(
                    "LSF backend failed executing '%s':"
                    "exit code: %d; stdout: '%s'; stderr: '%s'." %
                    (_command, exit_code, stdout, stderr))

            if stdout:
                bjobs_output = stdout.strip().split('\n')
                # Remove Header
                bjobs_output.pop(0)
            else:
                bjobs_output = [ ]

            # compute self.total_slots
            self.max_cores = 0
            for line in lhosts_output:
                # HOST_NAME      type    model  cpuf ncpus maxmem maxswp server RESOURCES
                (hostname, h_type, h_model, h_cpuf, h_ncpus) = line.strip().split()[0:5]
                try:
                    self.max_cores +=  int(h_ncpus)
                except ValueError:
                    # h_ncpus == '-'
                    pass

            # compute total queued
            self.queued = 0
            running_jobs = 0
            for line in bqueues_output:
                # QUEUE_NAME      PRIO STATUS          MAX JL/U JL/P JL/H NJOBS  PEND   RUN  SUSP
                (queue_name, priority, status, max_j, jlu, jlp, jlh, n_jobs, j_pend, j_run, j_susp) = line.split()
                self.queued += int(j_pend)
                running_jobs += int(j_run)

            self.free_slots = self.max_cores - running_jobs

            # user runing/queued
            self.user_run = 0
            self.user_queued = 0

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
                        self.user_queued += int(cores)
                    else:
                        self.user_run += int(cores)
                except ValueError:
                    # core == '-'
                    pass

            return self

        except Exception, ex:
            # self.transport.close()
            log.error("Error querying remote LRMS, see debug log for details.")
            log.debug("Error querying LRMS: %s: %s",
                      ex.__class__.__name__, str(ex))
            raise


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="lsf",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
