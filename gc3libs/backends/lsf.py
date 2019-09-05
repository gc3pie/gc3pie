#! /usr/bin/env python

"""
Job control on LSF clusters (possibly connecting to the front-end via SSH).
"""

# Copyright (C) 2009-2016, 2019  University of Zurich. All rights reserved.
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import absolute_import, print_function, unicode_literals
from builtins import str
from builtins import next
__docformat__ = 'reStructuredText'


from collections import defaultdict
import datetime
import re
import time

from gc3libs import log, Run
import gc3libs.defaults
from gc3libs.backends import LRMS
import gc3libs.exceptions
from gc3libs.quantity import Duration, seconds, Memory, GB, MB, kB, bytes
import gc3libs.utils
from gc3libs.utils import sh_quote_safe_cmdline, sh_quote_unsafe_cmdline

from . import batch

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
# JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME   # noqa
# 473713  gloessa PEND  pub.1h     brutus2                 TM-T       Oct 19 17:10  # noqa
# noqa
# $ bjobs 473713                                                                    # noqa
# JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME   # noqa
# 473713  gloessa RUN   pub.1h     brutus2     a6128       TM-T       Oct 19 17:10  # noqa
# [gloessa@brutus2 test_queue]$ bjobs -l 473713                                     # noqa
# noqa
# Job <473713>, Job Name <TM-T>, User <gloessa>, Project <default>, Status <RUN>,   # noqa
# Queue <pub.1h>, Job Priority <50>, Command <#!/bin/sh;#;#   # noqa
# ;echo 'sequential test job';echo $OMP_NUM_THREADS;echo '$T   # noqa
# URBODIR: '$TURBODIR;sysname;which dscf;echo '$TMPDIR: '$TM   # noqa
# PDIR; echo 'parallel test job';export OMP_NUM_THREADS=2;ec   # noqa
# ho $OMP_NUM_THREADS;echo '$TURBODIR: '$TURBODIR;sysname;wh   # noqa
# ich dscf;echo '$TMPDIR: '$TMPDIR; sleep 300>, Share group    # noqa
# charged </lsf_cfour/gloessa>                                 # noqa
# Wed Oct 19 17:10:27: Submitted from host <brutus2>, CWD <$HOME/test_queue>, Out   # noqa
# put File <lsf.o%J>, Requested Resources <order[-r1m] span[   # noqa
# ptile=1] same[model] rusage[mem=1800,xs=1]>, Specified Hos   # noqa
# ts <thin+7>, <single+5>, <shared+3>, <parallel+1>;           # noqa
# noqa
# RUNLIMIT                                                                         # noqa
# 10.0 min of a6128                                                                # noqa
# Wed Oct 19 17:11:02: Started on <a6128>, Execution Home </cluster/home/chab/glo   # noqa
# essa>, Execution CWD </cluster/home/chab/gloessa/test_queu   # noqa
# e>;                                                          # noqa
# Wed Oct 19 17:12:10: Resource usage collected.                                    # noqa
# MEM: 5 Mbytes;  SWAP: 201 Mbytes;  NTHREAD: 5                # noqa
# PGID: 23177;  PIDs: 23177 23178 23182 23259                  # noqa
#
#
#  SCHEDULING PARAMETERS:
#            r15s   r1m  r15m   ut      pg    io   ls    it    tmp    swp   mem
#  loadSched   -     -     -     -       -     -    -     -     -      -     -
#  loadStop    -     -     -     -       -     -    -     -     -      -     -
#
#           scratch      xs       s       m       l      xl      sp
#  loadSched     -       -       -       -       -       -       -
#  loadStop      -       -       -       -       -       -       -
#
# $ bkill 473713
# Job <473713> is being terminated
#
# $ bjobs -W -w 473713
# JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME  PROJ_NAME CPU_USED MEM SWAP PIDS START_TIME FINISH_TIME  # noqa
# 473713  gloessa EXIT  pub.1h     brutus2     a6128       TM-T       10/19-17:10:27 default    000:00:00.12 5208   206312 23177,23178,23182,23259 10/19-17:11:02 10/19-17:14:49    # noqa
#
# STAT would be DONE if not killed
# $ bjobs 473713
# JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME  # noqa
# 473713  gloessa EXIT  pub.1h     brutus2     a6128       TM-T       Oct 19 17:10  # noqa
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
# -----------------------------------------------------------------------------
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
# ddadb         Numeric   Dec   number of connections that ddadb.ethz.ch can handle    # noqa
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

# Job <850088>, Job Name <GdemoSimpleApp>, User <smaffiol>, Project <default>, Status <EXIT>, Queue <normal>, Command <./script.62b626ca7ad5acaf_01.sh>, Share group charged </smaffiol>  # noqa
# Wed Jul 11 14:11:10: Submitted from host <globus.vital-it.ch>, CWD <$HOME>, Output File (overwrite) <stdout.log>, Re-runnable, Login Shell </bin/sh>;  # noqa
# Wed Jul 11 14:11:47: Started on <cpt157>, Execution Home </home/smaffiol>, Execution CWD </home/smaffiol>;  # noqa
# Wed Jul 11 14:11:48: Exited with exit code 127. The CPU time used is 0.1 seconds.  # noqa
# Wed Jul 11 14:11:48: Completed <exit>.

_bjobs_long_re = re.compile(
    r'(?P<end_time>[a-zA-Z]+\s+[a-zA-Z]+\s+\d+\s+\d+:\d+:\d+):\s+'
    r'Exited with exit code (?P<exit_status>\d+)[^0-9]+'
    r'The CPU time used is (?P<used_cpu_time>[0-9\.]+)\s+'
)


class LsfLrms(batch.BatchSystem):

    """Job control on LSF clusters (possibly by connecting via SSH to a
    submit node).

    """

    _batchsys_name = 'LSF'

    def __init__(self, name,
                 # this are inherited from the base LRMS class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime,
                 auth,  # ignored if `transport` is 'local'
                 # these are inherited from `BatchSystem`
                 frontend, transport,
                 # these are specific to this backend
                 lsf_continuation_line_prefix_length=None,
                 # (Note that optional arguments to the `BatchSystem` class,
                 # e.g.:
                 #     keyfile=None, accounting_delay=15,
                 # are collected into `extra_args` and should not be explicitly
                 # spelled out in this signature.)
                 **extra_args):

        # init base class
        batch.BatchSystem.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth,
            frontend, transport,
            **extra_args)

        self.bsub = self._get_command_argv('bsub')

        # LSF commands
        self._bacct = self._get_command('bacct', 'bjobs')
        self._bacct2 = self._get_command('bacct2', 'bacct')
        self._bjobs = self._get_command('bjobs')
        self._bkill = self._get_command('bkill')
        self._lshosts = self._get_command('lshosts')

        if lsf_continuation_line_prefix_length is not None:
            self._CONTINUATION_LINE_START = (' ' * lsf_continuation_line_prefix_length)
        else:
            self._CONTINUATION_LINE_START = None

    def _submit_command(self, app):
        # LSF's `bsub` allows one to submit scripts and binaries with
        # the same syntax, so we do not need to create an auxiliary
        # submission script and can just specify the command on the
        # command-line
        sub_argv, app_argv = app.bsub(self)
        prologue = self.get_prologue_script(app)
        epilogue = self.get_epilogue_script(app)
        if prologue or epilogue:
            return (sh_quote_safe_cmdline(sub_argv),
                    sh_quote_unsafe_cmdline(app_argv))
        else:
            return (sh_quote_unsafe_cmdline(sub_argv + app_argv), '')

    def _parse_submit_output(self, bsub_output):
        """Parse the ``bsub`` output for the local jobid."""
        return self.get_jobid_from_submit_output(bsub_output, _bsub_jobid_re)

    def _stat_command(self, job):
        return ("%s -l %s" % (self._bjobs, job.lrms_jobid))

    def _acct_command(self, job):
        return ("%s -l %s" % (self._bjobs, job.lrms_jobid))

    def _secondary_acct_command(self, job):
        return ("%s -l %s" % (self._bacct2, job.lrms_jobid))

    @staticmethod
    def _lsf_state_to_gc3pie_state(stat):
        log.debug("Translating LSF's `bjobs` status '%s' to"
                  " gc3libs.Run.State ...", stat)
        try:
            return {
                # LSF 'stat' mapping:
                'PEND': Run.State.SUBMITTED,
                'RUN': Run.State.RUNNING,
                'PSUSP': Run.State.STOPPED,
                'USUSP': Run.State.STOPPED,
                'SSUSP': Run.State.STOPPED,
                # DONE = successful termination
                'DONE': Run.State.TERMINATING,
                # EXIT = job was killed / exit forced
                'EXIT': Run.State.TERMINATING,
                # ZOMBI = job "killed" and unreachable
                'ZOMBI': Run.State.TERMINATING,
                'UNKWN': Run.State.UNKNOWN,
            }[stat]
        except KeyError:
            log.warning(
                "Unknown LSF job status '%s', returning `UNKNOWN`", stat)
            return Run.State.UNKNOWN

    _job_not_found_re = re.compile('Job <(?P<jobid>[0-9]+)> is not found')
    _status_re = re.compile(r'Status <(?P<state>[A-Z]+)>', re.M)
    _unsuccessful_exit_re = re.compile(
        r'Exited with exit code (?P<exit_status>[0-9]+).', re.M)
    _cpu_time_re = re.compile(
        r'The CPU time used is (?P<cputime>[0-9]+(\.[0-9]+)?) seconds', re.M)
    _mem_used_re = re.compile(
        r'MAX MEM:\s+(?P<mem_used>[0-9]+)\s+(?P<mem_unit>[a-zA-Z]+);', re.M)

    def _parse_stat_output(self, stdout, stderr):
        # LSF's `bjobs` can only report info for terminated jobs, if
        # they finished no longer than ``CLEAN_PERIOD`` seconds
        # before; for older jobs it just prints ``Job XXX is not
        # found`` to STDERR.  However, it does the same when passed a
        # non-existent job ID.  We cannot distinguish the two cases
        # here; let's just be optimistic and presume that if a job ID
        # is not found, it must have been terminated since (at least)
        # we have it in our records so it *was* submitted...  See
        # issue #513 for details.
        if self._job_not_found_re.match(stderr):
            return self._stat_result(Run.State.TERMINATING, None)

        # LSF `bjobs -l` uses a LDIF-style continuation lines, wherein
        # a line is truncated at 79 characters and continues upon the
        # next one; continuation lines start with a fixed amount of
        # whitespace.  However, the amount of whitespace varies with
        # LSF release and possibly other factors, so we need to guess
        # or have users configure it...
        if self._CONTINUATION_LINE_START is None:
            self._CONTINUATION_LINE_START = ' ' \
                * self._guess_continuation_line_prefix_len(stdout)

        # Join continuation lines, so that we can work on a single
        # block of text.
        lines = []
        for line in stdout.split('\n'):
            if len(line) == 0:
                continue
            if line.startswith(self._CONTINUATION_LINE_START):
                lines[-1] += line[len(self._CONTINUATION_LINE_START):]
            else:
                lines.append(line)

        # now rebuild stdout by joining the reconstructed lines
        stdout = '\n'.join(lines)

        state = Run.State.UNKNOWN
        termstatus = None

        # XXX: this only works if the current status is the first one
        # reported in STDOUT ...
        match = LsfLrms._status_re.search(stdout)
        if match:
            lsf_job_state = match.group('state')
            state = LsfLrms._lsf_state_to_gc3pie_state(lsf_job_state)
            if lsf_job_state == 'DONE':
                # DONE = success
                termstatus = (0, 0)
            elif lsf_job_state == 'EXIT':
                # EXIT = job exited with exit code != 0
                match = LsfLrms._unsuccessful_exit_re.search(stdout)
                if match:
                    exit_status = int(match.group('exit_status'))
                    termstatus = Run.shellexit_to_returncode(exit_status)

        return self._stat_result(state, termstatus)

    @staticmethod
    def _guess_continuation_line_prefix_len(stdout):
        """
        Guess the most likely length of the initial run of spaces in
        continuation lines in `bjobs` output.

        The euristics is rather crude: we count how many spaces are at
        the beginning of each line, and take the value with most
        occurrences.

        Since, in addition, LSF's `bjobs` output contains also
        scheduling parameters and other differently-formatted reports,
        we only consider lines that contain a ``<`` or a ``>``
        character to be valid continuation lines -- any other line is
        just discarded.

        This is necessary as the amount of whitespace at the beginning
        of lines seems to vary with LSF version and/or some other
        parameter.
        """
        # count occurrences of each prefix length
        occurrences = defaultdict(int)
        for line in stdout.split('\n'):
            if '<' not in line and '>' not in line:
                continue
            # FIXME: incorrect result if LSF mixes TABs and spaces
            ws_length = len(line) - len(line.lstrip())
            occurrences[ws_length] += 1
        # now find the length that has the max occurrences
        max_occurrences = max(occurrences.values())
        for length, count in list(occurrences.items()):
            if count == max_occurrences:
                return length

    # e.g., 'Mon Oct  8 12:04:56 2012'
    _TIMESTAMP_FMT_WITH_YEAR = '%a %b %d %H:%M:%S %Y'
    # e.g., 'Mon Oct  8 12:04:56'
    _TIMESTAMP_FMT_NO_YEAR = '%a %b %d %H:%M:%S'

    @staticmethod
    def _parse_timespec(ts):
        """Parse a timestamp as it appears in LSF bjobs/bacct logs."""
        # try "with year" format first, as it has all the info we need
        try:
            return datetime.datetime.strptime(
                ts, LsfLrms._TIMESTAMP_FMT_WITH_YEAR)
        except ValueError:
            pass  # ignore and try again without year
        try:
            # XXX: since we do not have a year, we resort to the
            # following heuristics: if the month in the timespec is
            # less than or equal to the current month, the timestamp
            # is for an event occurred during this year; if it's in a
            # month later than the current one, the timestamp refers
            # to an event occurred in the *past* year.
            today = datetime.date.today()
            # XXX: datetime.strptime() only available starting Py 2.5
            tm = time.strptime(ts, LsfLrms._TIMESTAMP_FMT_NO_YEAR)
            if tm[1] <= today.month:
                return datetime.datetime(today.year, *(tm[1:6]))
            else:
                return datetime.datetime(today.year - 1, *(tm[1:6]))
        except ValueError as err:
            gc3libs.log.error(
                "Cannot parse '%s' as an LSF timestamp: %s: %s",
                ts, err.__class__.__name__, err)
            raise

    @staticmethod
    def _parse_memspec(m):
        unit = m[-1]
        if unit == 'G':
            return Memory(int(m[:-1]), unit=GB)
        elif unit == 'M':
            return Memory(int(m[:-1]), unit=MB)
        elif unit in ['K', 'k']:  # XXX: not sure which one is used
            return Memory(int(m[:-1]), unit=kB)
        else:
            # XXX: what does LSF use as a default?
            return Memory(int(m), unit=bytes)

    def _parse_acct_output(self, stdout, stderr):
        # FIXME: this is an ugly fix, but we have issues with bacct
        # on some LSF installation being veeeeery slow, so we have to
        # try and use `bjobs` whenever possible, and fall back to
        # bacct if bjobs does not work.
        #
        # However, since the user could update the configuration file
        # and put `bacct = bacct`, we also have to ensure that we are
        # calling the correct function to parse the output of the acct
        # command.
        if 'bacct' in self._bacct:
            parser = self.__parse_acct_output_w_bacct
        elif 'bjobs' in self._bacct:
            parser = self.__parse_acct_output_w_bjobs
        else:
            log.warning(
                "Unknown accounting command `%s`."
                " Assuming its output is compatible"
                " with `bacct`", self._bacct)
            parser = self.__parse_acct_output_w_bacct
        return parser(stdout)

    _parse_secondary_acct_output = _parse_acct_output

    @staticmethod
    def __parse_acct_output_w_bjobs(stdout):
        acctinfo = {}

        # Try to parse used cputime
        match = LsfLrms._cpu_time_re.search(stdout)
        if match:
            cpu_time = match.group('cputime')
            acctinfo['used_cpu_time'] = Duration(float(cpu_time), unit=seconds)

        # Parse memory usage
        match = LsfLrms._mem_used_re.search(stdout)
        if match:
            mem_used = match.group('mem_used')
            # mem_unit should always be Mbytes
            acctinfo['max_used_memory'] = Memory(float(mem_used), unit=MB)

        # Find submission time and completion time
        for line in stdout.split('\n'):
            match = LsfLrms._EVENT_RE.match(line)
            if match:
                timestamp = line.split(': ')[0]
                event = match.group('event')
                if event == 'Submitted':
                    acctinfo['lsf_submission_time'] = \
                        LsfLrms._parse_timespec(timestamp)
                elif event in ['Dispatched', 'Started']:
                    acctinfo['lsf_start_time'] = \
                        LsfLrms._parse_timespec(timestamp)
                elif event in ['Completed', 'Done successfully']:
                    acctinfo['lsf_completion_time'] = \
                        LsfLrms._parse_timespec(timestamp)
                continue
        if 'lsf_completion_time' in acctinfo and 'lsf_start_time' in acctinfo:
            acctinfo['duration'] = Duration(
                acctinfo['lsf_completion_time'] - acctinfo['lsf_start_time'])
        else:
            # XXX: what should we use for jobs that did not run at all?
            acctinfo['duration'] = Duration(0, unit=seconds)

        return acctinfo


    _RESOURCE_USAGE_RE = re.compile(r'^\s+ CPU_T \s+ '
                                    r'WAIT \s+ '
                                    r'TURNAROUND \s+ '
                                    r'STATUS \s+ '
                                    r'HOG_FACTOR \s+ '
                                    r'MEM \s+ '
                                    r'SWAP', re.X)
    _EVENT_RE = re.compile(
        r'^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)'
        r' \s+ (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
        r' \s+ [0-9]+ \s+ [0-9:]+:'
        r' \s+ (?P<event>Submitted|Dispatched|Started|Completed|'
        r'Done\ successfully)', re.X)

    @staticmethod
    def __parse_acct_output_w_bacct(stdout):
        acctinfo = {}
        lines = iter(stdout.split('\n'))  # need to lookup next line in the loop
        for line in lines:
            match = LsfLrms._EVENT_RE.match(line)
            if match:
                timestamp = line.split(': ')[0]
                event = match.group('event')
                if event == 'Submitted':
                    acctinfo['lsf_submission_time'] = \
                        LsfLrms._parse_timespec(timestamp)
                elif event == 'Dispatched':
                    acctinfo['lsf_start_time'] = \
                        LsfLrms._parse_timespec(timestamp)
                elif event == 'Completed':
                    acctinfo['lsf_completion_time'] = \
                        LsfLrms._parse_timespec(timestamp)
                continue
            match = LsfLrms._RESOURCE_USAGE_RE.match(line)
            if match:
                # actual resource usage is on next line
                rusage = next(lines)
                cpu_t, wait, turnaround, status, hog_factor, mem, swap = \
                    rusage.split()
                # common backend attrs (see Issue 78)
                if 'lsf_completion_time' in acctinfo and 'lsf_start_time' in acctinfo:
                    acctinfo['duration'] = Duration(
                        acctinfo['lsf_completion_time'] - acctinfo['lsf_start_time'])
                else:
                    # XXX: what should we use for jobs that did not run at all?
                    acctinfo['duration'] = Duration(0, unit=seconds)
                acctinfo['used_cpu_time'] = Duration(float(cpu_t), unit=seconds)
                acctinfo['max_used_memory'] = LsfLrms._parse_memspec(mem)\
                    + LsfLrms._parse_memspec(swap)
                # the resource usage line is the last interesting line
                break
        return acctinfo

    def _cancel_command(self, jobid):
        return ("%s %s" % (self._bkill, jobid))

    @gc3libs.utils.cache_for(gc3libs.defaults.LSF_CACHE_TIME)
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
            exit_code, stdout, stderr = self.transport.execute_command(
                _command)
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
                lhosts_output = []

            # compute self.total_slots
            self.max_cores = 0
            for line in lhosts_output:
                # HOST_NAME      type    model  cpuf ncpus maxmem maxswp server RESOURCES  # noqa
                (hostname, h_type, h_model, h_cpuf, h_ncpus) = \
                    line.strip().split()[0:5]
                try:
                    self.max_cores += int(h_ncpus)
                except ValueError:
                    # h_ncpus == '-'
                    pass

            # Run `bjobs -u all -w` to get information about the jobs
            # for a given user used to compute `running_jobs`,
            # `self.queued`, `self.user_run` and `self.user_queued`.
            #
            # bjobs output format:
            # JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME  # noqa
            _command = ('%s -u all -w' % self._bjobs)
            log.debug("Runing `%s`... ", _command)
            exit_code, stdout, stderr = \
                self.transport.execute_command(_command)
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
                bjobs_output = []

            # user runing/queued
            used_cores = 0
            self.queued = 0
            self.user_queued = 0
            self.user_run = 0

            queued_statuses = ['PEND', 'PSUSP', 'USUSP',
                               'SSUSP', 'WAIT', 'ZOMBI']
            for line in bjobs_output:
                # JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME  # noqa
                (jobid, user, stat, queue, from_h, exec_h) = \
                    line.strip().split()[0:6]
                # to compute the number of cores allocated per each job
                # we use the output format of EXEC_HOST field
                # e.g.: 1*cpt178:2*cpt151
                for node in exec_h.split(':'):
                    try:
                        # multi core
                        (cores, n_name) = node.split('*')
                    except ValueError:
                        # single core
                        cores = 1
                try:
                    cores = int(cores)
                except ValueError:
                    # core == '-'
                    pass
                used_cores += cores

                if stat in queued_statuses:
                    self.queued += 1
                if user == self._username:
                    if stat in queued_statuses:
                        self.user_queued += 1
                    else:
                        self.user_run += 1

            self.free_slots = self.max_cores - used_cores

            return self

        except Exception as ex:
            # self.transport.close()
            log.error("Error querying remote LRMS, see debug log for details.")
            log.debug("Error querying LRMS: %s: %s",
                      ex.__class__.__name__, str(ex))
            raise


# main: run tests
if "__main__" == __name__:
    import doctest
    doctest.testmod(name="lsf",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
