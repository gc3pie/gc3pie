#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011-2014, 2016  University of Zurich. All rights reserved.
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
#
from __future__ import absolute_import, print_function, unicode_literals
from builtins import object
__docformat__ = 'reStructuredText'

import datetime
import os
import shutil
import tempfile

import gc3libs
import gc3libs.core
import gc3libs.config
from gc3libs.backends.lsf import LsfLrms
from gc3libs.quantity import Duration, hours, Memory, GB

_datetime_date = None

files_to_remove = []


def setUpModule():
    """Mock the `datetime.date.today()` outcome in order to make the LSF
    parsing independent from the testing date."""
    # save the original `datetime.date` to restore it in `tearDownModule()`
    import datetime
    global _datetime_date
    _datetime_date = datetime.date
    # mock features of `datetime.date.today()` that are actually used
    # in `LsfLrms._parse_date()`

    class MockDate(object):

        def __init__(self, real):
            self.__date = real

        def __getattr__(self, name):
            return getattr(self.__date, name)

        def __call__(self, *args, **kwargs):
            return self.__date(*args, **kwargs)
    datetime.date = MockDate(datetime.date)

    class Today(object):

        def __init__(self):
            self.year = 2012
            self.month = 12
    datetime.date.today = Today


def tearDownModule():
    # restore the original `datetime.date`
    global _datetime_date
    import datetime
    datetime.date = _datetime_date
    for fname in files_to_remove:
        if os.path.isdir(fname):
            shutil.rmtree(fname)
        else:
            os.remove(fname)


def test_get_command():
    (fd, tmpfile) = tempfile.mkstemp()
    files_to_remove.append(tmpfile)
    f = os.fdopen(fd, 'w+')
    f.write("""
[auth/ssh]
type=ssh
username=NONEXISTENT

[resource/example]
# mandatory stuff
type=lsf
auth=ssh
transport=ssh
frontend=example.org
max_cores_per_job=128
max_memory_per_core=2
max_walltime=2
max_cores=80
architecture=x86_64

# alternate command paths
bsub = /usr/local/bin/bsub -R lustre
bjobs = /usr/local/bin/bjobs
lshosts = /usr/local/sbin/lshosts # comments are ignored!

lsf_continuation_line_prefix_length = 12
""")
    f.close()

    cfg = gc3libs.config.Configuration()
    cfg.merge_file(tmpfile)
    b = cfg.make_resources()['example']

    assert b.bsub == ['/usr/local/bin/bsub', '-R', 'lustre']

    assert b._bjobs == '/usr/local/bin/bjobs'
    assert b._lshosts == '/usr/local/sbin/lshosts'

    assert b._CONTINUATION_LINE_START == 12 * ' '


def test_bjobs_output_done1():
    lsf = LsfLrms(name='test',
                  architecture=gc3libs.Run.Arch.X86_64,
                  max_cores=1,
                  max_cores_per_job=1,
                  max_memory_per_core=1 * GB,
                  max_walltime=1 * hours,
                  auth=None,  # ignored if `transport` is `local`
                  frontend='localhost',
                  transport='local')
    bjobs_output = """
Job <131851>, Job Name <ChromaExtractShort>, User <wwolski>, Project <default>,
                     Status <DONE>, Queue <pub.8h>, Job Priority <50>, Command
                     <ChromatogramExtractor -in /cluster/scratch/malars/openswa
                     th/data/AQUA_fixed_water/split_napedro_L120224_001_SW-400A
                     QUA_no_background_2ul_dilution_10/split_napedro_L120224_00
                     1_SW-400AQUA_no_background_2ul_dilution_10_28.mzML.gz -tr
                     /cluster/scratch/malars/openswath/assays/iRT/DIA_iRT.TraML
                      -out split_napedro_L120224_001_SW-400AQUA_no_background_2
                     ul_dilution_10_28._rtnorm.chrom.mzML -is_swath -min_upper_
                     edge_dist 1 -threads 2>, Share group charged </lsf_biol_al
                     l/lsf_biol_other/wwolski>
Tue Jul 24 10:03:15: Submitted from host <brutus3>, CWD <$HOME/.gc3pie_jobs/lrm
                     s_job.YNZmU17755/.>, Output File <lsf.o%J>, Requested Reso
                     urces <select[mem<70000 && lustre] order[-ut] rusage[mem=1
                     000,m=1]>, Login Shell </bin/sh>, Specified Hosts <thin+9>
                     , <single+8>, <smp16+6>, <smp24+5>, <smp48+4>;

 RUNLIMIT
 480.0 min of a6122
Tue Jul 24 10:04:19: Started on <a6122>, Execution Home </cluster/home/biol/wwo
                     lski>, Execution CWD </cluster/home/biol/wwolski/.gc3pie_j
                     obs/lrms_job.YNZmU17755/.>;
Tue Jul 24 10:05:45: Done successfully. The CPU time used is 2.1 seconds.

 MEMORY USAGE:
 MAX MEM: 41 Mbytes;  AVG MEM: 41 Mbytes

 SCHEDULING PARAMETERS:
           r15s   r1m  r15m   ut      pg    io   ls    it    tmp    swp    mem
 loadSched   -     -     -     -       -     -    -     -  1000M     -      -
 loadStop    -     -     -     -       -     -    -     -     -      -      -

          scratch      xs       s       m       l      xl      sp
 loadSched 4000.0      -       -       -       -       -       -
 loadStop      -       -       -       -       -       -       -
"""
    jobstatus = lsf._parse_stat_output(bjobs_output, '')
    assert jobstatus.state == gc3libs.Run.State.TERMINATING
    assert jobstatus.termstatus == (0, 0)


def test_bjobs_output_for_accounting():
    lsf = LsfLrms(name='test',
                  architecture=gc3libs.Run.Arch.X86_64,
                  max_cores=1,
                  max_cores_per_job=1,
                  max_memory_per_core=1 * GB,
                  max_walltime=1 * hours,
                  auth=None,  # ignored if `transport` is `local`
                  frontend='localhost',
                  transport='local')
    bjobs_output = """
Job <131851>, Job Name <ChromaExtractShort>, User <wwolski>, Project <default>,
                     Status <DONE>, Queue <pub.8h>, Job Priority <50>, Command
                     <ChromatogramExtractor -in /cluster/scratch/malars/openswa
                     th/data/AQUA_fixed_water/split_napedro_L120224_001_SW-400A
                     QUA_no_background_2ul_dilution_10/split_napedro_L120224_00
                     1_SW-400AQUA_no_background_2ul_dilution_10_28.mzML.gz -tr
                     /cluster/scratch/malars/openswath/assays/iRT/DIA_iRT.TraML
                      -out split_napedro_L120224_001_SW-400AQUA_no_background_2
                     ul_dilution_10_28._rtnorm.chrom.mzML -is_swath -min_upper_
                     edge_dist 1 -threads 2>, Share group charged </lsf_biol_al
                     l/lsf_biol_other/wwolski>
Tue Jul 24 10:03:15: Submitted from host <brutus3>, CWD <$HOME/.gc3pie_jobs/lrm
                     s_job.YNZmU17755/.>, Output File <lsf.o%J>, Requested Reso
                     urces <select[mem<70000 && lustre] order[-ut] rusage[mem=1
                     000,m=1]>, Login Shell </bin/sh>, Specified Hosts <thin+9>
                     , <single+8>, <smp16+6>, <smp24+5>, <smp48+4>;

 RUNLIMIT
 480.0 min of a6122
Tue Jul 24 10:04:19: Started on <a6122>, Execution Home </cluster/home/biol/wwo
                     lski>, Execution CWD </cluster/home/biol/wwolski/.gc3pie_j
                     obs/lrms_job.YNZmU17755/.>;
Tue Jul 24 10:05:45: Done successfully. The CPU time used is 2.1 seconds.

 MEMORY USAGE:
 MAX MEM: 41 Mbytes;  AVG MEM: 41 Mbytes

 SCHEDULING PARAMETERS:
           r15s   r1m  r15m   ut      pg    io   ls    it    tmp    swp    mem
 loadSched   -     -     -     -       -     -    -     -  1000M     -      -
 loadStop    -     -     -     -       -     -    -     -     -      -      -

          scratch      xs       s       m       l      xl      sp
 loadSched 4000.0      -       -       -       -       -       -
 loadStop      -       -       -       -       -       -       -
"""

    # Also parse the output of jobs to get accounting information
    acct = lsf._parse_acct_output(bjobs_output, '')
    assert acct['duration'] == Duration('86s')
    assert acct['used_cpu_time'] == Duration('2.1s')
    assert acct['max_used_memory'] == Memory('41MB')


def test_bjobs_output_done2():
    lsf = LsfLrms(name='test',
                  architecture=gc3libs.Run.Arch.X86_64,
                  max_cores=1,
                  max_cores_per_job=1,
                  max_memory_per_core=1 * GB,
                  max_walltime=1 * hours,
                  auth=None,  # ignored if `transport` is `local`
                  frontend='localhost',
                  transport='local')
    jobstatus = lsf._parse_stat_output("""
Job <726659>, Job Name <Application>, User <wwolski>, Project <default>, Status
                      <DONE>, Queue <pub.8h>, Job Priority <50>, Command <FileM
                     erger -in /IMSB/users/wwolski/gc3pieScripts/split_napedro_
                     L120224_001_SW-400AQUA_no_background_2ul_dilution.ChromaEx
                     tractShort/split_napedro_L120224_001_SW-400AQUA_no_backgro
                     und_2ul_dilution_10_12._rtnorm.chrom.mzML /IMSB/users/wwol
                     ski/gc3pieScripts/split_napedro_L120224_001_SW-400AQUA_no_
                     background_2ul_dilution.ChromaExtractShort/split_napedro_L
                     120224_001_SW-400AQUA_no_background_2ul_dilution_10_13._rt
                     norm.chrom.mzML /IMSB/users/wwolski/gc3pieScripts/split_na
                     pedro_L120224_001_SW-400AQUA_no_background_2ul_dilution.Ch
                     romaExtractShort/split_napedro_L120224_001_SW-400AQUA_no_b
                     ackground_2ul_dilution_10_14._rtnorm.chrom.mzML /IMSB/user
                     s/wwolski/gc3pieScripts/split_napedro_L120224_001_SW-400AQ
                     UA_no_background_2ul_dilution.ChromaExtractShort/split_nap
                     edro_L120224_001_SW-400AQUA_no_background_2ul_dilution_10_
                     15._rtnorm.chrom.mzML /IMSB/users/wwolski/gc3pieScripts/sp
                     lit_napedro_L120224_001_SW-400AQUA_no_background_2ul_dilut
                     ion.ChromaExtractShort/split_napedro_L120224_001_SW-400AQU
                     A_no_background_2ul_dilution_10_16._rtnorm.chrom.mzML /IMS
                     B/users/wwolski/gc3pieScripts/split_napedro_L120224_001_SW
                     -400AQUA_no_background_2ul_dilution.ChromaExtractShort/spl
                     it_napedro_L120224_001_SW-400AQUA_no_background_2ul_diluti
                     on_10_17._rtnorm.chrom.mzML /IMSB/users/wwolski/gc3pieScri
                     pts/split_napedro_L120224_001_SW-400AQUA_no_background_2ul
                     _dilution.ChromaExtractShort/split_napedro_L120224_001_SW-
                     400AQUA_no_background_2ul_dilution_10_18._rtnorm.chrom.mzM
                     L /IMSB/users/wwolski/gc3pieScripts/split_napedro_L120224_
                     001_SW-400AQUA_no_background_2ul_dilution.ChromaExtractSho
                     rt/split_napedro_L120224_001_SW-400AQUA_no_background_2ul_
                     dilution_10_19._rtnorm.chrom.mzML /IMSB/users/wwolski/gc3p
                     ieScripts/split_napedro_L120224_001_SW-400AQUA_no_backgrou
                     nd_2ul_dilution.ChromaExtractShort/split_napedro_L120224_0
                     01_SW-400AQUA_no_background_2ul_dilution_10_1._rtnorm.chro
                     m.mzML /IMSB/users/wwolski/gc3pieScripts/split_napedro_L12
                     0224_001_SW-400AQUA_no_background_2ul_dilution.ChromaExtra
                     ctShort/split_napedro_L120224_001_SW-400AQUA_no_background
                     _2ul_dilution_10_20._rtnorm.chrom.mzML /IMSB/users/wwolski
                     /gc3pieScripts/split_napedro_L120224_001_SW-400AQUA_no_bac
                     kground_2ul_dilution.ChromaExtractShort/split_napedro_L120
                     224_001_SW-400AQUA_no_background_2ul_dilution_10_21._rtnor
                     m.chrom.mzML /IMSB/users/wwolski/gc3pieScripts/split_naped
                     ro_L120224_001_SW-400AQUA_no_background_2ul_dilution.Chrom
                     aExtractShort/split_napedro_L120224_001_SW-400AQUA_no_back
                     ground_2ul_dilution_10_22._rtnorm.chrom.mzML /IMSB/users/w
                     wolski/gc3pieScripts/split_napedro_L120224_001_SW-400AQUA_
                     no_background_2ul_dilution.ChromaExtractShort/split_napedr
                     o_L120224_001_SW-400AQUA_no_background_2ul_dilution_10_23.
                     _rtnorm.chrom.mzML /IMSB/users/wwolski/gc3pieScripts/split
                     _napedro_L120224_001_SW-400AQUA_no_background_2ul_dilution
                     .ChromaExtractShort/split_napedro_L120224_001_SW-400AQUA_n
                     o_background_2ul_dilution_10_24._rtnorm.chrom.mzML /IMSB/u
                     sers/wwolski/gc3pieScripts/split_napedro_L120224_001_SW-40
                     0AQUA_no_background_2ul_dilution.ChromaExtractShort/split_
                     napedro_L120224_001_SW-400AQUA_no_background_2ul_dilution_
                     10_25._rtnorm.chrom.mzML /IMSB/users/wwolski/gc3pieScripts
                     /split_napedro_L120224_001_SW-400AQUA_no_background_2ul_di
                     lution.ChromaExtractShort/split_napedro_L120224_001_SW-400
                     AQUA_no_background_2ul_dilution_10_26._rtnorm.chrom.mzML /
                     IMSB/users/wwolski/gc3pieScripts/split_napedro_L120224_001
                     _SW-400AQUA_no_background_2ul_dilution.ChromaExtractShort/
                     split_napedro_L120224_001_SW-400AQUA_no_background_2ul_dil
                     ution_10_27._rtnorm.chrom.mzML /IMSB/users/wwolski/gc3pieS
                     cripts/split_napedro_L120224_001_SW-400AQUA_no_background_
                     2ul_dilution.ChromaExtractShort/split_napedro_L120224_001_
                     SW-400AQUA_no_background_2ul_dilution_10_28._rtnorm.chrom.
                     mzML /IMSB/users/wwolski/gc3pieScripts/split_napedro_L1202
                     24_001_SW-400AQUA_no_background_2ul_dilution.ChromaExtract
                     Short/split_napedro_L120224_001_SW-400AQUA_no_background_2
                     ul_dilution_10_29._rtnorm.chrom.mzML /IMSB/users/wwolski/g
                     c3pieScripts/split_napedro_L120224_001_SW-400AQUA_no_backg
                     round_2ul_dilution.ChromaExtractShort/split_napedro_L12022
                     4_001_SW-400AQUA_no_background>, Share group charged </lsf
                     _biol_all/lsf_biol_other/wwolski>
Mon Jul 30 15:12:05: Submitted from host <brutus2>, CWD <$HOME/.gc3pie_jobs/lrm
                     s_job.QwyFmi4681/.>, Output File <lsf.o%J>, Requested Reso
                     urces <select[mem<70000] order[-ut] rusage[mem=1000,m=1]>,
                      Login Shell </bin/sh>, Specified Hosts <thin+9>, <single+
                     8>, <smp16+6>, <smp24+5>, <smp48+4>;

 RUNLIMIT
 480.0 min of a3168
Mon Jul 30 15:12:47: Started on <a3168>, Execution Home </cluster/home/biol/wwo
                     lski>, Execution CWD </cluster/home/biol/wwolski/.gc3pie_j
                     obs/lrms_job.QwyFmi4681/.>;
Mon Jul 30 15:12:56: Done successfully. The CPU time used is 1.7 seconds.

 SCHEDULING PARAMETERS:
           r15s   r1m  r15m   ut      pg    io   ls    it    tmp    swp    mem
 loadSched   -     -     -     -       -     -    -     -     -      -      -
 loadStop    -     -     -     -       -     -    -     -     -      -      -

          scratch      xs       s       m       l      xl      sp
 loadSched20000.0      -       -       -       -       -       -
 loadStop      -       -       -       -       -       -       -
""",
    # STDERR
    '')
    assert jobstatus.state == gc3libs.Run.State.TERMINATING
    assert jobstatus.termstatus == (0, 0)


def test_bjobs_output_done3():
    lsf = LsfLrms(name='test',
                  architecture=gc3libs.Run.Arch.X86_64,
                  max_cores=1,
                  max_cores_per_job=1,
                  max_memory_per_core=1 * GB,
                  max_walltime=1 * hours,
                  auth=None,  # ignored if `transport` is `local`
                  frontend='localhost',
                  transport='local')
    jobstatus = lsf._parse_stat_output("""
Job <2073>, Job Name <GRunApplication.0>, User <markmon>, Project <default>, St
                          atus <EXIT>, Queue <normal>, Command <sh -c inputfile
                          .txt>
Mon Aug  4 12:28:51 2014: Submitted from host <pa64.dri.edu>, CWD <$HOME/.gc3pi
                          e_jobs/lrms_job.5gDDxlxcty>, Specified CWD <$HOME/.gc
                          3pie_jobs/lrms_job.5gDDxlxcty/.>, Output File (overwr
                          ite) <stdout.txt>, Error File (overwrite) <stderr.txt
                          >, Requested Resources <rusage[mem=2000]>, Login Shel
                          l </bin/sh>;

 RUNLIMIT
 480.0 min of pa54.dri.edu
Mon Aug  4 12:28:51 2014: Started on <pa54.dri.edu>, Execution Home </home/mark
                          mon>, Execution CWD </home/markmon/.gc3pie_jobs/lrms_
                          job.5gDDxlxcty/.>;
Mon Aug  4 12:28:51 2014: Exited with exit code 127. The CPU time used is 0.1 s
                          econds.
Mon Aug  4 12:28:51 2014: Completed <exit>.

 SCHEDULING PARAMETERS:
           r15s   r1m  r15m   ut      pg    io   ls    it    tmp    swp    mem
 loadSched   -     -     -     -       -     -    -     -     -      -      -
 loadStop    -     -     -     -       -     -    -     -     -      -      -

 RESOURCE REQUIREMENT DETAILS:
 Combined: select[type == local] order[r15s:pg] rusage[mem=2000.00]
 Effective: select[type == local] order[r15s:pg] rusage[mem=2000.00]
    """,
    # STDERR
    '')
    assert jobstatus.state == gc3libs.Run.State.TERMINATING
    assert jobstatus.termstatus == (0, 127)


def test_bjobs_output_done_long_ago():
    """Test parsing `bjobs -l` output for a job that was removed from `mbatchd` core memory"""
    lsf = LsfLrms(name='test',
                  architecture=gc3libs.Run.Arch.X86_64,
                  max_cores=1,
                  max_cores_per_job=1,
                  max_memory_per_core=1 * GB,
                  max_walltime=1 * hours,
                  auth=None,  # ignored if `transport` is `local`
                  frontend='localhost',
                  transport='local')
    jobstatus = lsf._parse_stat_output(
        # empty STDOUT
        '',
        # STDERR
        'Job <943186> is not found')
    assert jobstatus.state == gc3libs.Run.State.TERMINATING
    assert jobstatus.termstatus == None


def test_bjobs_output_exit_nonzero():
    lsf = LsfLrms(name='test',
                  architecture=gc3libs.Run.Arch.X86_64,
                  max_cores=1,
                  max_cores_per_job=1,
                  max_memory_per_core=1 * GB,
                  max_walltime=1 * hours,
                  auth=None,  # ignored if `transport` is `local`
                  frontend='localhost',
                  transport='local')
    jobstatus = lsf._parse_stat_output("""
Job <132286>, User <wwolski>, Project <default>, Status <EXIT>, Queue <pub.1h>,
                     Job Priority <50>, Command <./x.sh>, Share group charged <
                     /lsf_biol_all/lsf_biol_other/wwolski>
Tue Jul 24 10:26:42: Submitted from host <brutus3>, CWD <$HOME/.>, Output File
                     <lsf.o%J>, Requested Resources <select[mem<70000] order[-u
                     t] rusage[mem=1024,xs=1]>, Specified Hosts <thin+9>, <sing
                     le+8>, <smp16+6>, <smp24+5>, <smp48+4>, <parallel+1>;
Tue Jul 24 10:26:47: Started on <a3010>, Execution Home </cluster/home/biol/wwo
                     lski>, Execution CWD </cluster/home/biol/wwolski/.>;
Tue Jul 24 10:26:53: Exited with exit code 42. The CPU time used is 0.0 seconds
                     .
Tue Jul 24 10:26:53: Completed <exit>.

 SCHEDULING PARAMETERS:
           r15s   r1m  r15m   ut      pg    io   ls    it    tmp    swp    mem
 loadSched   -     -     -     -       -     -    -     -     -      -      -
 loadStop    -     -     -     -       -     -    -     -     -      -      -

          scratch      xs       s       m       l      xl      sp
 loadSched20000.0      -       -       -       -       -       -
 loadStop      -       -       -       -       -       -       -
    """,
    # STDERR
    '')
    assert jobstatus.state == gc3libs.Run.State.TERMINATING
    assert jobstatus.termstatus == (0, 42)


def test_bjobs_incorrect_prefix_length():
    lsf = LsfLrms(name='test',
                  architecture=gc3libs.Run.Arch.X86_64,
                  max_cores=1,
                  max_cores_per_job=1,
                  max_memory_per_core=1 * GB,
                  max_walltime=1 * hours,
                  auth=None,  # ignored if `transport` is `local`
                  frontend='localhost',
                  transport='local',
                  lsf_continuation_line_prefix_length=7)
    stat_result = lsf._parse_stat_output("""
Job <2073>, Job Name <GRunApplication.0>, User <markmon>, Project <default>, St
                          atus <EXIT>, Queue <normal>, Command <sh -c inputfile
                          .txt>
Mon Aug  4 12:28:51 2014: Submitted from host <pa64.dri.edu>, CWD <$HOME/.gc3pi
                          e_jobs/lrms_job.5gDDxlxcty>, Specified CWD <$HOME/.gc
                          3pie_jobs/lrms_job.5gDDxlxcty/.>, Output File (overwr
                          ite) <stdout.txt>, Error File (overwrite) <stderr.txt
                          >, Requested Resources <rusage[mem=2000]>, Login Shel
                          l </bin/sh>;

 RUNLIMIT
 480.0 min of pa54.dri.edu
Mon Aug  4 12:28:51 2014: Started on <pa54.dri.edu>, Execution Home </home/mark
                          mon>, Execution CWD </home/markmon/.gc3pie_jobs/lrms_
                          job.5gDDxlxcty/.>;
Mon Aug  4 12:28:51 2014: Exited with exit code 127. The CPU time used is 0.1 s
                          econds.
Mon Aug  4 12:28:51 2014: Completed <exit>.

 SCHEDULING PARAMETERS:
           r15s   r1m  r15m   ut      pg    io   ls    it    tmp    swp    mem
 loadSched   -     -     -     -       -     -    -     -     -      -      -
 loadStop    -     -     -     -       -     -    -     -     -      -      -

 RESOURCE REQUIREMENT DETAILS:
 Combined: select[type == local] order[r15s:pg] rusage[mem=2000.00]
 Effective: select[type == local] order[r15s:pg] rusage[mem=2000.00]
""",
    # STDERR
    '')
    assert stat_result.state == gc3libs.Run.State.UNKNOWN
    assert stat_result.termstatus == None


def test_bjobs_correct_explicit_prefix_length():
    lsf = LsfLrms(name='test',
                  architecture=gc3libs.Run.Arch.X86_64,
                  max_cores=1,
                  max_cores_per_job=1,
                  max_memory_per_core=1 * GB,
                  max_walltime=1 * hours,
                  auth=None,  # ignored if `transport` is `local`
                  frontend='localhost',
                  transport='local',
                  lsf_continuation_line_prefix_length=26)
    stat_result = lsf._parse_stat_output("""
Job <2073>, Job Name <GRunApplication.0>, User <markmon>, Project <default>, St
                          atus <EXIT>, Queue <normal>, Command <sh -c inputfile
                          .txt>
Mon Aug  4 12:28:51 2014: Submitted from host <pa64.dri.edu>, CWD <$HOME/.gc3pi
                          e_jobs/lrms_job.5gDDxlxcty>, Specified CWD <$HOME/.gc
                          3pie_jobs/lrms_job.5gDDxlxcty/.>, Output File (overwr
                          ite) <stdout.txt>, Error File (overwrite) <stderr.txt
                          >, Requested Resources <rusage[mem=2000]>, Login Shel
                          l </bin/sh>;

 RUNLIMIT
 480.0 min of pa54.dri.edu
Mon Aug  4 12:28:51 2014: Started on <pa54.dri.edu>, Execution Home </home/mark
                          mon>, Execution CWD </home/markmon/.gc3pie_jobs/lrms_
                          job.5gDDxlxcty/.>;
Mon Aug  4 12:28:51 2014: Exited with exit code 127. The CPU time used is 0.1 s
                          econds.
Mon Aug  4 12:28:51 2014: Completed <exit>.

 SCHEDULING PARAMETERS:
           r15s   r1m  r15m   ut      pg    io   ls    it    tmp    swp    mem
 loadSched   -     -     -     -       -     -    -     -     -      -      -
 loadStop    -     -     -     -       -     -    -     -     -      -      -

 RESOURCE REQUIREMENT DETAILS:
 Combined: select[type == local] order[r15s:pg] rusage[mem=2000.00]
 Effective: select[type == local] order[r15s:pg] rusage[mem=2000.00]
""",
    # STDERR
    '')
    assert stat_result.state == gc3libs.Run.State.TERMINATING
    assert stat_result.termstatus == (0, 127)


def test_bacct_done0():
    """Test parsing accounting information of a <sleep 300> job."""
    # gotten with `bacct -l "jobid"`
    lsf = LsfLrms(name='test',
                  architecture=gc3libs.Run.Arch.X86_64,
                  max_cores=1,
                  max_cores_per_job=1,
                  max_memory_per_core=1 * GB,
                  max_walltime=1 * hours,
                  auth=None,  # ignored if `transport` is `local`
                  frontend='localhost',
                  transport='local',
                  bacct='bacct')
    acct = lsf._parse_acct_output("""
Accounting information about jobs that are:
  - submitted by all users.
  - accounted on all projects.
  - completed normally or exited
  - executed on all hosts.
  - submitted to all queues.
  - accounted on all service classes.
------------------------------------------------------------------------------

Job <3329613>, User <rmurri>, Project <default>, Status <DONE>, Queue <pub.1h>,
                     Command <sleep 60>, Share group charged </lsf_biol_all/lsf
                     _aeber/rmurri>
Mon Oct  8 17:07:54: Submitted from host <brutus4>, CWD <$HOME>, Output File <l
                     sf.o%J>;
Mon Oct  8 17:08:44: Dispatched to <a3201>;
Mon Oct  8 17:09:51: Completed <done>.

Accounting information about this job:
     Share group charged </lsf_biol_all/lsf_aeber/rmurri>
     CPU_T     WAIT     TURNAROUND   STATUS     HOG_FACTOR    MEM    SWAP
      0.08       50            117     done         0.0007     5M    222M
------------------------------------------------------------------------------

SUMMARY:      ( time unit: second )
 Total number of done jobs:       1      Total number of exited jobs:     0
 Total CPU time consumed:       0.1      Average CPU time consumed:     0.1
 Maximum CPU time of a job:     0.1      Minimum CPU time of a job:     0.1
 Total wait time in queues:    50.0
 Average wait time in queue:   50.0
 Maximum wait time in queue:   50.0      Minimum wait time in queue:   50.0
 Average turnaround time:       117 (seconds/job)
 Maximum turnaround time:       117      Minimum turnaround time:       117
 Average hog factor of a job:  0.00 ( cpu time / turnaround time )
 Maximum hog factor of a job:  0.00      Minimum hog factor of a job:  0.00

    """,
    # STDERR
    '')
    assert acct['duration'] == Duration('67s')
    assert acct['used_cpu_time'] == Duration('0.08s')
    assert acct['max_used_memory'] == Memory('227MB')
    # timestamps
    year = datetime.date.today().year
    assert (acct['lsf_submission_time'] ==
                 datetime.datetime(year, 10, 8, 17, 7, 54))
    assert (acct['lsf_start_time'] ==
                 datetime.datetime(year, 10, 8, 17, 8, 44))
    assert (acct['lsf_completion_time'] ==
                 datetime.datetime(year, 10, 8, 17, 9, 51))


def test_bacct_done1():
    """Test parsing `bacct -l` output for a not-so-trivial job."""
    lsf = LsfLrms(name='test',
                  architecture=gc3libs.Run.Arch.X86_64,
                  max_cores=1,
                  max_cores_per_job=1,
                  max_memory_per_core=1 * GB,
                  max_walltime=1 * hours,
                  auth=None,  # ignored if `transport` is `local`
                  frontend='localhost',
                  transport='local',
                  bacct='bacct')
    acct = lsf._parse_acct_output("""
Accounting information about jobs that are:
  - submitted by all users.
  - accounted on all projects.
  - completed normally or exited
  - executed on all hosts.
  - submitted to all queues.
  - accounted on all service classes.
------------------------------------------------------------------------------

Job <3329618>, User <rmurri>, Project <default>, Status <DONE>, Queue <pub.1h>,
                     Command <md5sum lsf.o3224113 lsf.o3224132>, Share group ch
                     arged </lsf_biol_all/lsf_aeber/rmurri>
Mon Oct  8 17:08:54: Submitted from host <brutus4>, CWD <$HOME>, Output File <l
                     sf.o%J>;
Mon Oct  8 17:10:01: Dispatched to <a3041>;
Mon Oct  8 17:10:07: Completed <done>.

Accounting information about this job:
     Share group charged </lsf_biol_all/lsf_aeber/rmurri>
     CPU_T     WAIT     TURNAROUND   STATUS     HOG_FACTOR    MEM    SWAP
      0.04       67             73     done         0.0005     3M     34M
------------------------------------------------------------------------------

SUMMARY:      ( time unit: second )
 Total number of done jobs:       1      Total number of exited jobs:     0
 Total CPU time consumed:       0.0      Average CPU time consumed:     0.0
 Maximum CPU time of a job:     0.0      Minimum CPU time of a job:     0.0
 Total wait time in queues:    67.0
 Average wait time in queue:   67.0
 Maximum wait time in queue:   67.0      Minimum wait time in queue:   67.0
 Average turnaround time:        73 (seconds/job)
 Maximum turnaround time:        73      Minimum turnaround time:        73
 Average hog factor of a job:  0.00 ( cpu time / turnaround time )
 Maximum hog factor of a job:  0.00      Minimum hog factor of a job:  0.00
    """,
    # STDERR
    '')
    assert acct['duration'] == Duration('6s')
    assert acct['used_cpu_time'] == Duration('0.04s')
    assert acct['max_used_memory'] == Memory('37MB')
    # timestamps
    year = datetime.date.today().year
    assert (acct['lsf_submission_time'] ==
                 datetime.datetime(year, 10, 8, 17, 8, 54))
    assert (acct['lsf_start_time'] ==
                 datetime.datetime(year, 10, 8, 17, 10, 1))
    assert (acct['lsf_completion_time'] ==
                 datetime.datetime(year, 10, 8, 17, 10, 7))


def test_bacct_killed():
    """Test parsing `bacct -l` output for a canceled job."""
    lsf = LsfLrms(name='test',
                  architecture=gc3libs.Run.Arch.X86_64,
                  max_cores=1,
                  max_cores_per_job=1,
                  max_memory_per_core=1 * GB,
                  max_walltime=1 * hours,
                  auth=None,  # ignored if `transport` is `local`
                  frontend='localhost',
                  transport='local',
                  bacct='bacct')
    acct = lsf._parse_acct_output("""
Accounting information about jobs that are:
  - submitted by all users.
  - accounted on all projects.
  - completed normally or exited
  - executed on all hosts.
  - submitted to all queues.
  - accounted on all service classes.
------------------------------------------------------------------------------

Job <3224113>, User <rmurri>, Project <default>, Status <EXIT>, Queue <pub.1h>,
                     Command <sleep 300>, Share group charged </lsf_biol_all/ls
                     f_aeber/rmurri>
Fri Oct  5 17:49:35: Submitted from host <brutus4>, CWD <$HOME>, Output File <l
                     sf.o%J>;
Fri Oct  5 17:50:35: Dispatched to <a3191>;
Fri Oct  5 17:51:30: Completed <exit>; TERM_OWNER: job killed by owner.

Accounting information about this job:
     Share group charged </lsf_biol_all/lsf_aeber/rmurri>
     CPU_T     WAIT     TURNAROUND   STATUS     HOG_FACTOR    MEM    SWAP
      0.04       60            115     exit         0.0003     1M     34M
------------------------------------------------------------------------------

SUMMARY:      ( time unit: second )
 Total number of done jobs:       0      Total number of exited jobs:     1
 Total CPU time consumed:       0.0      Average CPU time consumed:     0.0
 Maximum CPU time of a job:     0.0      Minimum CPU time of a job:     0.0
 Total wait time in queues:    60.0
 Average wait time in queue:   60.0
 Maximum wait time in queue:   60.0      Minimum wait time in queue:   60.0
 Average turnaround time:       115 (seconds/job)
 Maximum turnaround time:       115      Minimum turnaround time:       115
 Average hog factor of a job:  0.00 ( cpu time / turnaround time )
 Maximum hog factor of a job:  0.00      Minimum hog factor of a job:  0.00
""",
    # STDERR
    '')
    assert acct['duration'] == Duration('55s')
    assert acct['used_cpu_time'] == Duration('0.04s')
    assert acct['max_used_memory'] == Memory('35MB')
    # timestamps
    year = datetime.date.today().year
    assert (acct['lsf_submission_time'] ==
                 datetime.datetime(year, 10, 5, 17, 49, 35))
    assert (acct['lsf_start_time'] ==
                 datetime.datetime(year, 10, 5, 17, 50, 35))
    assert (acct['lsf_completion_time'] ==
                 datetime.datetime(year, 10, 5, 17, 51, 30))


# LSF incorporates resource usage information in a job's output;
# the job's output is a copy of the email that the LSF system
# sends to the user that submitted a job.
###############################################################################
# Sender: LSF System <lsfadmin@cpt086>
# Subject: Job 943186: <md5sum XXX/x.sh> Done
#
# Job <md5sum XXX/x.sh> was submitted from host <frt> by user <rmurri>
# in cluster <prdclst>.  Job was executed on host(s) <cpt086>, in
# queue <normal>, as user <rmurri> in cluster <prdclst>.
# </home/rmurri> was used as the home directory.  </home/rmurri> was
# used as the working directory.  Started at Mon Oct 8 17:23:30 2012
# Results reported at Mon Oct 8 17:23:31 2012
#
# Your job looked like:
#
# ------------------------------------------------------------
# LSBATCH: User input
# md5sum XXX/x.sh
# ------------------------------------------------------------
#
# Successfully completed.
#
# Resource usage summary:
#
#     CPU time   :      0.01 sec.
#     Max Memory :         2 MB
#     Max Swap   :        24 MB
#
#     Max Processes  :         1
#     Max Threads    :         1
#
# The output (if any) follows:
#
# b7f9d9c86469aa8b57c19af14bf80af9  XXX/x.sh
###############################################################################


if __name__ == "__main__":
    import pytest
    pytest.main(["-v", __file__])
