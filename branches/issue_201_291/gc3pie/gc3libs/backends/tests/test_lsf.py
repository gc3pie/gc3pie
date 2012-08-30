#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011, 2012, GC3, University of Zurich. All rights reserved.
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
import sys
import tempfile

import gc3libs
import gc3libs.core
import gc3libs.config
from gc3libs.backends.lsf import LsfLrms

from nose.tools import assert_equal


def test_get_command():
    (fd, tmpfile) = tempfile.mkstemp()
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
""")
    f.close()

    cfg = gc3libs.config.Configuration()
    cfg.merge_file(tmpfile)
    b = cfg.make_resources()['example']

    assert_equal(b.bsub, ['/usr/local/bin/bsub', '-R', 'lustre'])

    assert_equal(b._bjobs,   '/usr/local/bin/bjobs')
    assert_equal(b._lshosts, '/usr/local/sbin/lshosts')

def test_bjobs_output_done1():
    jobstatus = LsfLrms._parse_stat_output("""
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

 SCHEDULING PARAMETERS:
           r15s   r1m  r15m   ut      pg    io   ls    it    tmp    swp    mem
 loadSched   -     -     -     -       -     -    -     -  1000M     -      -
 loadStop    -     -     -     -       -     -    -     -     -      -      -

          scratch      xs       s       m       l      xl      sp
 loadSched 4000.0      -       -       -       -       -       -
 loadStop      -       -       -       -       -       -       -
""")
    assert_equal(jobstatus.state, gc3libs.Run.State.TERMINATING)
    assert_equal(jobstatus.exit_status, 0)


def test_bjobs_output_done2():
    jobstatus = LsfLrms._parse_stat_output("""
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
""")
    assert_equal(jobstatus.state, gc3libs.Run.State.TERMINATING)
    assert_equal(jobstatus.exit_status, 0)


def test_bjobs_output_exit_nonzero():
    jobstatus = LsfLrms._parse_stat_output("""
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
""")
    assert_equal(jobstatus.state, gc3libs.Run.State.TERMINATING)
    assert_equal(jobstatus.exit_status, 42)


if __name__ == "__main__":
    import nose
    nose.runmodule()
