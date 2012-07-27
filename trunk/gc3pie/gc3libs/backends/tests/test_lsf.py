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

def test_bjobs_output_done():
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
