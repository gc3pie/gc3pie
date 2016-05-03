#!/bin/bash
#
# gcombi -- wrapper script for executing gcombi Matalb code
#
# Authors: Tyanko Aleksiev <tyanko.aleksiev@uzh.ch>
#
# Copyright (c) 2016 S3IT, University of Zurich, http://www.s3it.uzh.ch/
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#

mkdir ./results
rm ./results/*

export MATLABPATH=/home/ubuntu/CombiMethod/3_pemutation:/home/ubuntu/CombiMethod/4_COMBI:/home/ubuntu/CombiMethod/computation_scripts:/home/ubuntu/CombiMethod/DAL:/home/ubuntu/CombiMethod/RPVT_perm

echo "[`date`] Start"
echo "matlab -nodesktop -nodisplay -nosplash -r \"wrapper_permtest_RPVT '$1' '$2' ;quit\""
matlab -nodesktop -nodisplay -nosplash -r "wrapper_permtest_RPVT '$1' '$2';quit"
ret1=$?
echo "wrapper_permtest_RPVT exited with $ret1 status"

echo "matlab -nodesktop -nodisplay -nosplash -r \"wrapper_permtest '$1' '$2' ;quit\""
matlab -nodesktop -nodisplay -nosplash -r "wrapper_permtest '$1' '$2'; quit"
ret2=$?
echo "wrapper_permtest exited with $ret2 status"

echo "matlab -nodesktop -nodisplay -nosplash -r \"wrapper_COMBI '$1' '$2' ;quit\""
matlab -nodesktop -nodisplay -nosplash -r "wrapper_COMBI '$1' '$2'; quit"
ret3=$?
echo "wrapper_permtest_COMBI exited with $ret3 status"
echo "[`date`] Stop"

mv /data/* ./results/

exit $ret
