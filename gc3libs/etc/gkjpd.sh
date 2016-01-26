#!/bin/bash
#
# gscuafish.sh -- wrapper script for executing gscuafish Matalb code
#
# Authors: Sergio Maffioletti <sergio.maffioletti@uzh.ch>
#
# Copyright (c) 2015 S3IT, University of Zurich, http://www.s3it.uzh.ch/
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

#!/bin/bash

command="preprocessing_s3it_adi"
echo "[`date`] Start"

INPUT=""
for var in "$@"
do
    INPUT=$INPUT"'"$var"',"
done
INPUT=${INPUT%?}

echo "matlab -nodesktop -nosplash -nodisplay -r \"addpath('input');$command($INPUT);quit\""
matlab -nodesktop -nosplash -nodisplay -r "addpath('input');$command($INPUT);quit"
ret=$?

echo "[`date`] Stop"

exit $ret

