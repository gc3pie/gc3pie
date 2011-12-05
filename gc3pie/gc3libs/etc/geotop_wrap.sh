#! /bin/bash
#
# GEO_WRAP -- base wrapper script for executing GEOTOP
# 
# Author: Tyanko Aleksiev <tyanko.aleksiev@oci.uzh.ch>
#
# Copyright (c) 201 OCI UZH http://www.oci.uzh.ch/ 
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
#-----------------------------------------------------------------------

## Prepare the environment for the execution. Untar the input.tgz file ##

#export LOG_FILE=GTLog 
export WORK_DIR=`pwd`
OVERWRITE="--keep-old-files"
GEOTOP_EXEC="GEOtop_1.223_static"

#=============

USAGE="Usage: `basename $0` [-w] args"

# Parse command line options.
while getopts w OPT; do
    case "$OPT" in
        w)
            OVERWRITE="--overwrite"
            ;;
        \?)
            # getopts issues an error message
            echo $USAGE >&2
            exit 1
            ;;
    esac
done

# Remove the switches we parsed above.
shift `expr $OPTIND - 1`

# We want at least one non-option argument.
# Remove this block if you don't need it.
if [ $# -eq 0 ]; then
    echo $USAGE >&2
    exit 1
fi

INPUT_ARCHIVE=$1

echo "Untar the input file"

tar $OVERWRITE -xzvf $INPUT_ARCHIVE -C .
RET=$?
rm $INPUT_ARCHIVE

if [ $RET -ne 0 ]; then
 echo "[failed]"
 exit $RET
fi 

## Execute the GEOTOP code ##

# check whether executable is there

if [ -x ./$GEOTOP_EXE ]; then
    echo "Start the code execution"
    ./$GEOTOP_EXE .
    RET=$? 
    rm ./$GEOTOP_EXE
    # Create output archive 
    tar -czf output.tar.gz ./* 
    exit $RET
else
    echo "$GEOTOP_EXE not found"
    exit 1
fi




