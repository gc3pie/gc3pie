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

OVERWRITE="--keep-old-files"
GEOTOP_EXEC="GEOtop_1.223_static"
OUTPUT_ARCHIVE="output.tgz"
TAR_EXCLUDE_PATTERN="--exclude .arc --exclude ./in --exclude ggeotop.log"

#=============

function gracefull_exit {
    echo -n "Creating output archive... "
    tar -czf $OUTPUT_ARCHIVE ./* $TAR_EXCLUDE_PATTERN
    if [ $? -eq 0 ]; then
        # Remove everything else if tar has been created successfully
	echo "[ok]"
	echo "Cleaning... "
	ls | grep -v output.tgz | grep -v .log | grep -v .arc | xargs --replace rm -rf {}
    else
	echo "[failed]"
    fi
}

USAGE="Usage: `basename $0` [-w] <input archive> <GEOTop executable>"

# Parse command line options.
while getopts w OPT; do
    case "$OPT" in
        w)
            OVERWRITE="--overwrite"
            ;;
        \?)
            # getopts issues an error message
            echo $USAGE >&2
	    gracefull_exit
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
    gracefull_exit
    exit 1
fi

INPUT_ARCHIVE=$1
GEOTOP_EXEC=$2

# Check INPUT_ARCHIVE
echo -n "Checking input archive [$INPUT_ARCHIVE] ... "
if [ ! -r $INPUT_ARCHIVE ]; then
    echo "[failed]"
    gracefull_exit
    exit 1
else
    echo "[ok]"
fi

echo -n "Running: tar $OVERWRITE -xzf $INPUT_ARCHIVE ... "
tar $OVERWRITE -xzf $INPUT_ARCHIVE
RET=$?
rm $INPUT_ARCHIVE

if [ $RET -ne 0 ]; then
    echo "[failed]"
    gracefull_exit
    exit $RET
else
    echo "[ok]"
fi

## Execute the GEOTOP code ##

echo -n "Checking executable [$GEOTOP_EXEC] ... "

if [ -x $GEOTOP_EXEC ]; then
    echo "[ok]"
    echo "Start execution... "
    # why is it not redirecting to stdout/stderr ?
    $GEOTOP_EXEC .
    RET=$?
    echo "GEOTop execution termianted with [$RET]"
    rm $GEOTOP_EXEC
    gracefull_exit
    exit $RET
else
    echo "[failed]"
    gracefull_exit
    exit 1
fi
