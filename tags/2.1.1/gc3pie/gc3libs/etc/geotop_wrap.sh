#! /bin/bash
#
# geotop_wrap.sh -- base wrapper script for executing GEOTOP
# 
# Author: Tyanko Aleksiev <tyanko.aleksiev@oci.uzh.ch>
#
# Copyright (c) 2011-2012 GC3, University of Zurich, http://www.gc3.uzh.ch/ 
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
PROG=$(basename "$0")

## configuration defaults

OVERWRITE="--keep-old-files"
OUTPUT_ARCHIVE="output.tgz"
TAR_EXCLUDE_PATTERN="--exclude .arc --exclude ./in --exclude ggeotop.log"
tag_files='_SUCCESSFUL_RUN _FAILED_RUN'

## helper functions
function die () {
  rc="$1"
  shift
  (echo -n "$PROG: ERROR: ";
      if [ $# -gt 0 ]; then echo "$@"; else cat; fi) 1>&2
  exit $rc
}

function cleanup_on_exit {
    echo -n "$PROG: Creating output archive... "
    # be sure to include tag files, wherever they are located
    actual_tag_files=''
    for file in $tag_files; do 
        if [ -r "$file" ]; then
            actual_tag_files="$actual_tag_files $file"
        fi
    done
    tar czvf $OUTPUT_ARCHIVE out/ $actual_tag_files $TAR_EXCLUDE_PATTERN
    if [ $? -eq 0 ]; then
        # Remove everything else if tar has been created successfully
        echo "[ok]"
        echo "$PROG: Cleaning up temporary files... "
        rm -rfv in
        rm -rfv out
        ls | egrep -v "($OUTPUT_ARCHIVE|.log|.arc)" | xargs --replace rm {}
    else
        echo "[failed]"
    fi
}
trap "cleanup_on_exit" EXIT TERM INT


## Parse command line options.
USAGE="Usage: $PROG [-w] <input archive> <GEOTop executable>"
while getopts w OPT; do
    case "$OPT" in
        w)
            OVERWRITE="--overwrite"
            ;;
        \?)
            # getopts issues an error message
            die 1 "$USAGE"
            ;;
    esac
done

# Remove the switches we parsed above.
shift `expr $OPTIND - 1`

# We want at least one non-option argument.
# Remove this block if you don't need it.
if [ $# -eq 0 ]; then
    die 1 "$USAGE"
fi

INPUT_ARCHIVE=$1
GEOTOP_EXEC=$2

# Check INPUT_ARCHIVE
echo -n "$PROG: Checking for presence of input archive [$INPUT_ARCHIVE] ... "
if [ ! -r $INPUT_ARCHIVE ]; then
    echo "[failed]"
    die 1 "Cannot read input archive file '$INPUT_ARCHIVE', aborting."
else
    echo "[ok]"
fi

echo -n "$PROG: Running: tar $OVERWRITE -xzf $INPUT_ARCHIVE ... "
tar $OVERWRITE -xzf $INPUT_ARCHIVE
RET=$?
rm -fv $INPUT_ARCHIVE
if [ $RET -ne 0 ]; then
    echo "[failed]"
    die $RET "Could not extract files from input archive '$INPUT_ARCHIVE', aborting."
else
    echo "[ok]"
fi

## Execute the GEOTOP code ##

echo -n "$PROG: Checking for presence of executable [$GEOTOP_EXEC] ... "
if [ -x $GEOTOP_EXEC ]; then
    echo "[ok]"
    echo "$PROG: Starting execution of $GEOTOP_EXEC ... "
    $GEOTOP_EXEC .
    RET=$?
    echo "$PROG: GEOtop execution terminated with exit code [$RET]"
    rm -fv $GEOTOP_EXEC
    exit $RET
else
    echo "[failed]"
    exit 126 # command not found
fi
