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

# Assume shared FS by default
SHARED_FS=1

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

## Parse command line options.
USAGE=`cat <<EOF
Usage: $PROG [-w -l] <input> <GEOTop executable>
where options include:
      -w        overwrite


EOF`

while getopts "wh" opt; do
    case $opt in
	w)
	    # set overrite flag when opening tar input archive
            OVERWRITE="--overwrite"
	    ;;
	h)
	    echo "$USAGE"
	    exit 1
	    ;;
	\?)
	    usage
	    exit 1
	    ;;
    esac
done

# Remove the switches we parsed above.
shift `expr $OPTIND - 1`

# We want 2 non-option argument:
# 1. Input folder
# 2. GEOTop executable
# Remove this block if you don't need it.
if [ ! $# -eq 2 ]; then
    die 1 "$USAGE"
fi

INPUT=$1
GEOTOP_EXEC=$2

# Determine input type
# Allowed input types:
# application/x-tar
# inode/directory
# 
# This will also instruct the script whether to use 
# a shared filesystem or not

echo -n "Resolving filesystem layout... "
MIME_TYPE=`file --mime-type -b $INPUT 2>&1`
if [ ! $? -eq 0 ]; then
    echo "[failed]"
    die 1 $MIME_TYPE
else
    if [ $MIME_TYPE == "application/x-gzip" ] || [ $MIME_TYPE == "application/gzip" ]; then
	SHARED_FS=0
	echo "[NON shared]"
    elif [ $MIME_TYPE == "inode/directory" ]; then
	# Input is a folder in a shared filesystem
	SHARED_FS=1
	echo "[shared]"
    else
	echo "[failed: $MIME_TYPE]"
	die 1 "Unsupported mime-type $MIME_TYPE"
    fi
fi

# Check INPUT_ARCHIVE
echo -n "$PROG: Checking input [$INPUT] ... "

if [ $SHARED_FS -eq 1 ]; then
    # Use local filesystem. No input archive
    # Check whether input is a valid folder
    if [ ! -d  $INPUT ]; then
	echo "[failed]"
	die 1 "Cannot read input folder '$INPUT', aborting."
    else
	WORKING_DIR=$INPUT
	echo "[ok]"
    fi
else
    # Use shared filesystem
    # check whether input is a valid 
    # tar archive
    if [ ! -r $INPUT ]; then
	echo "[failed]"
	die 1 "Cannot read input archive file '$INPUT', aborting."
    else
	echo "[ok]"
	# Now untar input archive and set working_dir to
	# extracted path
	echo -n "$PROG: Running: tar $OVERWRITE -xzf $INPUT ... "
	tar $OVERWRITE -xzf $INPUT
	RET=$?
	if [ $RET -ne 0 ]; then
	    echo "[failed]"
	    die $RET "Could not extract files from input archive '$INPUT', aborting."
	else
	    echo "[ok]"
	fi
	# Cleanup input archive
	# XXX: is it really necessary ?
	rm -fv $INPUT
	WORKING_DIR=$PWD
    fi
fi

# Set cleanup on exit for generating output archive
# in case localFS is set to False
if [ $SHARED_FS -eq 0 ]; then
    trap "cleanup_on_exit" EXIT TERM INT
fi

## Execute the GEOTOP code ##

echo -n "$PROG: Checking GEOTop executable [$GEOTOP_EXEC] ... "
if [ -x $GEOTOP_EXEC ]; then
    echo "[ok]"
    echo "$PROG: Starting execution of $GEOTOP_EXEC ... "
    $GEOTOP_EXEC $WORKING_DIR
    RET=$?
    echo "$PROG: GEOtop execution terminated with exit code [$RET]"
    # rm -fv $GEOTOP_EXEC
    exit $RET
else
    echo "[failed]"
    exit 127 # command not found
fi
