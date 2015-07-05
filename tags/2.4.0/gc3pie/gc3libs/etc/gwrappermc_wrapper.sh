#!/bin/bash
#
# gwrappermc_wrapper.sh -- base wrapper script for executing Adimat functions
# 
# Authors: Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>
#
# Copyright (c) 2013-2014 GC3, University of Zurich, http://www.gc3.uzh.ch/ 
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

## helper functions
function log {
    if [ $DEBUG -ne 0 ]; then
	echo "DEBUG: $1"
    fi
}

function die () {
  rc="$1"
  shift
  (echo -n "$PROG: ERROR: ";
      if [ $# -gt 0 ]; then echo "$@"; else cat; fi) 1>&2
  exit $rc
}

## Parse command line options.
USAGE=`cat <<EOF
Usage: 
  $PROG [options] <input .csv file> <result folder>

Options:
  -d  enable debug
  -m  matlab Main_Loop location
EOF`

# Set system utilities
TODAY=`date +%Y-%m-%d`
CUR_DIR="$PWD"
MAIN_LOOP_LOCATION="./"

# Set default values
DEBUG=0
FAILED=0
INDEX_CHUNK=0

while getopts "m:i:d" opt; do
    case $opt in
	i )
	    INDEX_CHUNK=$OPTARG
	    ;;
	m )
	    MAIN_LOOP_LOCATION=$OPTARG
	    ;;
	d )
	    DEBUG=1
	    ;;
    esac
done

# Remove the switches we parsed above.
shift `expr $OPTIND - 1`

# We want 2 non-option argument:
# 1. Input folder
# 2. Result folder
if [ ! $# -eq 2 ]; then
    die 1 "$USAGE"
fi

# Check input arguments
INPUT=$1
RESULT_FOLDER=$2

echo "[`date +%Y-%m-%d" "%H:%M:%S`] Start"

log "INPUT ${INPUT}"
log "RESULT_FOLDER: ${RESULT_FOLDER}"

# Check options

# echo configuration

# run script
echo "Running: matlab -nodesktop -nodisplay -nosplash -r \'addpath('$MAIN_LOOP_LOCATION'); Main_Loop input.csv results; quit()\'"
matlab -nodesktop -nodisplay -nosplash -r "addpath('$MAIN_LOOP_LOCATION'); Main_Loop $INPUT $RESULT_FOLDER $INDEX_CHUNK; quit()"
RET=$?

log "Simulation ended with exit code $RET"

# Prepare result
echo "[`date +%Y-%m-%d" "%H:%M:%S`] Done"

exit $RET

