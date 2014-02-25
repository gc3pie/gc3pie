#!/bin/bash
#
# gpyrad.sh -- base wrapper script for executing MODIS function
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
  $PROG [options] <input .fastq file>

Options:
  -w <wclust>\t\tClustering threshold as a decimal.
  -p <params.txt file>\t\tPath to params.txt file required by pyrad
  -d  enable debug

EOF`

# Set system utilities
TODAY=`date +%Y-%m-%d`
CUR_DIR="$PWD"

# Set default values
DEBUG=0
FAILED=0
#XXX: check whether this is corrent or need more test
PYRAD=`which pyrad`
OUTPUT_FOLDER="./output/"
WCLUST=0.9
PARAMS_FILE="./params.txt"

while getopts "w:p:d" opt; do
    case $opt in
	d )
	    DEBUG=1
	    ;;
	w )
	    # Clustering threshold as a decimal.
            WCLUST=$OPTARG
	    ;;
	p )
	    # Path to params.txt file required by pyrad
	    PARAMS_FILE=$OPTARG
	    ;;
    esac
done

# Remove the switches we parsed above.
shift `expr $OPTIND - 1`

# We want 2 non-option argument:
# 1. Input folder
# 2. GEOTop executable
# Remove this block if you don't need it.
if [ ! $# -eq 1 ]; then
    die 1 "$USAGE"
fi

# Check input data
INPUT_FASTQ=$1

echo "[`date +%Y-%m-%d" "%H:%M:%S`] Start"
log "PYRAD executable: ${PYRAD}"
log "WCLUST: ${WCLUST}"
log "PARAMS_FILE: ${PARAMS_FILE}"

echo -n "PYRAD... "
if [ -x ${PYRAD} ]; then
    echo "[ok]"
else
    echo "[Command not found]"
    exit 127 # Command not found
fi

echo -n "PARAM_FILE... "
if [ -e ${PARAM_FILE} ]; then
    echo "[ok]"
else
    echo "[File not found]"
    exit 1 
fi

echo -n "OUTPUT_FOLDER... "
if [ -d ${OUTPUT_FOLDER} ]; then
    echo "[ok]"
else
    # create it
    log "Creating output folder ${OUTPUT_FOLDER}"
    mkdir -p ${OUTPUT_FOLDER}
    echo "[ok]"
fi

# Customize param file

log "Customizing params file adding wclust value"
sed -i -e 's/WCLUST/${WCLUST}/g' ${PARAMS_FILE}

# echo configuration

# run script
log "Running: ${PYRAD} -p ${PARAMS_FILE} -s123"
${PYRAD} -p ${PARAMS_FILE} -s123
RET=$?

log "Simulation ended with exit code $RET"

# Prepare result
echo "[`date +%Y-%m-%d" "%H:%M:%S`] Done"

exit $RET

