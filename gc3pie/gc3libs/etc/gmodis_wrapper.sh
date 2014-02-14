#!/bin/bash
#
# gmodis.sh -- base wrapper script for executing MODIS function
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
  $PROG [options] <input FSC .mat file>

Options:
  -x <gmodis binary>\t\tPath to gmodis binary executable
  -f <FSC snowline input>\t\Path to FSC snowline input folder
  -s <Matlab driver script>]tPath to alternative Matlab driver script to execute gmodis
  -o <output_folder>\t\tOutput folder
  -d  enable debug

EOF`

# Set system utilities
TODAY=`date +%Y-%m-%d`
CUR_DIR="$PWD"

# Set default values
DEBUG=0
FAILED=0
MATLAB_COMPILED_SCRIPT="$HOME/bin/gmodis_single"
FSC_LOAD_FOLDER="$HOME/default_input_snowline_data"
OUTPUT_FOLDER="./output/"
MATLAB_DRIVER_SCRIPT="$HOME/bin/run_compiled_matlab.sh"

while getopts "x:f:s:o:d" opt; do
    case $opt in
	d )
	    DEBUG=1
	    ;;
	x)
	    # Define gmodis binary
            MATLAB_COMPILED_SCRIPT=$OPTARG
	    ;;
	f)
	    # shift
	    FSC_LOAD_FOLDER=$OPTARG
	    ;;
	s)
	    # shift
	    MATLAB_DRIVER_SCRIPT=$OPTARG
	    ;;
	o)
	    # shift
	    OUTPUT_FOLDER=$OPTARG
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
INPUT_FSC=$1

echo "[`date +%Y-%m-%d" "%H:%M:%S`] Start"
log "MATLAB_DRIVER_SCRIP: ${MATLAB_DRIVER_SCRIPT}"
log "MATLAB_COMPILED_SCRIPT: ${MATLAB_COMPILED_SCRIPT}"
log "FSC_LOAD_FOLDER ${FSC_LOAD_FOLDER}"
log "INPUT_FSC ${INPUT_FSC}"
log "OUTPUT_FOLDER: ${OUTPUT_FOLDER}"

echo -n "MATLAB_DRIVER_SCRIPT... "
if [ -x ${MATLAB_DRIVER_SCRIPT} ]; then
    echo "[ok]"
else
    echo "[Command not found]"
    exit 127 # Command not found
fi

echo -n "MATLAB_COMPILED_SCRIPT... "
if [ -x ${MATLAB_COMPILED_SCRIPT} ]; then
    echo "[ok]"
else
    echo "[Command not found]"
    exit 127 # Command not found
fi

echo -n "FSC_LOAD_FOLDER... "
if [ -d ${FSC_LOAD_FOLDER} ]; then
    echo "[ok]"
else
    echo "[Folder not found]"
    exit 1 
fi

echo -n "INPUT_FSC... "
if [ -e ${INPUT_FSC} ]; then
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

# Check options

# echo configuration

# run script
echo "Running: ${MATLAB_DRIVER_SCRIPT} ${MATLAB_COMPILED_SCRIPT} ${FSC_LOAD_FOLDER} ${INPUT_FSC} ${OUTPUT_FOLDER}"
${MATLAB_DRIVER_SCRIPT} ${MATLAB_COMPILED_SCRIPT} ${FSC_LOAD_FOLDER} ${INPUT_FSC} ${OUTPUT_FOLDER}
RET=$?

log "Simulation ended with exit code $RET"

# Prepare result
echo "[`date +%Y-%m-%d" "%H:%M:%S`] Done"

exit $RET

