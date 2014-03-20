#!/bin/bash
#
# smd_projections_wrapper.sh -- base wrapper script for executing smd projections 
# 
# Authors: Tyanko Aleksiev <tyanko.aleksiev@chem.uzh.ch>
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
function usage () {
cat <<EOF
Usage: 
  $PROG [options] input_tar projections_directory_name 

Options:
   --help/-h Prints this help menu
   -d enable debug
EOF
}

# Set system utilities
TODAY=`date +%Y-%m-%d`
CUR_DIR="$PWD"

# Set default values
DEBUG=0
FAILED=0
## parse command-line 
NEW_CALIBRATION="NO"

if [ "x$(getopt -T)" == 'x--' ]; then
    # old-style getopt, use compatibility syntax
    set -- $(getopt 'h' "$@")
else
    # GNU getopt
    args=$(getopt --shell sh -l 'help' -o 'hdb' -- "$@")
    # need `eval` here to remove quotes added by GNU `getopt`
    eval set -- $args
fi
while [ $# -gt 0 ]; do
    case "$1" in
        --help|-h) usage; exit 0 ;;
	-d) DEBUG=1 ;;
    -b) NEW_CALIBRATION="YES" ;; 
	--) shift; break ;;
    esac
    shift
done


# Copy the R scripts need to start the simulation in the local execution directory.

cp ~/SDM_projections.R .

if [ "$NEW_CALIBRATION" == "NO" ]; then
    cp -a ~/SDMs_calibration .
else 
    tar -xvf ./calibration.tar 
fi

echo "[`date +%Y-%m-%d" "%H:%M:%S`] Start"

# Prepare the input files
log "Prepare the input by decompressing the archive"
tar -xvf $1
if [ $? -eq 0 ]; then
 log "Output prepared correctly" 
else
 log "Some errors occured when preparing the input" 
fi  

# run R script
Rscript --vanilla SDM_projections.R $2 SDMs_calibration 
RET=$?

rm -rf ./SDMs_calibration ./SDM_projections.R ./*.tar ./$2 

log "Simulation ended with exit code $RET"

# Prepare result
echo "[`date +%Y-%m-%d" "%H:%M:%S`] Done"

exit $RET

