#!/bin/bash
#
# gbenchmark_infomap_wrapper.sh -- base wrapper script for executing Infomap 
# benchmark applications
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

# Default is python
BENCHMARK="python"

INFOMAP_PYTHON="/home/gc3-user/bin/run.py"
INFOMAP_R="/home/gc3-user/bin/run.R"
INFOMAP_CPP="Infomap"

infomap_python_command="python ${INFOMAP_PYTHON} "
infomap_cpp_command="${INFOMAP_CPP} --input-format=link-list --tree --map --clu "
infomap_R_command="Rscript --vanilla ${INFOMAP_R} "

RUN_SCRIPT=""
RESULT_FOLDER="./results"

DEBUG=0

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
  $PROG [options] <input .dat file>

Options:
  -b <benchmark>\tBenchmark to run
  -d  enable debug

EOF`

while getopts "r:b:d" opt; do
    case $opt in
	r )
	    # Run the following script instead of the default one
	    RUN_SCRIPT=$OPTARG
	    ;;
	d )
	    DEBUG=1
	    ;;
	b )
	    # Clustering threshold as a decimal.
            BENCHMARK=$OPTARG
	    ;;
	* )
	    die 1 "Unrecognised option" 
	    ;;
    esac
done

# Remove the switches we parsed above.
shift `expr $OPTIND - 1`

# We want 1 non-option argument:
# 1. Network data file
if [ ! $# -eq 1 ]; then
    die 1 "$USAGE"
fi

# Check input data
NETWORK_FILE=$1

echo "[`date +%Y-%m-%d" "%H:%M:%S`] Start"

# Check input network file
echo -n "NETWORK_FILE... "
if [ -e ${NETWORK_FILE} ]; then
    echo "[ok]"
else
    echo "[Network file not found]"
    exit 1 
fi

# Check output folder
echo -n "RESULT_FOLDER... "
if [ -d ${RESULT_FOLDER} ]; then
    echo "[ok]"
else
    # create it
    log "Creating output folder ${RESULT_FOLDER}"
    mkdir -p ${RESULT_FOLDER}
    echo "[ok]"
fi

# Define command to run depending on the selected benchmark
if [ "${RUN_SCRIPT}x" == "x" ]; then
    case $BENCHMARK in
	"python" )
	    command="$infomap_python_command $NETWORK_FILE $RESULT_FOLDER"
	    ;;
	"cpp")
	    command="$infomap_cpp_command $NETWORK_FILE $RESULT_FOLDER"
	    ;;
	"r")
	    command="$infomap_R_command $NETWORK_FILE $RESULT_FOLDER"
	    ;;
	*)
	    die 1 "Not supported benchmark format: '$BENCHMARK'"
	    ;;
    esac
else
    command="${RUN_SCRIPT} $NETWORK_FILE $RESULT_FOLDER"
fi

log "Network file: $NETWORK_FILE"
log "Result folder: $RESULT_FOLDER"
log "Benchmark to run: $BENCHMARK"
log "-----------------------------"

log "Running $command"
$command
RET=$?

log "Simulation ended with exit code $RET"

# Prepare result
echo "[`date +%Y-%m-%d" "%H:%M:%S`] Done"

exit $RET




