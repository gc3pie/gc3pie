#!/bin/bash -x
#
# gperm_wrapper.sh -- base wrapper script for executing Docker BIDS apps
# 
# Authors: Sergio Maffioletti <sergio.maffioletti@uzh.ch>
#
#   Copyright (C) 2017, 2018 S3IT, University of Zurich
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
DEBUG=1

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
  $PROG <subject folder> <subject name> <result folder> <freesurfer license file> <docker image> [<docker args>]
EOF`

# We want >5 non-option argument:
if [ $# < 5 ]; then
    die 1 "$USAGE"
fi

# Check input arguments
SUBJECT_DIR=$PWD/$1
shift
SUBJECT_NAME=$1
shift
OUTPUT_DIR=$PWD/$1
shift
FREESURFER_LICENSE=$1
shift
DOCKER_TO_RUN=$1
shift
DOCKER_ARGS=$@

# Create output folder if not present
if ![ -d $OUTPUT_DIR ]; then
    mkdir -p OUTPUT_DIR
fi

echo "[`date +%Y-%m-%d" "%H:%M:%S`] Start"

log "Subject dir: ${SUBJECT_DIR}"
log "Subject name: ${SUBJECT_NAME}"
log "Output dir: ${OUTPUT_DIR}"
log "Using Docker image: ${DOCKER_TO_RUN}"
log "Using Docker arguments: ${DOCKER_ARGS}"

# Define mount points
DOCKER_MOUNT="-v ${SUBJECT_DIR}:/bids:ro -v ${OUTPUT_DIR}:/output"
if [ -n "$freesurfer_license" ]; then
    DOCKER_MOUNT+="-v $freesurfer_license:/opt/freesurfer/license.txt"
fi

# run script
echo "docker run -i --rm ${DOCKER_MOUNT} ${DOCKER_TO_RUN} /bids /output participant --participant_label ${SUBJECT_NAME} ${DOCKER_ARGS}"
docker run -i --rm ${DOCKER_MOUNT} ${DOCKER_TO_RUN} /bids /output participant --participant_label ${SUBJECT_NAME} ${DOCKER_ARGS}
RET=$?

log "Simulation ended with exit code $RET"

# Prepare result
echo "[`date +%Y-%m-%d" "%H:%M:%S`] Done"

exit $RET

