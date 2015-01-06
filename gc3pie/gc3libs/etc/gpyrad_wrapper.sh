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

# Set system utilities
TODAY=`date +%Y-%m-%d`
CUR_DIR="$PWD"

# Check for pyRAD location
if [ "${PYRAD_LOCATION}x" == "x" ]; then
    # No pyRAD location set. Use default
    PYRAD_LOCATION="$HOME/pyRAD"
fi  

S3CMD=`which s3cmd`
S3CFG="./etc/s3cfg"

# Set default values
DEBUG=0
FAILED=0
#XXX: check whether this is corrent or need more test
# PYRAD="$PYRAD_LOCATION/pyRAD"
PYRAD=`which pyRAD`
OUTPUT_FOLDER="./output/"
INPUT_FOLDER="./input"
WCLUST=0.9
PARAMS_FILE="$PYRAD_LOCATION/params.tmpl"


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

function verify_s3 {
    echo "Dumping S3cmd configuration"
    $S3CMD -c $S3CFG --dump-config 1>/dev/null 2>&1
    return $?
}

function get_s3_bucket {
    S3_URL=$1
    LOCAT_DES=$2

    log "$S3CMD -c $S3CFG --check-md5 sync ${S3_URL} ${LOCAL_DEST}"
    $S3CMD -c $S3CFG sync ${S3_URL} ${LOCAL_DEST}
    return $?
}

function sync_s3_bucket {
    LOCAL_FOLDER=$1
    S3_URL=$2

    log "$S3CMD -c $S3CFG sync ${LOCAL_FOLDER} ${S3_URL}"
    $S3CMD -c $S3CFG put ${LOCAL_FOLDER} ${S3_URL}
    return $?
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
log "Checking input archive... "
case ${INPUT_FASTQ} in
    "s3://"* )
	# S3 URL
	log "S3 archive type"
	verify_s3
	if [ $? -ne 0 ]; then
	    echo "CRITICAL: Cannot use s3cmd command."
	    echo "Please check s3cfg configuration."
	    exit 1
	fi
	log "Downloading fastq from S3... "
	get_s3_bucket ${INPUT_FASTQ} .
	if [ $? -ne 0 ]; then
	    echo "CRITICAL: Failed getting input fastq"
	    exit 1
	fi
	log "Download completed."
	;;
    "/"*|"./"* )
	# Local archive
	log "Local archive"
	;;
     * )
	echo "Not supported input archive format"
	exit 1
	;;
esac

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
# copy params file
cp ${PARAMS_FILE} ./params.txt
PARAMS_FILE="./params.txt"

log "Customizing params file adding wclust value"
sed -i -e "s|@PYRAD@|${PYRAD_LOCATION}|g" ${PARAMS_FILE}
sed -i -e "s|@WCLUST@|${WCLUST}|g" ${PARAMS_FILE}
sed -i -e "s|@INPUT@|${INPUT_FOLDER}|g" ${PARAMS_FILE}


# echo configuration

# run script
log "Running: ${PYRAD} -p ${PARAMS_FILE} -s23"
if [ $DEBUG -ne 0 ]; then
    log "Running: strace -f -o strace.log ${PYRAD} -p ${PARAMS_FILE} -s23"
    strace -f -o strace.log ${PYRAD} -p ${PARAMS_FILE} -s23
else
    log "Running: ${PYRAD} -p ${PARAMS_FILE} -s23"
    ${PYRAD} -p ${PARAMS_FILE} -s23
fi

RET=$?

log "Simulation ended with exit code $RET"

# Prepare result
echo "[`date +%Y-%m-%d" "%H:%M:%S`] Done"

exit $RET

