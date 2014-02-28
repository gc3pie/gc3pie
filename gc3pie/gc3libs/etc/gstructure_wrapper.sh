#!/bin/bash
#
# gstructure_wrapper.sh -- base wrapper script for executing Structure programm
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
  $PROG [options] nloc nind input_file 

Options:
   --help/-h Prints this help menu
   -p Use different mainparams file 
   -x Use different extraparams file
   -e Structure replicates.
   -r Structure K-range 
   -u Strucutre output file
   -d enable debug
EOF
}

# Set system utilities
TODAY=`date +%Y-%m-%d`
CUR_DIR="$PWD"

# Set default values
DEBUG=0
FAILED=0
STRUCTURE=`which structure`


## parse command-line 

if [ "x$(getopt -T)" == 'x--' ]; then
    # old-style getopt, use compatibility syntax
    set -- $(getopt 'h' "$@")
else
    # GNU getopt
    args=$(getopt --shell sh -l 'help' -o 'hdp:x:u:r:e:' -- "$@")
    # need `eval` here to remove quotes added by GNU `getopt`
    eval set -- $args
fi
while [ $# -gt 0 ]; do
    case "$1" in
        --help|-h) usage; exit 0 ;;
	-d) DEBUG=1 ;; 
	-p) MAINPARAM_FILE="$2" ;;
	-x) EXTRAPARAM_FILE="$2" ;;
	-u) OUTPUT_FILE="$2" ;; 
	-r) K_RANGE="$2" ;;
	-e) REPLICA="$2" ;;
	--) shift; break ;;
    esac
    shift
done

# get the number of cores
NCORE=`/usr/bin/nproc` 

echo "[`date +%Y-%m-%d" "%H:%M:%S`] Start"
log "STRUCTURE executable: ${STRUCTURE}"
log "main param file: ${MAINPARAM_FILE}"
log "extra param file: ${EXTRAPARAM_FILE}"
log "output file: ${OUTPUT_FILE}"
log "k range: ${K_RANGE}"
log "replica: ${REPLICA}"

echo -n "STRUCTURE... "
if [ -x ${STRUCTURE} ]; then
    echo "[ok]"
else
    echo "[Command not found]"
    exit 127 # Command not found
fi

echo -n "MAIN PARAM_FILE... "
if [ -e ${MAINPARAM_FILE} ]; then
    echo "[ok]"
else
    echo "[File not found]"
    exit 1 
fi

echo -n "EXTRA PARAM FILE... "
if [ -d ${EXTRAPARAM_FILE} ]; then
    echo "[ok]"
else
    echo "[File not found]"
    exit 1
fi

# Customize pms_usecase_structure.R file based on the specified arguments and options

log "Customizing the file with the passed arguments and options"
sed -i "s/NLOC/$1/g" pms_usecase_structure.R 
sed -i "s/NIND/$2/g" pms_usecase_structure.R
sed -i "s/INPUT_FILE/$3/g" pms_usecase_structure.R
sed -i "s/output.out/$OUTPUT_FILE/g" pms_usecase_structure.R
sed -i "s/KRANGE/$K_RANGE/g" pms_usecase_structure.R
sed -i "s/REPLICA/$REPLICA/g" pms_usecase_structure.R
sed -i "s/CORES/$NCORE/g" pms_usecase_structure.R

#FIXME: Run dos2unix command on the input fils and param files
#FIXME: Manage the cases in which mainparams and extraparams files are copied.

# echo configuration

# run R script
R CMD BATCH --no-save --no-restore ./pms_usecase_structure.R 
RET=$?

log "Simulation ended with exit code $RET"

# Prepare result
echo "[`date +%Y-%m-%d" "%H:%M:%S`] Done"

exit $RET

