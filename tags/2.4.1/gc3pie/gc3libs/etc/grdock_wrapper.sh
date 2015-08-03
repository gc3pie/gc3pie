#!/bin/bash
#
# grdock_wrapper.sh -- wrapper script for executing rdock
#
# Authors: Sergio Maffioletti <sergio.maffioletti@uzh.ch>
#
# Copyright (c) 2014 S3IT, University of Zurich, http://www.s3it.uzh.ch/
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
#

iterations=20
inputprm="UL04_rdock.prm"
docked="Docked"
me=$(basename "$0")

## helper functions

function exit-on-fail () {
    ret="$1"
    if [ $ret -ne 0 ]; then
	echo "[failed: $ret]"
	exit $ret
    else
	echo "[ok]"
    fi
}

function die () {
    rc=$1
    shift
    (echo -n "$me: ERROR: ";
        if [ $# -gt 0 ]; then echo "$@"; else cat; fi) 1>&2
    exit $rc
}


## usage info

usage () {
    cat <<__EOF__
Usage:
  $me [OPTIONS] INPUT_SD RESULT_FOLDER

Execute rbcavity and rbdock on SD_input_file.
Write results in result_folder.

Options:
  -p INPUT RPM      Input .prm file (default: $inputprm)
  -n INTEGER        Number of iterations for rdock (default: $iterations)
  -o OUTPUT FILE    Rbdock output file name (Default: $docked)
__EOF__
}

have_command () {
  type "$1" >/dev/null 2>/dev/null
}

require_command () {
  if ! have_command "$1"; then
    die 1 "Could not find required command '$1' in system PATH. Aborting."
  fi
}

## parse command-line
short_opts='p:n:o:h'

getopt -T > /dev/null
rc=$?
if [ "$rc" -eq 4 ]; then
    # GNU getopt
    args=$(getopt --name "$me" --shell sh -o "$short_opts" -- "$@")
    if [ $? -ne 0 ]; then
        die 1 "Type '$me --help' to get usage information."
    fi
    # use 'eval' to remove getopt quoting
    eval set -- $args
else
    # old-style getopt, use compatibility syntax
    args=$(getopt "$short_opts" "$@")
    if [ $? -ne 0 ]; then
        die 1 "Type '$me --help' to get usage information."
    fi
    set -- $args
fi

while [ $# -gt 0 ]; do
    case "$1" in
	-p) shift; inputprm=$1 ;;
        -n) shift; iterations=$1 ;;
	-o) shift; docked=$1 ;;
        -h) usage; exit 0 ;;
        --) shift; break ;;
    esac
    shift
done

# the INPUT_SD and RESULT_FOLDER arguments have to be present
if [ $# -lt 2 ]; then
    die 1 "Missing required argument INPUT_SD and/or RESULT_FOLDER. Type '$me --help' to get usage help."
fi

INPUT=$1
RESULTS=$2

## main
echo "[`date`] Start"

export RBT_ROOT=/apps/rDock-2013.1
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$RBT_ROOT/lib
export PATH=$PATH:$RBT_ROOT/exe

require_command rbcavity
require_command rbdock

cat <<__EOF__
#########################
Input SD file:    $INPUT
Result folder:    $RESULTS
RBCAVITY:         `which rbcavity`
RBDOCK:           `which rbdock`
Iterations:       $iterations
#########################
__EOF__

## Check result folder

if [ ! -d $RESULTS ] && [ ! -f $RESULTS ]; then
    mkdir $RESULTS
    exit-on-fail $?
fi

echo -n "Running rbcavity... "
rbcavity -was -d -r UL04_rdock.prm
exit-on-fail $?

echo -n "Running rbdock..."
rbdock -r UL04_rdock.prm -p dock.prm -n $iterations -i $INPUT -o $RESULTS/$docked > $RESULTS/Docking.log
rc=$?
exit-on-fail $rc

echo "[`date`] Stop"

exit $rc
