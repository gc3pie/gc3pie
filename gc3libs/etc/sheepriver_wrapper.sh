#!/bin/bash
#
# sheepriver_wrapper.sh -- wrapper script for executing Java code
#
# Authors: Sergio Maffioletti <sergio.maffioletti@uzh.ch>
#
# Copyright (c) 2016 S3IT, University of Zurich, http://www.s3it.uzh.ch/
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
me=$(basename "$0")

## defaults
jar=""
source=""
param="$PWD/param_SheepRiver_wear"
seeds="$PWD/listOfRandomSeeds"
iterations="10000"
output="./results"
output_archive_file_name="results.tgz"
manifest="MANIFEST.MF"

# no settings for this script


## Exit status codes (mostly following <sysexits.h>)

# successful exit
EX_OK=0

# wrong command-line invocation
EX_USAGE=64

# missing dependencies (e.g., no C compiler)
EX_UNAVAILABLE=69

# wrong MATLAB version
EX_SOFTWARE=70

# cannot create directory or file
EX_CANTCREAT=73

# user aborted operations
EX_TEMPFAIL=75

# misused as: unexpected error in some script we call
EX_PROTOCOL=76


## helper functions

function die () {
    rc="$1"
    shift
    (echo -n "$me: ERROR: ";
        if [ $# -gt 0 ]; then echo "$@"; else cat; fi) 1>&2
    exit $rc
}


## usage info

usage () {
    cat <<__EOF__
Usage:
  $me [options] HUNTING [ARGS]

Execute StartSimulation using HUNTING value as hunting pressure value.

Options:
  -j            Path to StartSimulation JAR file
  -s            Path to java sources for Simulation code
  -p            Path to param sheepRiver file
  -m            Path to seeds file
  -i            Number of iterations. Default: 10000
  -v            Enable verbose logging
  -h            Print this help text
  -o            Output archive filename (e.g. results.tgz)
__EOF__
}

warn () {
  (echo -n "$me: WARNING: ";
      if [ $# -gt 0 ]; then echo "$@"; else cat; fi) 1>&2
}

have_command () {
  type "$1" >/dev/null 2>/dev/null
}

require_command () {
  if ! have_command "$1"; then
    die $EX_UNAVAILABLE "Could not find required command '$1' in system PATH. Aborting."
  fi
}

is_absolute_path () {
    expr match "$1" '/' >/dev/null 2>/dev/null
}


## parse command-line

short_opts='hvj:s:p:m:i:o:'
long_opts='help,verbose,jar,source,param,seed,iterations,output'

getopt -T > /dev/null
rc=$?
if [ "$rc" -eq 4 ]; then
    # GNU getopt
    args=$(getopt --name "$me" --shell sh -l "$long_opts" -o "$short_opts" -- "$@")
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
        --jar|-j)        shift; jar=$1 ;;
        --source|-s)     shift; source=$1; echo "Warning!!! source compiling not yet supported. This option will be ignored" ;;
        --param|-p)      shift; param=$1 ;;
        --seed|-m)       shift; seed=$1 ;;
        --iterations|-i) shift; iterations=$1 ;;
        --output|-o)     shift; output_archive_file_name=$1 ;;
        --verbose|-v)    verbose='--verbose' ;;
        --help|-h)       usage; exit 0 ;;
        --)              shift; break ;;
    esac
    shift
done

# the SCRIPT argument has to be present
if [ $# -lt 1 ]; then
    die 1 "Missing required argument HUNTING value. Type '$me --help' to get usage help."
fi

hunting=$1

## sanity checks
echo -e "Jar:\t\t$jar"
echo -e "Source:\t\t$source"
echo -e "Param:\t\t$param"
echo -e "Seed:\t\t$seed"
echo -e "Iterations:\t$iterations"
echo -e "Output:\t\t$output"
echo -e "Hunting value:\t\t$hunting"

cur_dir="$PWD"

# Create output folder if needed
if ! [ -d $output ]; then
    echo -n "Creating output folder... "
    mkdir $output
    if [ $? -ne 0 ]; then
	echo "[failed]"
	exit 1
    else
	echo "[ok]"
    fi
fi

# Check if src code needs to be compiled
if [ -n $source ] && [ -z $jar ]; then
    echo -n "Compiling from source: $src... "
    cd $source
    if [ -e $manifest ]; then
	main_class=$(grep -i "main-class" $manifest | awk '{print $2}')
	if [ -z ${main_class} ]; then
	    echo "CRITICAL: Failed extracting main class information from $manifest"
	    exit 1
	fi
    else
	echo "CRITICAL: MANIFEST.MF file not found"
	exit 1
    fi
	
    if ! [ -e ${main_class}.java ]; then
	echo "CRITICAL: main class $main_class file not found"
	exit 1
    fi
    javac ${main_class}.java
    if [ $? -ne 0 ]; then
	echo "CRITICAL: Failed compiling $main_class"
	exit 1
    else
	echo "{ok]"
    fi

    echo -n "Generating jar... "
    jar cmf $manifest ${main_class}.jar *.class
    if [ $? -ne 0 ]; then
	echo "CRITICAL: Failed creating jar file."
	exit 1
    fi
    jar="$PWD/${main_class}.jar"
    cd $cur_dir
    echo "[ok]"
fi

# Compile source code if provided
## main
echo "=== ${me}: Starting at `date '+%Y-%m-%d %H:%M:%S'`"

for i in `seq 1 $iterations`; do
     seedN=$(sed -n -e ${i}p $seed)
     time java -jar $jar $param $hunting $seedN $output/h_$hunting-$i
done

# Archive the whole output folder
tar cfz ${output_archive_file_name} $output

rc=$?

## All done.
echo "=== Script done at `date '+%Y-%m-%d %H:%M:%S'`."

exit $rc
