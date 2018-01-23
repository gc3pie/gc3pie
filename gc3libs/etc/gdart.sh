#!/bin/bash
#
# gdart.sh -- wrapper script for executing MATLAB code
# Extension of the run_matlab.sh original script
#
# Authors: Riccardo Murri <riccardo.murri@uzh.ch>,
#          Sergio Maffioletti <sergio.maffioletti@uzh.ch>
#
#   Copyright (c) 2017,2018 S3IT, University of Zurich,
#   http://www.s3it.uzh.ch/
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
add_path=$PWD
source_folder="./src"

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
  $me [options] MAIN [ARGS]

Compile the MATLAB code in file MAIN (a '.m' is appended if not
present) and run it, passing ARGS (if any) as command-line arguments.

Options:
  -s TARFILE    Path to '.tar' file containing the '.m' source files to run
  -v            Enable verbose logging

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

is_matlab_root () {
    dir="$1"
    test -d "${dir}/runtime" -a -d "${dir}/bin"
}


## parse command-line

short_opts='dhs:v'
long_opts='debug,matlab-root:,sources:,help,verbose,compile'

# test which `getopt` version is available:
# - GNU `getopt` will generate no output and exit with status 4
# - POSIX `getopt` will output `--` and exit with status 0
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
        --source*|-s)  src="$2"; shift ;;
        --verbose|-v)  verbose='-v' ;;
        --debug|-d)    verbose='-v'; set -x ;;
        --help|-h)     usage; exit 0 ;;
        --)            shift; break ;;
    esac
    shift
done


## sanity checks

# the MAIN argument has to be present
if [ $# -lt 1 ]; then
    die 1 "Missing required argument MAIN. Type '$me --help' to get usage help."
fi

## main
echo "=== Starting at `date '+%Y-%m-%d %H:%M:%S'`"

main="${1%%.m}"
shift

require_command matlab
require_command xvfb-run

# If Matlab tar file passed, create $source_folder
mkdir -p $source_folder

if [ -n "$src" ]; then
    echo "=== Extracting sources from file '$src'"
    case "$src" in
        *.zip)     unzip -d "$source_folder" "$src" ;;
        *.tar)     tar -x $verbose -f "$src" -C "$source_folder";;
        *.tar.gz)  zcat "$src"  | tar -x $verbose -f '-' -C "$source_folder";;
        *.tgz)  zcat "$src"  | tar -x $verbose -f '-' -C "$source_folder";;	
        *.tar.bz2) bzcat "$src" | tar -x $verbose -f '-' -C "$source_folder";;
    esac
else
    # a Matlab script has been provided.
    source_folder=$PWD
fi

# Set DART_LOCAL variables
cd $source_folder
source ~/.dartrc
export DART_LOCAL=$PWD

## run script
echo "=== Running: ${main} $@"
args=""
for var in "$@"
do
    args=$args"'"$var"',"
done
args=${args%?}

matlab_function_call="addpath(genpath('.'));$main($args);quit"
echo "xvfb-run -a matlab -nodesktop -nosplash -nodisplay -r \"$matlab_function_call\""
xvfb-run -a matlab -nodesktop -nosplash -nodisplay -r "$matlab_function_call"
rc=$?
echo "=== Script ended with exit code $rc"


## All done.
echo "=== Script '${main}' done at `date '+%Y-%m-%d %H:%M:%S'`."

exit $rc
