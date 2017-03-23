#!/bin/bash
#
# sheepriver_wrapper.sh -- wrapper script for executing Java code
#
# Authors: Sergio Maffioletti <sergio.maffioletti@uzh.ch>
#
# Copyright (c) 2017 S3IT, University of Zurich, http://www.s3it.uzh.ch/
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
jar="$HOME/myjarfile.jar"
seed=""
state=""
output="./results"

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
qEX_TEMPFAIL=75

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
  $me [options] InputXML.xml

Execute StartSimulation using HUNTING value as hunting pressure value.

Options:
  -j            Path to Beast.jar JAR file
  -s            seed number
  -t            Path to .state file for resume
  -v            Enable verbose logging
  -h            Print this help text
  -o            Output folder
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

short_opts='hvj:s:t:o:'
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
        --seed|-s)       shift; seed=$1;;
        --statefile|-t)  shift; state=$1 ;;
        --output|-o)     shift; output=$1 ;;
        --verbose|-v)    verbose='--verbose' ;;
        --help|-h)       usage; exit 0 ;;
        --)              shift; break ;;
    esac
    shift
done

# the SCRIPT argument has to be present
if [ $# -lt 1 ]; then
    die 1 "Missing required argument InputXML file. Type '$me --help' to get usage help."
fi

inputxml=$1

## sanity checks
echo -e "Jar:\t\t$jar"
echo -e "Seed:\t\t$seed"
echo -e "State File:\t\t$state"
echo -e "Output:\t\t$output"
echo -e "Input XML:\t\t$inputxml"

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

## get UID and GID from current user
gid=`id -g`
uid=`id -u`

## main
echo "=== ${me}: Starting at `date '+%Y-%m-%d %H:%M:%S'`"

if [ -n "$state" ]; then
    cmd="java -jar ${jar} -seed ${seed} -statefile ${state} -resume ${inputxml}"
else
    cmd="java -jar ${jar} -seed ${seed} ${inputxml}"
fi

# Install docker
echo "Installing dockers... "
if ! command -v docker; then
  curl https://get.docker.com/ | sudo bash
fi
sudo usermod -aG docker $USER

# execute command
echo "Runnning: sudo sudo -u $USER -g docker docker run -i --rm -v "$PWD":/mnt -w /mnt java:8 /bin/bash -c \"groupadd docker -g $gid; useradd docker -u $uid; su docker -c \"$cmd\"\""
time sudo sudo -u $USER -g docker docker run -i --rm -v "$PWD":/mnt -w /mnt java:8 /bin/bash -c "groupadd docker -g $gid; useradd docker -u $uid -g $gid; su docker -c \"$cmd\""
rc=$?

## All done.
echo "=== Script done at `date '+%Y-%m-%d %H:%M:%S'` with exit code [$rc]."

exit $rc
