#!/bin/bash
#
# run_matlab.sh -- wrapper script for executing MATLAB code
#
# Authors: Riccardo Murri <riccardo.murri@uzh.ch>,
#          Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>
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
#
me=$(basename "$0")

## defaults

mcr_root=/usr/local/MATLAB


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
  -m PATH       Path to alternate MATLAB installation directory (default: '$mcr_root')
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

short_opts='dhm:s:v'
long_opts='debug,matlab-root:,sources:,help,verbose'

if [ "x$(getopt -T)" != 'x--' ]; then
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
        --matlab*|-m)  mcr_root="$2"; shift ;;
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

# is `$mcr_root` a valid path to a MATLAB install?
if ! is_matlab_root "${mcr_root}"; then
    # no, then try to guess
    for dir in "${mcr_root}"/R*; do
        if is_matlab_root "$dir"; then
            mcr_root="$dir"
            break
        fi
    done
fi

if ! is_matlab_root "${mcr_root}"; then
    die $EX_UNAVAILABLE "Cannot find MATLAB root directory"
fi

PATH="${mcr_root}/bin:$PATH"
require_command mcc


## extract sources (if needed)

if [ -n "$src" ]; then
    echo "=== Extracting sources from file '$src'"
    case "$src" in
        *.zip)     unzip -d . "$src" ;;
        *.tar)     tar -x $verbose -f "$src" ;;
        *.tar.gz)  zcat "$src"  | tar -x $verbose -f '-' ;;
        *.tar.bz2) bzcat "$src" | tar -x $verbose -f '-' ;;
    esac
fi

# if the sources were not extracted in the current directory,
# move them here
loc=$(dirname $(find . -name "${main}.m")) \
    || die $EX_SOFTWARE "Cannot find source file '${main}.m'. Look above: were the sources correctly extracted?"
if [ "$loc" != '.' ]; then
    mv $verbose "${loc}"/* ./
fi


## compile sources

echo "=== Compiling source file '${main}.m'"

# patch sources
for m in *.m; do
    sed -re 's/^( *profile +(on|off).*)/%\1/' -i "${m}"
done

# run the compiler
mkdir -p $verbose bin
if ! mcc $verbose -m -d bin -R '-nojvm,-nodisplay' -o "${main}" "${main}.m"; then
    die $EX_SOFTWARE "Cannot compile source file '${main}.m'.  Lines above may give more details."
fi

# check that it worked
if ! test -x "bin/${main}"; then
    die $EX_SOFTWARE "Compilation did not produce an executable file named '${main}'. Lines above may give more details."
fi

if ! test -x "bin/run_${main}.sh"; then
    die $EX_SOFTWARE "Compilation did not produce a script named 'run_${main}.sh'. Lines above may give more details."
fi


## run script
echo "=== Running: ${main} $@"
./bin/"run_${main}.sh" "${mcr_root}" "$@"
rc=$?
echo "=== Script '${main}' ended with exit code $rc"


## All done.
echo "=== Script '${main}' done at `date '+%Y-%m-%d %H:%M:%S'`."

exit $rc
