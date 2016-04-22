#!/bin/bash
#
#
#  Copyright (C) 2016, S3IT, University of Zurich. All rights
#  reserved.
#
#  This program is free software; you can redistribute it and/or modify it
#  under the terms of the GNU General Public License as published by the
#  Free Software Foundation; either version 2 of the License, or (at your
#  option) any later version.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
#

PROG="$(basename $0)"
VERBOSITY=1
# Verbosity levels:
# 0: error
# 1: warning
# 2: info
# 3: debug

usage () {
cat <<EOF
Usage: $PROG [options]

A short description of what the program does should be here,
but it's not (yet).

Options:

  --help, -h     Print this help text.
  -v, --verbose  Increase verbosity.
EOF
}


## helper functions
die () {
  rc="$1"
  shift
  (echo -n "$PROG: ERROR: ";
      if [ $# -gt 0 ]; then echo "$@"; else cat; fi) 1>&2
  exit $rc
}

have_command () {
  type "$1" >/dev/null 2>/dev/null
}

require_command () {
  if ! have_command "$1"; then
    die 1 "Could not find required command '$1' in system PATH. Aborting."
  fi
}

is_absolute_path () {
    expr match "$1" '/' >/dev/null 2>/dev/null
}

log () {
    level=$1
    [ $level -le $VERBOSITY ] || return
    shift
    (echo -n "$PROG: ";
        if [ $# -gt 0 ]; then echo "$@"; else cat; fi)
}

error () {
    log 0 "ERROR: $@"
}

warn () {
    log 1 "WARNING: $@"
}

info () {
    log 2 "INFO: $@"
}

debug () {
    log 3 "DEBUG: $@"
}

## parse command-line

short_opts='hv'
long_opts='help,verbose'

getopt -T > /dev/null
rc=$?
if [ "$rc" -eq 4 ]; then
    # GNU getopt
    args=$(getopt --name "$PROG" --shell sh -l "$long_opts" -o "$short_opts" -- "$@")
    if [ $? -ne 0 ]; then
        die 1 "Type '$PROG --help' to get usage information."
    fi
    # use 'eval' to remove getopt quoting
    eval set -- $args
else
    # old-style getopt, use compatibility syntax
    args=$(getopt "$short_opts" "$@")
    if [ $? -ne 0 ]; then
        die 1 "Type '$PROG --help' to get usage information."
    fi
    set -- $args
fi

while [ $# -gt 0 ]; do
    case "$1" in
        --help|-h) usage; exit 0 ;;
        -v|--verbose) VERBOSITY=$[VERBOSITY+1];;
        --) shift; break ;;
    esac
    shift
done

var=$1

## main

export DISPLAY=:1
xwininfo -root >& /dev/null || Xvfb $DISPLAY -auth /dev/null >& /dev/null &

exec Rscript bemovi.R $@
