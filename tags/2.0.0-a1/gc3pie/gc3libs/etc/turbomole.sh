#! /bin/sh
#
PROG="$(basename $0)"

usage () {
cat <<EOF
Usage: $PROG [options] PROG [ARGS ...]

Run PROG with ARGS (if any).  If a file named 'define.in' is present
in the current directory, then run TURBOMOLE's 'define' program with
'define.in' as input prior to running PROG.

The following options are only recognized if they come *before* PROG;
any argument following PROG is passed on to PROG unchanged.

Options:

  --help        Print this help text.
  --verbose     Verbosely report about this script's actions.
  --just-print  Do not execute any command; show what would
                be run instead.

EOF
}


# FIXME: This is only needed to have TURBOMOLE running on idgc3grid01...
if [ -z "$TURBODIR" ]; then
	export TURBODIR=/home/mpackard/apps/TURBOMOLE
fi
echo "DEBUG: TURBODIR='$TURBODIR'"
export PATH=$TURBODIR/scripts:$PATH
export PATH=$TURBODIR/bin/$(sysname):$PATH
echo "DEBUG: PATH='$PATH'"


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


## parse command-line 

maybe=''
verbose=':'

if [ "x$(getopt -T)" == 'x--' ]; then
    # old-style getopt, use compatibility syntax
    set -- $(getopt 'h' "$@")
else
    # GNU getopt
    set -- $(getopt --shell sh -l 'help' -o 'h' -- "$@")
fi
while [ $# -gt 0 ]; do
    case "$1" in
        --help|-h) 
            usage; exit 0 ;;
        --verbose|-v) 
            verbose='echo' ;;
        --just-print|--no-act|-n) 
            maybe=':' ;;
        --) shift; break ;;
        -*) die 1 "Unknown option '$1'" ;;
        # first non-option argument is PROG
        *) break ;;
    esac
    shift
done


## main

if [ -r define.in ]; then
    $verbose "Found 'define.in', now running TURBOMOLE's 'define' ..."
    if [ -z "$TURBOMOLE_DEFINE" ]; then	    
	require_command define
	TURBOMOLE_DEFINE='define'
    fi
    echo "DEBUG: Using define from `which $TURBOMOLE_DEFINE`"
    $maybe "$TURBOMOLE_DEFINE" < define.in \
        || die $? "Failed running 'define'."
fi

eval exe="$1"; shift
$verbose "Now executing program '$exe' ..."
require_command "$exe"
$maybe exec "$exe" "$@"
