#! /bin/sh
#
PROG="$(basename $0 .sh)"

usage () {
cat <<EOF
Usage: $PROG [script options] [rosetta arguments ...]

Run the Rosetta '${PROG}' application with any arguments specified on
the command-line; if a '${PROG}.flags' file exists in the current
directory, all options specified there will be appended to the
RosettaDock invocation.  

Output from the RosettaDock application will be collected to a
'${PROG}.log' file.

After a successful run, computed decoy energy scores will be
collected into a file named '${PROG}.tar.gz'

Arguments starting with a single '-' are not interpreted as
options, instead they are passed unchanged to RosettaDock.

Option processing stops at the first argument that is not recognized
as an option; i.e., all script options must precede RosettaDock
options.

Script options:

  --help  Print this help text.

  --quiet Do not report on script progress status: only errors
          and messages directly coming from RosettaDock will appear
          in the standard output.

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

require_environment_variable () {
    if [ -z "$(eval echo \$$1)" ]; then
        die 1 "Environment variable '$1' has empty value, but is required for running this script.  Please check the run-time environment."
    fi
}

get_argument_of_option () {
    option="$1"
    shift
    while [ $# -gt 0 ]; do
        if [ "$1" = "$option" ]; then
            # XXX: should we handle the case of missing argument? (e.g., next arg is another option)
            echo "$2"
        fi
    done
}

is_absolute_path () {
    expr match "$1" '/' >/dev/null 2>/dev/null
}


## parse command-line 

say="echo '$PROG: '"

while [ $# -gt 0 ]; do
    case "$1" in
        --help) usage; exit 0 ;;
        --quiet) say=':' ;;
        *) shift; break ;;
    esac
    shift
done

## main

# XXX: these are all std UNIX cmds, we may as well skip the check
# (problems will arise if they don't behave as their GNU counterparts,
# e.g. on Solaris)
require_command sed
require_command tar
require_command xargs
require_command cut

# if the -database option is specified on the command-line,
# then skip any DB processing below
if [ -n "$(get_argument_of_option -database "$@")" ]; then
    skip_db_patching=yes
fi

# read additional options from the '${PROG}.flags' file, if present
if [ -e "${PROG}.flags" ]; then
    flags="@${PROG}.flags"
    if [ -z "$skip_db_patching" ]; then
        # correct database location
        require_environment_variable ROSETTA_DB_LOCATION
        sed -i -r -e "s/-database +.*/-database $ROSETTA_DB_LOCATION/g" "${PROG}.flags"
        $say Changed database location in ${PROG}.flags file to "$ROSETTA_DB_LOCATION"
    fi
else
    if [ -z "$skip_db_patching" ]; then
        database="-database $ROSETTA_DB_LOCATION"
    fi 
fi

require_environment_variable ROSETTA_LOCATION

$say Running: $ROSETTA_LOCATION/${PROG}.linuxgccrelease $database $flags "$@"
${ROSETTA_LOCATION}/${PROG}.linuxgccrelease $database $flags "$@" | tee ${PROG}.log

$say Collecting computed decoys energy scores in file "${stem}${PROG}.tar.gz" ...
tar $verbose -czf "${stem}${PROG}.tar.gz" ${stem}.pdb

$say All done.

