#! /bin/sh -x
#
PROG="$(basename $0 .sh)"

usage () {
cat <<EOF
Usage: $PROG [option file]

Run the Rosetta '${PROG}' application with any arguments specified on
the command-line; if a '${PROG}.flags' file exists in the current
directory, all options specified there will be appended to the
Rosetta invocation command-line.

Output from the Rosetta application will be collected to a
'${PROG}.log' file.

After a successful run, all '.pdb', '.sc' and '.fasc' files will be
collected into a file named '${PROG}.tar.gz'.  The list of files to be
packed into the archive can be overridden with the '--tar' script
option.

Option processing stops at the first argument that is not recognized
as an option; i.e., all script options must precede Rosetta options.

Script options:

  --help   Print this help text.

  --quiet  Do not report on script progress status: only errors
           and messages directly coming from RosettaDock will appear
           in the standard output.

  --tar "FILES"
           Pack the given files into a single '${PROG}.tar.gz' archive;
           by default this includes all '.pdb', '.sc' and '.fasc' files.
           Argument FILES must be a whitespace-separated list of files
           to include in the archive; shell glob patterns '*' and '?' 
           are allowed.  No error will be produced if any of the listed
           files is not found in the execution directory.
              
   --no-tar Do not create a tar archive of results at all.

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

is_absolute_path () {
    expr match "$1" '/' >/dev/null 2>/dev/null
}

## parse command-line 

say="echo $PROG: "
tar_patterns='*.pdb *.sc *.fasc'

short_opts='hnqt:m:'
long_opts='help,no-tar,quiet,tar,mpi'

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
        --help|-h)   usage; exit 0 ;;
        --no-tar|-n) tar_patterns='' ;;
        --quiet|-q)  say=':' ;;
        --tar|-t)    tar_patterns="$2"; shift ;;
	--mpi|-m)    mpi_max_cores="$2"; shift ;;
        --) shift; break ;;
    esac
    shift
done

# the SCRIPT argument has to be present
if [ $# -lt 1 ]; then
    die 1 "Missing required argument OPTION file. Type '$me --help' to get usage help."
fi


## main

# XXX: these are all std UNIX cmds, we may as well skip the check
# (problems will arise if they don't behave as their GNU counterparts,
# e.g. on Solaris)
# require_command sed
# require_command tar
# require_command xargs
# require_command cut

# # read additional options from the '${PROG}.flags' file, if present
# if [ -e "${PROG}.flags" ]; then
#     if grep -q -e '^-database' "${PROG}.flags"; then
#         # correct database location
#         case "${PROG}" in
#             minirosetta*)
#                 sed -i -r -e "s|-database +.*|-database ${MINIROSETTA_DB_LOCATION}|g" "${PROG}.flags" 
#                 $say Changed database location in ${PROG}.flags file to "$MINIROSETTA_DB_LOCATION"
#                 ;;
#             *) 
#                 sed -i -r -e "s|-database +.*|-database ${ROSETTA_DB_LOCATION}|g" "${PROG}.flags" 
#                 $say Changed database location in ${PROG}.flags file to "$ROSETTA_DB_LOCATION"
#                 ;;
#         esac
#         database_specified_in_flags_file=yes
#     fi
#     flags="`grep ^- ${PROG}.flags`"
# fi

# if [ -z "$database_specified_in_flags_file" ]; then
#     case "${PROG}" in
#         # minirosetta*)
#         #     require_environment_variable MINIROSETTA_DB_LOCATION
#         #     database="-database $MINIROSETTA_DB_LOCATION"
#         #     $say "Database location not in flags file, using the one from RTE: '$MINIROSETTA_DB_LOCATION'"
#         #     ;;
#         # *)
#         #     require_environment_variable ROSETTA_DB_LOCATION
#         #     database="-database $ROSETTA_DB_LOCATION"
#         #     $say "Database location not in flags file, using the one from RTE: '$ROSETTA_DB_LOCATION'"
#         #     ;;
#     esac
# fi

# require_environment_variable ROSETTA_LOCATION

$say Running: $ROSETTA_LOCATION/${PROG}.linuxgccrelease $database $flags "$@"
# (${ROSETTA_LOCATION}/${PROG}.linuxgccrelease $flags $database "$@" 2>&1; echo $? > ${PROG}.exitcode) | tee ${PROG}.log
# screen -L mpirun -np 6 /home/ubuntu/Rosetta/main/source/bin/docking_protocol.mpi.linuxgccdebug @docking.options

$say Contents of the execution directory, after processing:
ls -l

if [ -n "$tar_patterns" ]; then
    $say Collecting  "'$tar_patterns'" into archive "${PROG}.tar.gz" ...
    tar $verbose -cvzf "${PROG}.tar.gz" $(ls $tar_patterns 2>/dev/null)
fi

rc=$?
# rc=$(cat ${PROG}.exitcode)

$say All done, exitcode: $rc
exit $rc

