# LOAD THIS FILE WITH:
#
#     . ~/sc-authenticate.sh
#
# This file is intentionally not executable: it should *not* be run as
# normal command (as it cannot modify the shell environment in this
# case); load it instead, using the `.` invocation above.
#
me="$(basename -- $0)"

usage () {
cat <<EOF
Usage: source $me [options] [project [username]]
       eval \$($me -s [options] [project [username]])

Set up the shell environment for accessing Science Cloud's OpenStack
networked API. If PROJECT and USERNAME are not given on the command
line, you will be prompted interactively to type them.

You will be prompted for a password only if the environment variable
'OS_PASSWORD' is empty or undefined; otherwise its value is assumed
correct and re-used.

Options:

  -k, --keep  If environment variables 'OS_PROJECT_NAME' or
              'OS_USERNAME' are defined, keep that value
              and do not prompt for overwriting. (This is the
              default behavior; use option '--new' to override.)

  -n, --new   Prompt for new values of 'OS_PROJECT_NAME'
              or 'OS_USERNAME' even if they are already defined
              in the environment.

  -s, --show  Show 'export' commands instead of executing them.

  -2          Use endpoints for Keystone API v3 (default)

  -3          Use endpoints for Keystone API v3,
              instead of the default v2.0

  --help, -h  Print this help text.

EOF
}


## defaults

os_project_default='training'
os_username_default=""

os_auth_url_v2_default='https://cloud.s3it.uzh.ch:5000/v2.0'
os_auth_url_v3_default='https://cloud.s3it.uzh.ch:35357/v3'


## helper functions

ask () {
    local prompt="$1"
    local default="$2"
    local reply=''

    if [ -n "$default" ]; then
        prompt="${prompt} [${default}]"
    fi

    while [ -z "$reply" ]; do
        printf "${prompt}: " 1>&2
        read reply
        if [ -z "$reply" ] && [ -n "$default" ]; then
            reply="$default"
        fi
    done
    printf '%s' "$reply"
}

die () {
  local rc="$1"
  shift
  (echo -n "$me: ERROR: ";
      if [ $# -gt 0 ]; then echo "$@"; else cat; fi) 1>&2
  exit $rc
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
    die 1 "Could not find required command '$1' in system PATH. Aborting."
  fi
}

is_absolute_path () {
    expr match "$1" '/' >/dev/null 2>/dev/null
}


## parse command-line

short_opts='23hkns'
long_opts='help,keep,new,show,v2,v3'

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
        --keep|-k) keep='yes' ;;
        --new|-n)  keep='no' ;;
        --show|-s) show='echo' ;;
        --v2|-2)   use_keystone_api_v3='no' ;;
        --v3|-3)   use_keystone_api_v3='yes' ;;
        --help|-h) usage; exit 0 ;;
        --) shift; break ;;
    esac
    shift
done


## main

require_command printf stty

# set OS_PROJECT_NAME
if [ -n "$1" ]; then
    # project name given on command-line, max priority
    os_project_reply="$1"
elif [ "$keep" = 'yes' ] && [ -n "$OS_PROJECT_NAME" ]; then
    os_project_reply="$OS_PROJECT_NAME"
else
    os_project_reply=$(ask "Please enter your ScienceCloud project name" "${OS_PROJECT_NAME:-$os_project_default}")
fi

# set OS_USERNAME
if [ -n "$2" ]; then
    # user name given on command-line, max priority
    os_username_reply="$2"
elif [ "$keep" = 'yes' ] && [ -n "$OS_USERNAME" ]; then
    os_username_reply="$OS_USERNAME"
else
    os_username_reply=$(ask "Please enter your ScienceCloud username" "${OS_USERNAME:-$os_username_default}")
fi

# set OS_PASSWORD
if [ -z "$OS_PASSWORD" ]; then
    stty -echo
    os_password_reply=$(ask "Please enter your OpenStack Password")
    stty echo
    echo
fi

# all output at the end
if [ "$use_keystone_api_v3" = 'yes' ]; then
    $show export OS_AUTH_URL="$os_auth_url_v3_default"
else
    $show export OS_AUTH_URL="$os_auth_url_v2_default"
fi
$show export OS_USERNAME="$os_username_reply"
$show export OS_TENANT_NAME="$os_project_reply"
$show export OS_PROJECT_NAME="$os_project_reply"
$show export OS_PASSWORD="${OS_PASSWORD:-$os_password_reply}"

# Don't leave a blank variable, unset it if it was empty
if [ -z "$OS_REGION_NAME" ]; then
    $show unset OS_REGION_NAME;
fi

# cleanup (in case we're being sourced)
unset \
    args \
    os_auth_url_v2_default \
    os_auth_url_v3_default \
    os_password_reply \
    os_project_default \
    os_project_reply \
    os_username_default \
    os_username_reply \
    reply \
    show use_keystone_api_v3 \
    ;
