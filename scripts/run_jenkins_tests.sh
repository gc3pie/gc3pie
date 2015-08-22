#! /bin/sh

# short-cut for reporting
_ () { echo "$@" 1>&2; }

# exit at first error
set -e

# set test defaults
gc3pie_source_checkout='gc3pie.svn'
gc3pie_test_env='gc3pie.env'

if [ -n "$1" ]; then
    # only run this set of tests
    tox_env="-e $1"
else
    _ "=== Running all test environments configured in tox.ini"
fi

# sanity checks
test -d "$gc3pie_source_checkout"
test -d "$gc3pie_source_checkout"/gc3pie

# GC3Pie creates state files in $HOME; ensure we do not run with the
# *real* home dir
if [ -n "$WORKSPACE" ]; then
    # set home directory to Jenkins' workspace
    HOME="$WORKSPACE"
else
    # set home directory to current work dir
    HOME=$(pwd)
fi
export HOME

_ "Ensure we're running in the POSIX/C locale ..."
# ensure this is run in the POSIX locale,
# to minimize encode/decode issues
unset LANG
unset LANGUAGE
LC_ALL=C
export LANG LANGUAGE LC_ALL
locale

_ "=== Create safe GC3Pie configuration file ..."
mkdir -pv $HOME/.gc3
set -x
cat > $HOME/.gc3/gc3pie.conf <<__EOF__
[resource/localhost]
enabled = yes
type = shellcmd
frontend = localhost
transport = local
max_cores_per_job = 1
max_memory_per_core = 2GiB
max_walltime = 8 hours
max_cores = 1
architecture = x86_64
auth = none
override = no
__EOF__
GC3PIE_CONF="$HOME/.gc3/gc3pie.conf"
export GC3PIE_CONF
set +x

_ "=== Complete UNIX environment of the tests:"
env | sort

_ "=== Removing old virtualenv to ensure clean-slate testing ..."
rm -rf "$gc3pie_test_env"

_ "=== Creating new test virtualenv ..."
virtualenv "$gc3pie_test_env"
. "$gc3pie_test_env"/bin/activate

# Enter the svn directory
cd "$gc3pie_source_checkout"/gc3pie

_ "=== Cleaning up old '.pyc' and '.pyo' files ..."
find . -name '*.py[co]' -exec rm -vf {} \;

_ "=== Run tests"
pip install tox
exec tox ${tox_env} -r
