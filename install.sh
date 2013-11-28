#!/bin/sh
# @(#)install.sh
#
#
#  Copyright (C) 2012 GC3, University of Zurich. All rights reserved.
#
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
PROG="GC3Pie install"

BASE_PIP_URL="https://pypi.python.org/simple"
VIRTUALENV_LATEST_URL="https://raw.github.com/pypa/virtualenv/master/virtualenv.py"
VIRTUALENV_172_URL="https://raw.github.com/pypa/virtualenv/1.7.2/virtualenv.py"
PIP_11_URL="http://pypi.python.org/packages/source/p/pip/pip-1.1.tar.gz"
GC3_SVN_URL="http://gc3pie.googlecode.com/svn/trunk/gc3pie"

VIRTUALENV_CMD="virtualenv"

# Defaults
VENVDIR=$HOME/gc3pie
DEVELOP=0
WITHAPPS=1
OVERWRITEDIR=ask
ASKCONFIRMATION=1
PYTHON=python

# Auxiliary functions

# abort RC [MSG]
#
# Print error message MSG and abort shell execution with exit code RC.
# If MSG is not given, read it from STDIN.
#
abort () {
  rc="$1"
  shift
  (echo -n "$PROG: ERROR: ";
      if [ $# -gt 0 ]; then echo "$@"; else cat; fi) 1>&2
  exit $rc
}

# die RC HEADER <<...
#
# Print an error message with the given header, then abort shell
# execution with exit code RC.  Additional text for the error message
# *must* be passed on STDIN.
#
die () {
  rc="$1"
  header="$2"
  shift 2
  cat 1>&2 <<__EOF__
====================================================
${PROG}: ERROR: ${header}
====================================================

__EOF__
  if [ $# -gt 0 ]; then
      # print remaining arguments one per line
      for line in "$@"; do
          echo "$line" 1>&2;
      done
  else
      # additional message text provided on STDIN
      cat 1>&2;
  fi
  cat 1>&2 <<__EOF__

If the above does not help you solve the isue, please contact the
GC3Pie team by sending an email to gc3pie@googlegroups.com.  Include
the full output of this script to help us identifying the problem.

Aborting installation!
__EOF__
  exit $rc
}

say () {
    echo "$PROG: $@";
}

warn () {
    echo 1>&2 "$PROG: WARNING: $@";
}

# ask_yn PROMPT
#
# Ask a Yes/no question preceded by PROMPT.
# Set the env. variable REPLY to 'yes' or 'no'
# and return 0 or 1 depending on the users'
# answer.
#
ask_yn () {
    if [ $ASKCONFIRMATION -eq 0 ]; then
        # assume 'yes'
        REPLY='yes'
        return 0
    fi
    while true; do
        read -p "$1 [yN] " REPLY
        case "$REPLY" in
            [Yy]*)    REPLY='yes'; return 0 ;;
            [Nn]*|'') REPLY='no';  return 1 ;;
            *)        say "Please type 'y' (yes) or 'n' (no)." ;;
        esac
    done
}

have_command () {
  type "$1" >/dev/null 2>/dev/null
}

require_command () {
  if ! have_command "$1"; then
    abort 1 "Could not find required command '$1' in system PATH. Aborting."
  fi
}

require_cc () {
    have_command $CC || have_command cc
    if [ $? -ne 0 ]
    then
        die 1 "Unable to find a C compiler!" <<EOF
To install the GC3Pie development branch, a C language compiler
is needed.

Please, install one using the software manager of your distribution.
If this computer already has a C compiler, set the environment variable
CC to the full path to the C compiler command.
EOF
    fi
}

require_svn () {
    have_command svn
    if [ $? -ne 0 ]
    then
        die 2 "Unable to find the 'svn' command!" <<EOF
To install the GC3Pie development branch installation,
the SubVersion ('svn') command is needed.

Please, install it using the software manager of your distribution
or download it from http://subversion.tigris.org/
EOF
    fi

}

require_python () {
    require_command "$PYTHON"
    # We only support python 2.6+, we do not support python 3.x at the
    # moment.
    python_right_version=$($PYTHON <<EOF
import sys
print(sys.version_info[0]==2 and sys.version_info[1] >= 6)
EOF
)

    if [ "$python_right_version" != "True" ]; then
        die 1 "Wrong version of python is installed" <<EOF

GC3Pie requires Python version 2.6+. Unfortunately, we do not support
your version of python: $($PYTHON -V 2>&1|sed 's/python//gi').

If a version of python suitable by GC3Pie is present in some
non-standard location you can specify it from the command line by
rerunning the script with option '--python' followed by the path of
the python binary.
EOF
    fi
}

integer_to_boolean () {
    # Get an integer `N` as input,
    # returns the string "Yes" if N > 0
    # returns the string "No" otherwise
    N=$1
    if [ -n "$N" ] && [ "$N" -ge 1 ]; then
        echo "yes"
    else
        echo "no"
    fi
}

have_sw_package () {
    # instead of guessing which distribution this is, we check for the
    # package manager name as it basically identifies the distro
    if have_command dpkg; then
        (dpkg -l "$1" | egrep -q ^i ) >/dev/null 2>/dev/null;
    elif have_command rpm; then
        rpm -q "$1" >/dev/null 2>/dev/null;
    fi
}

which_missing_packages () {
    missing=''
    for pkgname in "$@"; do
        if have_sw_package "$pkgname"; then
            continue;
        else
            missing="$missing $pkgname"
        fi
    done
    echo "$missing"
}

install_required_sw () {
    # instead of guessing which distribution this is, we check for the
    # package manager name as it basically identifies the distro
    if have_command apt-get; then
        # Debian/Ubuntu
        missing=$(which_missing_packages subversion python-dev gcc g++)
        if [ -n "$missing" ]; then
            cat <<EOF
The following software packages need to be installed
in order for GC3Pie to work: $missing

There is a small chance that the required software
is actually installed though we failed to detect it,
so you may choose to proceed with GC3Pie installation
anyway.  Be warned however, that continuing is very
likely to fail!

EOF
            ask_yn "Proceed with installation anyway?"
            if [ "$REPLY" = 'yes' ]; then
                warn "Proceeding with installation at your request... keep fingers crossed!"
            else
                die 3 "missing software prerequisites" <<EOF
Please ask your system administrator to install the missing packages,
or, if you have root access, you can do that by running the following
command from the 'root' account:

    apt-get install $missing

EOF
            fi
        fi
    elif have_command yum; then
        # RHEL/CentOS
        missing=$(which_missing_packages subversion python-devel gcc gcc-c++)
        if [ -n "$missing" ]; then
            cat <<EOF
The following software packages need to be installed
in order for GC3Pie to work: $missing

There is a small chance that the required software
is actually installed though we failed to detect it,
so you may choose to proceed with GC3Pie installation
anyway.  Be warned however, that continuing is very
likely to fail!

EOF
            ask_yn "Proceed with installation anyway?"
            if [ "$REPLY" = 'yes' ]; then
                warn "Proceeding with installation at your request... keep fingers crossed!"
            else
                die 3 "missing software prerequisites" <<EOF
Please ask your system administrator to install the missing packages,
or, if you have root access, you can do that by running the following
command from the 'root' account:

    yum install $missing

EOF
            fi
        fi
    elif have_command zypper; then
        # SuSE
        warn "Cannot check if requisite software is installed: SuSE and compatible Linux distributions are not yet supported. I'm proceeding anyway, but you may run into errors later. Please write to gc3pie@googlegroups.com asking for information."
    else
        # MacOSX maybe?
        warn "Cannot determine what package manager this Linux distribution has, so I cannot check if requisite software is installed. I'm proceeding anyway, but you may run into errors later. Please write to gc3pie@googlegroups.com to get help."
    fi
}

install_virtualenv () {
    DESTDIR=$1

    if [ -n "$VIRTUAL_ENV" ]; then
        cat <<EOF

ERROR
=====

A virtual environment seems to be already *active*. This will cause
this script to FAIL.

Try to run 'deactivate' and run again the script.

EOF
        exit 0
    fi

    # Check if gc3pie is already installed.
    if have_command gc3utils; then
        WITH_SITE_PACKAGES="--no-site-packages"
        cat <<EOF

WARNING
=======

Creating virtual environment with '--no-site-packages' option.  If you
need to use nordugrid libraries you need to install them inside your
virtual environment!

EOF
        ask_yn "Do you still want to proceed?"
        if [ "$REPLY" = 'no' ]; then
            echo "Aborting installation as requested."
            echo
            exit 0
        fi
    else
        WITH_SITE_PACKAGES="--system-site-packages"
    fi

    # Check python version using the *same* system used by
    # virtualenv.py
    $PYTHON <<EOF
import sys
if sys.version_info < (2, 5):
  sys.exit(101)
else:
  sys.exit(0)
EOF
    case $? in
        101)
            # Using virtualenv 1.7.2, which is compatible with Python version < 2.5
            warn "Using an old and possibly unsupported version of 'virtualenv' (1.7.2)"
            VIRTUALENV_URL=$VIRTUALENV_172_URL
            warn "Using an old and possibly unsupported version of 'pip' (1.1)"
            download pip-1.1.tar.gz $PIP_11_URL
            ;;
        0)
            # using latest virtualenv
            VIRTUALENV_URL=$VIRTUALENV_LATEST_URL
            pip_download pip
            pip_download setuptools
            # download $(basename $PIP_URL) $PIP_URL
            # download $(basename $SETUPTOOLS_URL) $SETUPTOOLS_URL
            ;;
        *)
            die 4 "unable to check python version" <<EOF
The script was unable to check the version of the Python
language installed (check returned an exit status of "$?").
This check is needed to know which version of the "virtualenv"
auxiliary script we need to download.
EOF
            ;;
    esac

    download virtualenv.py $VIRTUALENV_URL
    VIRTUALENV_CMD="$PYTHON virtualenv.py"

    # python virtualenv.py --[no,system]-site-packages $DESTDIR
    $VIRTUALENV_CMD $verbose $WITH_SITE_PACKAGES -p $PYTHON $DESTDIR
}

install_gc3pie_via_pip () {
    PATH=$VENVDIR/bin:$PATH
    if ! test -x $VENVDIR/bin/pip; then
        # sometimes virtualenv installs pip as pip-X.Y, try to patch this
        for pip in $VENVDIR/bin/pip-*; do
            if test -x $pip; then
                ln $verbose -s $pip $VENVDIR/bin/pip
                break
            fi
        done
    fi
    if ! test -x $VENVDIR/bin/pip; then
        die 5 "cannot find command 'pip' in '$VENVDIR/bin'" <<EOF
The script was unable to create a valid virtual environment.
EOF
    fi
    echo "Installing GC3Pie from PyPI package with '$VENVDIR/bin/pip' ..."
    $VENVDIR/bin/pip install gc3pie
}

install_gc3pie_via_svn () {
    require_cc
    require_svn
    (
        cd $VENVDIR
        echo "Downloading GC3Pie from subversion repository $GC3_SVN_URL"
        svn co $GC3_SVN_URL src
        cd src
        python setup.py develop
    )
}

install_gc3apps () {
    echo "Installing extra applications in $VENVDIR"

    if [ $DEVELOP -eq 0 ]
    then
        for cmd in $VENVDIR/gc3apps/*.py
        do
            ln $verbose -s $cmd $VENVDIR/bin/$(basename $cmd .py)
            # setup.py install package_data without the 'x' permission
            chmod $verbose +x "$cmd"
        done
    else
        LIBDIR=$VENVDIR/src/gc3apps
        COMMANDS="gc3.uzh.ch/gridrun.py \
              zods/gzods.py \
              geotop/ggeotop.py \
              geotop/ggeotop_utils.py \
              ieu.uzh.ch/gmhc_coev.py \
              ieu.uzh.ch/gbiointeract.py \
              turbomole/gricomp.py \
              rosetta/gdocking.py \
              rosetta/grosetta.py \
              lacal.epfl.ch/gcrypto.py \
              codeml/gcodeml.py \
              gamess/grundb.py \
              gamess/ggamess.py"

        for cmd in $COMMANDS
        do
            binary=$(basename $cmd .py)
            ln $verbose -s  $LIBDIR/$cmd     $VENVDIR/bin/$binary
        done
    fi
}

usage () {
    cat <<EOF
This program will install GC3Pie in your directory '$HOME/gc3pie'.

usage:
$0 [OPTIONS]

Options:

      -d, --target DIRECTORY Install GC3Pie virtual environment into DIRECTORY.
                             (Default: $VENVDIR)

      -p, --python EXE       The python interpreter to use. The default is $(which python).

      -f, --overwrite        Remove target directory if it already exists.

      -y, --yes              Do not ask for confirmation: assume a 'yes' reply to every question.

      -D, --develop          Install development version.

      -N, --no-gc3apps       Do not install extra GC3 applications, like 'gcodeml', 'grosetta' and 'ggamess'.

      -h, --help             Print this help text.

      -v, --verbose          Be more verbose in reporting.

EOF
}


# Main program
short_opts='d:p:hDNfvxy'
long_opts='target:,python:,help,debug,develop,no-gc3apps,overwrite,verbose,yes'

if [ "x$(getopt -T)" = 'x' ]; then
    # GNU getopt
    args=$(getopt --name "$PROG" --shell sh -l "$long_opts" -o "$short_opts" -- "$@")
    if [ $? -ne 0 ]; then
        abort 1 "Type '$PROG --help' to get usage information."
    fi
    # use 'eval' to remove getopt quoting
    eval set -- $args
else
    # old-style getopt, use compatibility syntax
    args=$(getopt "$short_opts" "$@")
    if [ $? -ne 0 ]; then
        abort 1 "Type '$PROG --help' to get usage information."
    fi
    eval set -- $args
fi

# eval set  -- "$ARGS"

while true
do
    case "$1" in
        -d|--target)
            shift
            VENVDIR=$1
            ;;
        -p|--python)
            shift
            PYTHON=$1
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        -D|--develop)
            DEVELOP=1
            ;;
        -N|--no-gc3apps)
            WITHAPPS=0
            ;;
        -f|--overwrite)
            OVERWRITEDIR=yes
            ;;
        -y|--yes)
            ASKCONFIRMATION=0
            OVERWRITEDIR=yes
            ;;
        -v|--verbose)
            verbose='-v'
            ;;
        -x|--debug)
            set -x
            ;;
        --)
            shift
            break
            ;;
        *)
            warn "Unknown option: $1"
            echo
            usage
            exit 1
            ;;
    esac
    shift
done

if [ $DEVELOP -eq 1 ]; then
    versioninfo="*development version*"
else
    versioninfo="*latest stable version*"
fi

cat <<EOF
==========================
GC3Pie installation script
==========================

This script installs $versioninfo of GC3Pie in "$VENVDIR".

If you encounter any problem running this script, please contact
the GC3Pie team by sending an email to gc3pie@googlegroups.com.

Remember to attach the full output of the script, in order to help us
to identify the problem.

Installation info:

Destination directory:    $VENVDIR
Python executable:        $PYTHON
Ask for confirmation:     $(integer_to_boolean $ASKCONFIRMATION)
Development mode:         $(integer_to_boolean $DEVELOP)
Install gc3apps:          $(integer_to_boolean $WITHAPPS)
Overwrite:                $OVERWRITEDIR

EOF

ask_yn "Are you ready to proceed?"
if [ "$REPLY" = 'no' ]; then
    echo "Aborting installation as requested."
    echo
    exit 0
fi

# check and install prerequisites
install_required_sw

require_python

# Download command
if have_command wget
then
    download () { wget $verbose --no-check-certificate -O "$@"; }
elif have_command curl
then
    download () { curl $verbose --insecure -L -s -o "$@"; }
else
    die 6 "No 'curl' or 'wget' command found." <<EOF
The script needs either one of the 'curl' or 'wget' commands to run.
Please, install at least one of them using the software manager of
your distribution, or downloading it from internet.

wget: http://www.gnu.org/software/wget/

curl: http://curl.haxx.se/
EOF
fi

# Command to get packages from pip
pip_download () {
    pkg=$1
    url=$(download - $BASE_PIP_URL/$pkg/ | grep "source/./$pkg.*z" |sed 's:.*href="\([^#"]*\)["#].*:\1:g' | sort | tail -1 )
    if [ -n "$url" ]; then
        download $(basename $url) $BASE_PIP_URL/$pkg/$url
    else
        die 5 "Package '$pkg' not found on PyPI!" <<EOF
Unable to download package '$pkg' from PyPI.
EOF
    fi
}

# Install virtualenv
if [ -d $VENVDIR ]
then
    if [ $OVERWRITEDIR = 'ask' ]; then
        echo "Destination directory '$VENVDIR' already exists."
        echo "I can wipe it out in order to make a new installation,"
        echo "but this means any files in that directory, and the ones"
        echo "underneath it will be deleted."
        echo
        ask_yn "Do you want to wipe the installation directory '$VENVDIR'?"
        OVERWRITEDIR="$REPLY"
        if [ "$OVERWRITEDIR" = 'no' ]; then
            say "*Not* overwriting destination directory '$VENVDIR'."
            OVERWRITEDIR=no
        fi
    fi
    if [ $OVERWRITEDIR = 'no' ]
    then
        die 7 "Unable to create virtualenv in '$VENVDIR': directory already exists." <<EOF
The script was unable to create a virtual environment in "$VENVDIR"
because the directory already exists.

In order to proceed, you must take one of the following action:

* delete the directory, or

* run this script again adding '--overwrite' option, which will
  overwrite the $VENVDIR directory, or

* specify a different path by running this script again adding the
  option "--target" followed by a non-existent directory.
EOF
    elif [ $OVERWRITEDIR = 'yes' ]; then
        echo "Removing directory $VENVDIR as requested."
        rm $verbose -rf $VENVDIR
    else
        abort 66 "Internal error: unexpected value '$OVERWRITEDIR' for OVERWRITEDIR."
    fi
fi

echo "Installing GC3Pie virtualenv in directory '$VENVDIR' ..."
install_virtualenv $VENVDIR
rc=$?
if [ $rc -ne 0 ]
then
    die 8 "Unable to create a new virtualenv in '$VENVDIR': 'virtualenv.py' script exited with code $rc." <<EOF
The script was unable to create a valid virtual environment.
EOF
fi

. $VENVDIR/bin/activate

rc=0
if [ $DEVELOP -eq 0 ]
then
    echo "Installing GC3Pie stable release ..."
    install_gc3pie_via_pip
    rc=$?
else
    echo "Installing GC3Pie *development* tree using Subversion ..."
    install_gc3pie_via_svn
    rc=$?
fi

if [ $rc -eq 0 ]
then
    if [ $WITHAPPS -eq 1 ]
    then
        install_gc3apps
        if [ $? -ne 0 ]
        then
            echo "WARNING"
            echo "======="
            echo "GC3Pie is installed. However, extra applications have not been installed because of an error."
            echo "Please, check the previous logs and send an email to gc3pie@googlegroups.com if you need help."
            echo
        fi
    fi
    echo "==============================="
    echo "Installation of GC3Pie is done!"
    echo "==============================="
    echo
    echo "In order to work with GC3Pie you have to enable the virtual"
    echo "environment with the command:"
    echo
    echo "    . $VENVDIR/bin/activate"
    echo
    echo "You need to run the above command on every new shell you open just once before using GC3Pie commands."
    echo
    echo "If the shell's prompt starts with '(gc3pie)' it means that the virtual environment"
    echo "has been enabled."

fi
