#!/bin/bash
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

die () {
  rc="$1"
  shift
  (echo -n "$PROG: ERROR: ";
      if [ $# -gt 0 ]; then echo "$@"; else cat; fi) 1>&2
  exit $rc
}

say () {
    echo "$PROG: $@";
}

warn () {
    echo 1>&2 "$PROG: WARNING: $@";
}

have_command () {
  type "$1" >/dev/null 2>/dev/null
}

require_command () {
  if ! have_command "$1"; then
    die 1 "Could not find required command '$1' in system PATH. Aborting."
  fi
}

require_cc () {
    have_command $CC || have_command cc
    if [ $? -ne 0 ]
    then
        cat 1>&2 <<EOF
====================================================
GC3Pie install: ERROR: Unable to find a C compiler!"
====================================================

To install the GC3Pie development branch, a C language compiler
is needed.

Please, install one using the software manager of your distribution.
If this computer already has a C compiler, set the environment variable
CC to the full path to the C compiler command.

If the above looks like Greek to you, please contact the GC3Pie team
by sending an email to gc3pie@googlegroups.com.

Aborting installation!
EOF
        exit 1
    fi
}

require_svn () {
    have_command svn
    if [ $? -ne 0 ]
    then
        cat 1>&2 <<EOF
========================================================
GC3Pie install: ERROR: Unable to find the 'svn' command!
========================================================

To install the GC3Pie development branch installation,
the SubVersion ('svn') command is needed.

Please, install it using the software manager of your distribution
or download it from http://subversion.tigris.org/

If the above looks like Greek to you, please contact the GC3Pie team
by sending an email to gc3pie@googlegroups.com.

Aborting installation!
EOF
        exit 1
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
            cat 1>&2 <<EOF
=====================================================
GC3Pie install: ERROR: missing software prerequisites
=====================================================

The following software packages need to be installed
in order for GC3Pie to work: $missing

Please ask your system administrator to install them,
or, if you have root access, you can do that by
running the following command from the 'root' account:

    apt-get install $missing

EOF
            exit 2
        fi
    elif have_command yum; then
        # RHEL/CentOS
        missing=$(which_missing_packages subversion python-devel gcc gcc-g++)
        if [ -n "$missing" ]; then
            cat 1>&2 <<EOF
=====================================================
GC3Pie install: ERROR: missing software prerequisites
=====================================================

The following software packages need to be installed
in order for GC3Pie to work: $missing

Please ask your system administrator to install them,
or, if you have root access, you can do that by
running the following command from the 'root' account:

    yum install $missing

EOF
            exit 2
        fi
    elif have_command zypper; then
        # SuSE
        warn "Cannot check if requisite software is installed: SuSE and compatible Linux distributions are not yet supported. I'm proceeding anyway, but you may run into errors later. Please write to gc3pie@googlegroups.com asking for information."
    else
        # ???
        die 1 "Cannot determine what package manager this Linux distribution has, so I cannot check if requisite software is installed. I'm proceeding anyway, but you may run into errors later. Please write to gc3pie@googlegroups.com to get help."
    fi
}

install_virtualenv () {
    DESTDIR=$1

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
            ;;
        *)
            cat 1>&2 <<EOF
=====================================================
GC3Pie install: ERROR: unable to check python version
=====================================================

The script was unable to check the version of the Python
language installed (check returned an exit status of "$?").
This check is needed to know which version of the "virtualenv"
auxiliary script we need to download.

Please contact the GC3Pie team by sending an email to
'gc3pie@googlegroups.com' and attach the full output of
this script, in order to help us identify the problem.

Aborting installation!
EOF
            ;;
    esac

        download virtualenv.py $VIRTUALENV_URL
        VIRTUALENV_CMD="$PYTHON virtualenv.py"

    # python virtualenv.py --system-site-packages $DESTDIR
    $VIRTUALENV_CMD --system-site-packages -p $PYTHON $DESTDIR
}

install_gc3pie_via_pip () {
    PATH=$VENVDIR/bin:$PATH
    if ! have_command pip; then
cat 1>&2 <<EOF
===============================================
GC3Pie install: ERROR: 'pip' command not found.
===============================================

The script was unable to create a valid virtual environment. If the
above output does not help you in solving the issue, please contact
the GC3Pie team by sending an email to gc3pie@googlegroups.com.

Remember to attach the full output of the script, in order to help us
to identify the problem.

Aborting installation!
EOF
        exit 1
    fi
    echo "Installing GC3Pie from PIP package"
    pip install gc3pie
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
            ln -s $cmd $VENVDIR/bin/$(basename $cmd .py)
        done
    else
        LIBDIR=$VENVDIR/src/gc3apps
        COMMANDS="gc3.uzh.ch/gridrun.py \
              zods/gzods.py \
              geotop/ggeotop.py \
              geotop/ggeotop_utils.py \
              ieu.uzh.ch/gmhc_coev.py \
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
            ln -s  $LIBDIR/$cmd     $VENVDIR/bin/$binary
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

      --develop              Install development version. Requires svn and gcc.

      --no-gc3apps           Do not install extra GC3 applications, like gcodeml, grosetta and gamess.

      --overwrite            Remove target directory if it already exists.

      --yes                  Do not ask for confirmation: assume a 'yes' reply to every question.

      -h, --help             print this help
EOF
}


# Main program
ARGS=$(getopt -o "d:p:h" -l "target:,python:,help,develop,no-gc3apps,overwrite,yes" -- "$@")
eval set  -- "$ARGS"

while true
do
    case "$1" in
        -d|--target)
            shift
            VENVDIR=$(readlink -f $1)
            ;;
        -p|--python)
            shift
            PYTHON=$1
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --develop)
            DEVELOP=1
            ;;
        --no-gc3apps)
            WITHAPPS=0
            ;;
        --overwrite)
            OVERWRITEDIR=yes
            ;;
        --yes)
            ASKCONFIRMATION=0
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

if [ $ASKCONFIRMATION -eq 1 ]
then
    read -p "Are you ready to proceed? [yN] " yn
    if [ "$yn" != "y" -a "$yn" != "Y" ]
    then
        echo "Aborting installation as requested"
        exit 0
    fi
    echo
fi

# check and install prerequisites
install_required_sw

# Download command
if have_command curl
then
    download () { curl -L -s -o "$@"; }
elif have_command wget
then
    download () { wget -O "$@"; }
else
    cat 1>&2 <<EOF
=========================================================
GC3Pie install: ERROR: No 'curl' or 'wget' command found.
=========================================================

The script needs either one of the 'curl' or 'wget' commands to run.
Please, install at least one of them using the software manager of
your distribution, or downloading it from internet.

wget: http://www.gnu.org/software/wget/

curl: http://curl.haxx.se/

Aborting installation!
EOF
    exit 1
fi

# Install virtualenv
if [ -d $VENVDIR ]
then
    if [ $OVERWRITEDIR = 'ask' ]; then
        echo "Destination directory '$VENVDIR' already exists."
        echo "I can wipe it out in order to make a new installation,"
        echo "but this means any files in that directory, and the ones"
        echo "underneath it will be deleted."
        echo
        read -p "Do you want to wipe the installation directory '$VENVDIR'? [yN] " yn
        if [ "$yn" != "y" -a "$yn" != "Y" ]
        then
            say "*Not* overwriting destination directory '$VENVDIR'."
            OVERWRITEDIR=no
        else
            OVERWRITEDIR=yes
        fi
    fi
    if [ $OVERWRITEDIR = 'no' ]
    then
cat 1>&2 <<EOF
=============================================================================================
GC3Pie install: ERROR: Unable to create a virtualenv in "$VENVDIR": directory already exists.
=============================================================================================

The script was unable to create a virtual environment in "$VENVDIR"
because the directory already exists.

In order to proceed, you must take one of the following action:

* delete the directory, or

* run this script again adding '--overwrite' option, which will
  overwrite the $VENVDIR directory, or

* specify a different path by running this script again adding the
  option "--target" followed by a non-existent directory.

Aborting installation!
EOF
        exit 1
    elif [ $OVERWRITEDIR = 'yes' ]; then
        echo "Removing directory $VENVDIR as requested."
        rm -rf $VENVDIR
    else
        die 66 "Internal error: unexpected value '$OVERWRITEDIR' for OVERWRITEDIR."
    fi
fi

echo "Installing GC3Pie virtualenv in $VENVDIR ..."
install_virtualenv $VENVDIR
if [ $? -ne 0 ]
then
    cat 1>&2 <<EOF
===========================================================================================================
GC3Pie install: ERROR: Unable to create a new virtualenv in $VENVDIR: "virtualenv.py" script exit status: $?
===========================================================================================================

The script was unable to create a valid virtual environment. If the
above output does not help you in solving the issue, please contact
the GC3Pie team by sending an email to gc3pie@googlegroups.com.

Remember to attach the full output of the script, in order to help us
to identify the problem.

Aborting installation!
EOF
    exit 1
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
