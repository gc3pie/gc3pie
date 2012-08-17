#!/bin/bash
# @(#)install.sh
#
#
#   Copyright (C) 2012 by Antonio Messina <amessina@ictp.it> for the 
#   Abdus Salam International Center for Theoretical Phisics (ICTP). 
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

VIRTUALENV_URL="https://raw.github.com/pypa/virtualenv/master/virtualenv.py"
GC3_SVN_URL="http://gc3pie.googlecode.com/svn/trunk/gc3pie"

# Defaults
VENVDIR=$HOME/gc3pie
DEVELOP=0
WITHAPPS=0
OVERWRITEDIR=0
ASKCONFIRMATION=1

# Download command
if [ $(which curl) ]; then
    dl_cmd="curl -L -s -o"
else
    dl_cmd="wget -O"
fi

# Auxiliary functions

function install_virtualenv(){
    DESTDIR=$1
    $dl_cmd virtualenv.py $VIRTUALENV_URL

    python virtualenv.py --system-site-packages $DESTDIR    
}

function install_gc3pie_via_pip(){

    pipbin=($VENVDIR/bin/pip*)
    if [ ${#pipbin[*]} -lt 1 ]; then
        echo "No PIP binary found. Error in creating the Virtual Environment"
        exit 1
    fi
    pip=${pipbin[0]}
    echo "Installing GC3Pie from PIP package $GC3_URL"
    $pip install gc3pie
}

function install_gc3pie_via_svn(){

    which svn >&/dev/null
    if [ $? -ne 0 ]
    then
        echo "Unable to find Subversion binary!"
        echo "GC3Pie development branch installation *needs* subversion."
        echo "Please, install it using the software manager of your distribution"
        echo "or downloading it from http://subversion.tigris.org/"
        exit 1
    fi

    which cc >&/dev/null
    if [ $? -ne 0 ]
    then
        echo "Unable to find a C compiler"
        echo "GC3Pie development branch installation *needs* a C compiler"
    fi

    (
        cd $VENVDIR
        echo "Downloading GC3Pie from subversion repository $GC3_SVN_URL"
        svn co $GC3_SVN_URL src
        cd src
        python setup.py develop
    )
}

function install_gc3apps(){
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

function usage(){
cat <<EOF
This program will install GC3Pie in your home directory.

usage:
$0 [OPTIONS]

Options

      --add-file=FILE        add given FILE to the archive (useful if its name
      -d, --target=DIRECTORY Install GC3Pie virtual environment int DIRECTORY.
                             (Default: $VENVDIR)
      --develop              Install development version. Requires svn and gcc.
      --gc3apps              Install extra GC3 applications, like gcodeml, rosetta, turbomole and gamess.
      --overwrite            Remove target directory if it already exists.
      --batch                Run in batch mode, without asking confirmation.
      -h, --help             print this help
EOF
}

ARGS=$(getopt -o "d:h" -l "target:,help,develop,gc3apps,overwrite,batch" -- "$@")
eval set  -- "$ARGS"

# Main program
while true
do
    case "$1" in
        -d|--target)
            shift
            VENVDIR=$1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --develop)
            shift
            DEVELOP=1
            ;;
        --gc3apps)
            shift
            WITHAPPS=1
            ;;
        --overwrite)
            shift
            OVERWRITEDIR=1
            ;;
        --batch)
            shift
            ASKCONFIRMATION=0
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Unknown option $1"
            echo
            usage
            exit 1
            ;;
    esac
done

versioninfo="*latest stable version* of"
[ $DEVELOP -eq 1 ] && versioninfo="*development version* of"

cat <<EOF
==========================
GC3Pie installation script
==========================

This script will install $versioninfo GC3Pie in "$VENVDIR". 

If you will encounter any problem running this script, please contact
the GC3Pie team by sending an email to gc3pie@googlegroups.com

Remember to attach the full output of the script, in order to help us
to identify the problem.

EOF

if [ $ASKCONFIRMATION -eq 1 ]
then
    read -p "Are you ready to proceed? [yN] " yn
    if [ "$yn" != "y" -a "$yn" != "Y" ]
    then
        echo "Aborting installation"
        exit 0
    fi
fi


# Install virtualenv
if [ -d $VENVDIR ]
then
    if [ $OVERWRITEDIR -eq 0 ]
    then
        echo "Directory $VENVDIR already exists."
        echo "Please specify a different one using the --target option"
        echo "or force installation using --overwrite option."
        exit 1
    else
        echo "Removing directory $VENVDIR as requested."
        rm -rf $VENVDIR
    fi
else
    echo "Installing GC3Pie virtualenv in $VENVDIR"
fi

install_virtualenv $VENVDIR

if [ $? -ne 0 ]
then
    echo "Error installing virtualenv in $VENVDIR. GC3Pie is NOT installed!"
    echo "Please, check previous logs and send an email to gc3pie@googlegroups.com"
    echo "with the full output if you need help."
    exit 1
fi

source $VENVDIR/bin/activate

rc=0
if [ $DEVELOP -eq 0 ]
then
    echo "Installing GC3Pie stable release"
    install_gc3pie_via_pip
    rc=$?
else
    echo "Installing GC3Pie *development* tree using Subversion"
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
    echo "In order to work with GC3Pie you have to enale the virtual"
    echo "environment with the command:"
    echo
    echo "    source ~/gc3pie/bin/activate"
    echo
    echo "You need to run the above command on every new shell you open just once before using GC3Pie commands."
    echo 
    echo "If the shell's prompt starts with '(gc3pie)' it means that the virtual environment"
    echo "has been enabled."
    
fi