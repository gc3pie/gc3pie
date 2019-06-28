#!/usr/bin/env python
#
#  Copyright (C) 2015-2019 University of Zurich.
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
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
from __future__ import absolute_import
__version__ = '2.0.5'
__author__ = '''
Antonio Messina <antonio.s.messina@gmail.com>
Riccardo Murri <riccardo.murri@gmail.com>
'''

## first, check that we're running a compatible version of Python

import sys

# since we do not (yet) know what version of Python this script is
# running on, we can only use a very restricted subset of Python, in
# order to stay comaptible with the broadest range of version:
#
# - use `sys.hexversion` since it's there since 1.5.2
# - use `sys.stdout.write()` instead of `print()` since it did not
#   change syntax during the 2 to 3 transition
# - do not use `in` and `not in` operators
# - do not rely on `os` for the `EX_*` error exit codes
# - no use of the `True` and `False` constants

major = (sys.hexversion >> 24)
minor = (sys.hexversion >> 16) & 0xff
release = (sys.hexversion >> 8) & 0xff

if major < 2 or (major == 2 and minor < 6) or major >= 3:
    sys.stderr.write("""
GC3Pie requires Python version 2.6 or 2.7.
Unfortunately, the Python interpreter '%s'
is running version %d.%d.%d of the language.

If a version of Python suitable for using GC3Pie is present in some
non-standard location, then please run this script again through
the correct 'python' binary.  For example:

  /usr/local/bin/python26 %s
""" % (sys.executable, major, minor, release, sys.argv[0]))
    sys.exit(70) # os.EX_UNAVAILABLE


## now we know we're running Py 2.6+, do the rest of the setup

import logging

# see: http://stackoverflow.com/questions/377017/test-if-executable-exists-in-python
from distutils.spawn import find_executable

# for use in `ask`, see: http://stackoverflow.com/a/14274466/459543
from distutils.util import strtobool

# for use in `download_from_pypi`
from distutils.version import LooseVersion

from fnmatch import fnmatch

import os
from os import path

import re

import shutil
import tarfile

from subprocess import call, check_call, CalledProcessError
try:
    from subprocess import check_output
except:
    from subprocess import PIPE, Popen
    # Possibly running python 2.6
    def check_output(*popenargs, **kwargs):
        if 'stdout' in kwargs:
            raise ValueError('stdout argument not allowed, it will be overridden.')
        process = Popen(stdout=PIPE, *popenargs, **kwargs)
        output, unused_err = process.communicate()
        retcode = process.poll()
        if retcode:
            cmd = kwargs.get("args")
            if cmd is None:
                cmd = popenargs[0]
            raise CalledProcessError(retcode, cmd, output=output)
        return output



from urllib2 import urlopen
import json


## defaults and constants

PROG="GC3Pie install"

class default:
    BASE_PIP_URL="https://pypi.python.org/pypi"
    GC3PIE_REPO_URL="https://github.com/uzh/gc3pie.git"
    TARGET = path.expandvars('$HOME/gc3pie')
    UNRELEASED = False
    WITH_APPS = True


# by default, ask for confirmation
DO_NOT_ASK_AND_ASSUME_YES = False

paths_to_cleanup = []

def cleanup(paths=paths_to_cleanup):
    if isinstance(paths, (str, basestring)):
        paths = [paths]

    for path in paths:
        path = os.path.abspath(path)
        typ = 'directory' if os.path.isdir(path) else 'file'
        if ask("Do you want to delete %s '%s'?" % (typ, path)):
            logging.info("Deleting %s '%s' as requested...", typ, path)
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)


def cleanup_defer(paths):
    if isinstance(paths, (str, basestring)):
        paths = [paths]
    paths_to_cleanup.extend([os.path.abspath(path) for path in paths])


## main

logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stderr,
    format=('<{me}> %(levelname)8s: %(message)s'.format(me=PROG)),
)
try:
    logging.captureWarnings(True)
except AttributeError:
    # Possibly running on python 2.6. Ignore.
    pass

def main():
    options = parse_command_line_options()
    if options.unreleased:
        options.version_info = '*development version*'
    else:
        options.version_info = '*latest stable version*'

    # compatibility check
    if options.features != 'none' and (major, minor) == (2, 6):
        abort(70, """Optional features require Python 2.7+

Python interpreter '{exe}' is running version
{major}.{minor}.{release} of the language.

If a version of Python suitable for using GC3Pie is present in some
non-standard location, then please run this script again through
the correct 'python' binary.  For example:

        /usr/local/bin/python2.7 {me}
        """.format(exe=sys.executable,
                   major=major, minor=minor, release=release,
                   me=__file__))

    print_intro_and_options(options)

    require_sw_prerequisites()

    check_target_directory(options.target, options.overwrite)

    create_virtualenv(options.target, options.python)

    if options.unreleased:
        logging.info("Installing GC3Pie master source tree from GitHub ...")
        install_gc3pie_from_github(options.target, options.features)
    else:
        logging.info("Installing GC3Pie released code from PyPI ...")
        install_gc3pie_from_pypi(options.target, options.features)

    print("""
===============================
Installation of GC3Pie is done!
===============================

In order to work with GC3Pie you have to enable the virtual
environment with the command (yes, it starts with a dot+space):

    . {target}/bin/activate

You need to run the above command on every new shell you open before
using GC3Pie commands, but just once per session.

If the shell's prompt starts with '(gc3pie)' it means that the virtual
environment has been enabled.

    """.format(target=options.target))

    cleanup()
    sys.exit(os.EX_OK)


## auxiliary functions
#
# Functions are defined in *alphabetical order*
# to make them simpler to find; Python's runtime does
# not care about definition order.
#

def abort(rc, msg=None):
    """
    Print error message `msg` (if any) and abort program with exit code `rc`.
    """
    if msg:
        sys.stderr.write("{me}: FATAL: {msg}".format(me=PROG, msg=msg))
        sys.stderr.write('\n')
    sys.exit(rc)


def ask(question, default="yes"):
    """
    Ask a yes/no question via raw_input() and return the answer.

    `question` is a string that the user is prompted with.

    `default` is the presumed answer if the user just hits <Enter>.
    It must be "yes" (the default), "no" or None (meaning an answer is
    required of the user).

    Return ``True`` for a "yes" reply, or ``False`` for "no".

    See <http://stackoverflow.com/a/3041990/459543> for the original
    sources.
    """
    assert default in [None, 'yes', 'no']

    if DO_NOT_ASK_AND_ASSUME_YES:
        print(question + " (Assuming a 'yes' reply because of `--yes` command-line option.)")
        return True

    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            choice = default
        try:
            return strtobool(choice)
        except ValueError:
            sys.stdout.write("Please respond with 'yes' or 'no'.\n")


def bool_to_yn(val):
    """Return ``yes`` or ``no`` depending on the truth value of `val`."""
    return ('yes' if val else 'no')


def check_ok_to_continue_or_abort(msg):
    """
    Print a warning message and ask user for confirmation to continue.
    Abort execution if user does not want to proceed.
    """
    print("""
WARNING
=======

{msg}

    """.format(**locals()))
    proceed = ask("Do you still want to proceed?")
    if not proceed:
        logging.fatal("Aborting installation as requested.")
        sys.exit(os.EX_TEMPFAIL)
    return True


def check_target_directory(target, overwrite=None):
    """
    Ensure that `target` directory does *not* exist.

    Second argument `overwrite` can have three values:

    - ``None``: if target exists, ask user permission to remove it;
    - ``False``: do not remove target if it exists;
    - ``True``: wipe target away if exists, without warning.
    """
    if path.exists(target):
        if overwrite is None:
            print("""
Destination directory '{target}' already exists.
I can wipe it out in order to make a new installation,
but this means any files in that directory, and the ones
underneath it will be deleted.

            """.format(**locals()))
            wipe = ask(
                "Do you want to wipe the installation directory '{target}' ?"
                .format(** locals()))
            if wipe:
                overwrite = True
            else:
                overwrite = False

        # if directory exists and cannot be overwritten, abort
        if not overwrite:
            die(os.EX_CANTCREAT,
                "Unable to create virtualenv: target directory already exists",
                """
The script was unable to create a virtual environment in "{target}"
because the directory already exists.

In order to proceed, you must take one of the following action:

* delete the directory, or

* run this script again adding '--overwrite' option, which will
  overwrite the {target} directory, or

* specify a different path by running this script again adding the
  option "--target" followed by a non-existent directory.
                """.format(**locals()))

        # else remove the entire directory tree
        else:
            logging.info("Deleting directory '%s' as requested ...", target)
            shutil.rmtree(target)


def create_virtualenv(destdir, python=sys.executable):
    """
    Create a Python virtual environment into `destdir`.
    """

    # cannot run if already inside a virtenv
    if os.environ.get('VIRTUAL_ENV'):
        die(os.EX_SOFTWARE,
            "A virtual environment is already active",
            """
A Python virtual environment seems to be already *active*. This
will cause this script to FAIL.

Please run 'deactivate', then run this script again.
            """)

    # Anaconda Python needs special treatment because of the relative
    # RPATH, see Issue 479
    if 'Anaconda' in sys.version:
        python_is_anaconda = True
        anaconda_root_dir = fix_virtualenv_issue_with_anaconda(python, destdir)
    else:
        python_is_anaconda = False

    # check for conditions that require us to skip site packages:
    # - GC3Pie already installed
    # - `pip` or `easy_install` already in the system path (cannot upgrade setuptools and friends)
    if (have_command('gc3utils')
        or have_command('easy_install')
        or have_command('pip')):
        # Ananconda Python ships setuptools so we have to explicitly exclude them
        if python_is_anaconda:
            with_site_packages = ['--system-site-packages', '--no-setuptools']
            check_ok_to_continue_or_abort("""
Anaconda Python detected!  I'm creating the virtual environment with a
'--system-site-packages' option, to let GC3Pie programs use all the
libraries that come bundled with Anaconda.  If this leads to errors
related to 'setuptools', 'distribute' or 'pip', then please report a
bug to the GC3Pie mailing list gc3pie@googlegroups.com
""")
        else:
            # XXX: could this be replaced by `--no-setuptools` simply?
            with_site_packages = ['--no-site-packages']
    else:
        # no issue in allowing access to system site packages
        with_site_packages = ['--system-site-packages']

    tarball = download_from_pypi('virtualenv', keep=False)

    tar = tarfile.open(tarball)
    tar.extractall()
    venvsourcedir = tar.getnames()[0]
    cleanup_defer(venvsourcedir)

    # search 'virtualenv.py'
    venvpath = search_for(venvsourcedir, 'virtualenv.py')
    if not venvpath:
        die(os.EX_SOFTWARE,
            "Failed to locate `virtualenv.py`.",
            """
            Failed to locate `virtualenv.py` in downloaded package directory `{0}`
            """.format(venvsourcedir))

    # run: python virtualenv.py --[no,system]-site-packages $DESTDIR
    try:
        check_call(
            [python, venvpath]
            + with_site_packages
            + ['-p', python, destdir])
        logging.info("Created Python virtual environment in '%s'", destdir)
    except CalledProcessError as err:
        rc = err.returncode
        die(os.EX_SOFTWARE,
            "Cannot create virtual environment",
            """
The command::

        {python} virtualenv.py {with_site_packages} -p {python} {destdir}

exited with failure code {rc} and did not create a Python virtual environment.

Please get in touch with the GC3Pie developers at the email address
gc3pie@googlegroups.com to get help with this error.
            """.format(**locals()))

    if python_is_anaconda:
        # since we created the virtenv with `--no-setuptools`, the
        # `pip` executable has not been created; copy the one shipped
        # with Anaconda now
        run(
            "sed -e '1s^{anaconda_root_dir}/bin/python^{destdir}/bin/python^'"
            " < '{anaconda_root_dir}/bin/pip' > '{destdir}/bin/pip'"
            .format(**locals()))
        run("chmod -v +x '{destdir}/bin/pip'".format(**locals()))
        logging.info("Installed Anaconda's version of `pip` into the virtual env")

    # sometimes virtualenv installs pip as pip-X.Y, try to patch this
    bindir = '{destdir}/bin/'.format(**locals())
    if not path.isfile('{bindir}/pip'.format(**locals())):
        # find `pip` version that matches current Python
        target = ('%d.%d' % sys.version_info[:2])
        pips = [entry for entry in os.listdir(bindir)
                if entry.startswith('pip-')]
        for pip in pips:
            _, pip_version = pip.split('-')
            if pip_version == target:
                os.symlink('{bindir}/{pip}'.format(**locals()),
                           '{bindir}/pip'.format(**locals()))
                logging.info("Using `%s` as default `pip` tool", pip)
                break
        else:
            # no `pip-X.Y` matched
            die(os.EX_UNAVAILABLE, "No `pip` executable found",
                """
No `pip` executable was installed in the Python virtual environment.

Installation cannot continue; please get in touch with the GC3Pie
developers at the email address gc3pie@googlegroups.com to get help
with this error.

Please include the following information in your issue report:

>>> sys.executable = {python}
>>> sys.version = {version}
>>> bindir = {bindir}
>>> pips = {pips}
                """.format(version=sys.version, **locals()))

    # Recent versions of `pip` insist that setuptools>=0.8 is
    # installed, because they try to use the "wheel" format for any
    # kind of package.  So we need to update setuptools --without
    # using `pip`!--, or `pip` will error out::
    #
    #     Wheel installs require setuptools >= 0.8 for dist-info support.
    #
    if run_in_virtualenv(
            destdir,
            "pip wheel --help 1>/dev/null 2>/dev/null",
            abort_on_nonzero_exit=False) == 0:
        logging.info("Trying to install or upgrade setuptools ...")
        # NOTE: setuptools 2.x requires Python >= 2.6; since GC3Pie 2.1+
        # dropped support for Python <2.6, we assume the Python version
        # has already been checked when the code gets here and do not test
        # it again ...
        try:
            if path.exists(path.join(destdir, 'bin/easy_install')):
                logging.info("`easy_install` is available, using it to upgrade setuptools")
                run_in_virtualenv(destdir, "easy_install -U setuptools")
            else:
                tfile = download_from_pypi('setuptools', keep=False)
                logging.info("Downloaded %s from PyPI; now installing it ...",
                             path.basename(tfile))
                run("tar -xzf {tfile}".format(**locals()))
                run_in_virtualenv(
                    destdir,
                    "cd setuptools-* && {python} setup.py install".format(**locals()))
        except CalledProcessError as err:
            die(os.EX_SOFTWARE,
                "Failed to install the latest version of Python 'setuptools'",
                """
The required Python package 'setuptools' could not be installed.

Please get in touch with the GC3Pie developers at the email address
gc3pie@googlegroups.com to get help with this error.
                """)


def die(rc, header, msg):
    """
    Print error message `msg` preceded by the given `header`, then
    abort program with exit code `rc`.
    """
    sys.stderr.write("""
====================================================
    {me}: ERROR: {header}
====================================================

{msg}

If the above does not help you solve the issue, please contact the
GC3Pie team by sending an email to gc3pie@googlegroups.com
(mailing-list subscription required).  Include the full output of this
script to help us identifying the problem.

Aborting installation!
    """.format(me=PROG, header=header, msg=msg))
    sys.exit(rc)


def download(url, to_file=None, keep=True):
    """
    Download contents of `url` to a local file.
    Return name of the local file.

    The local file name is derived from `url` by taking the segment
    after the last `/`, but can be overridden by explicitly passing a
    `to_file` argument.
    """
    if to_file is None:
        to_file = path.basename(url)
    src = urlopen(url)
    dst = open(to_file, 'w')
    dst.write(src.read())
    if not keep:
        cleanup_defer(to_file)
    return to_file


def download_from_pypi(pkgname, pip_url=default.BASE_PIP_URL, version=None, keep=False):
    """
    Download the given package from PyPI into a local file.
    Return path to the downloaded file.

    The local file name is derived from the package source URL; see
    `download`:func: for details.

    If there are multiple versions of the same package available, the
    one actually downloaded is the one which comes first in Python
    string sorting order.
    """
    base_url = (pip_url + '/' + pkgname + '/json')
    try:
        data = urlopen(base_url).read()
    except:
        die(os.EX_PROTOCOL,
            ("Package '{pkgname}' not found on PyPI!".format(**locals())),
            """
Unable to download package {pkgname} from PyPI.
""".format(**locals()))

    try:
        jdata = json.loads(data)
    except ValueError:
        raise ValueError(
            "While downloading from %s, we received an invalid JSON"
            " data." % base_url)

    if not version:
        version = jdata['info']['version']
    try:
        releases = jdata['releases'][version]
    except KeyError:
        raise ValueError(
            "Unable to find version '%s' of PyPI package '%s'."
            " Available versions are: %s" % (
                version, pkgname, sorted(jdata['releases'].keys())))

    sources = [r for r in releases if r['packagetype'] == 'sdist']
    if not sources:
        raise ValueError(
            "Unable to find source package for version %s of package %s on PyPI" % (
                version, pkgname))
    src = sources[-1]
    url = src['url']
    return download(url, keep=keep)


def fix_virtualenv_issue_with_anaconda(python, destdir):
    """
    Apply a workaround for Anaconda Python's incompatibility with virtualenvs.

    The issue is that Anaconda Python ships an executable built with a
    relative RPATH; if the executable is located in Ananconda's
    distribution directory, then the correct `libpython.so` is found
    and loaded.  Otherwise, Linux' `ld.so` falls back to searching for
    `libpython.so` in the standard library search path, which ends up
    finding the system Python `libpython.so` (or none at all) and
    breaks things in subtle ways. (For instance, Anaconda-shipped
    packages are no longer found...)  Since Python's `virtualenv.py`
    *copies* the executable into the virtualenv (don't ask me why),
    this breakage is certain.

    The workaround is simply to create a `lib/` directory in the virtualenv
    directory and symlink the Anaconda Python libraries from there.

    See GC3Pie issue 495 for more information and the sources for this
    workaround.
    """
    anaconda_root_dir = path.realpath(path.join(path.dirname(python), '..'))
    anaconda_lib_dir = path.join(anaconda_root_dir, 'lib')
    if not path.isdir(anaconda_lib_dir):
        die(os.EX_SOFTWARE, "Unexpected Anaconda directory layout",
            """
Anaconda Python detected, but I expected to find a 'lib/' directory
under the root directory '{anaconda_root_dir}', and there is none.
Cannot proceed; please report this issue to the GC3Pie developers
at gc3pie@googlegroups.com.

Please include the following information in your issue report:

>>> sys.executable = {python}
>>> sys.version = {version}
            """.format(version=sys.version, **locals()))

    # list `libpython*.so*` files
    libs = os.listdir(anaconda_lib_dir)
    libpython_files = [filename for filename in libs
                       if (fnmatch(filename, 'libpython*.so*')
                           or fnmatch(filename, 'libpython*.dylib'))]
    if not libpython_files:
        die(os.EX_SOFTWARE, "Unexpected Anaconda directory layout",
            """
Anaconda Python detected, but I expected to find a a libpython.so
file under directory '${anaconda_root_dir}/lib', and there is none.
Cannot proceed; please report this issue to the GC3Pie developers
at gc3pie@googlegroups.com.

Please include the following information in your issue report:

>>> sys.executable = {python}
>>> sys.version = {version}

$ ls -l {anaconda_root_dir}/lib/
{libs}
            """.format(version=sys.version, **locals()))

    # actually do the patching
    logging.info(
        "Applying workaround for Anaconda Python's"
        " incompatibility with virtualenv ...")
    dest_lib_dir = path.join(destdir, 'lib')
    os.makedirs(dest_lib_dir)
    for filename in libpython_files:
        src= path.join(anaconda_lib_dir, filename)
        dst = path.join(dest_lib_dir, filename)
        os.symlink(src, dst)
        logging.info("%s -> %s", src, dst)

    return anaconda_root_dir


__have_command_cache = {}

def have_command(cmd):
    """
    Return ``True`` if `cmd` can be found on the executable search path.
    """
    if cmd not in __have_command_cache:
        __have_command_cache[cmd] = find_executable(cmd)
    return (__have_command_cache[cmd] is not None)


def have_sw_package(pkg):
    """
    Return ``True`` if `pkg` is installed on the system.
    """
    # instead of guessing which distribution this is, we check for the
    # package manager name as it basically identifies the distro
    if have_command('dpkg'):
        # Example output:
        #
        #     $ dpkg -l gcc
        #     Desired=Unknown/Install/Remove/Purge/Hold
        #     | Status=Not/Inst/Conf-files/Unpacked/halF-conf/Half-inst/trig-aWait/Trig-pend
        #     |/ Err?=(none)/Reinst-required (Status,Err: uppercase=bad)
        #     ||/ Name                                  Version                 Architecture            Description
        #     +++-=====================================-=======================-=======================-===============================================================================
        #     ii  gcc                                   4:4.9.2-2ubuntu2        amd64                   GNU C compiler
        #
        try:
            lines = check_output(['dpkg', '-l', pkg]).split('\n')
        except CalledProcessError:
            return False
        found = lines[-2].startswith('ii  ' + pkg)
        return found
    elif have_command('rpm'):
        # `rpm -q` exists with non-zero status if the package is not installed
        try:
            check_call(['rpm', '-q', pkg])
            return True
        except CalledProcessError:
            return False


def install_gc3pie_from_github(venv_dir, features,
                               repo=default.GC3PIE_REPO_URL):
    require_cc()
    require_git()

    logging.info(
        "Downloading GC3Pie from GitHub repository '%s' ...", repo)
    # cloning the entire repo is awfully slow! so simulate `svn`
    # behavior by checking out only the last revision of the sources;
    # people interested in development should be able to check out the
    # full repo on their own, anyway...
    run("git clone --single-branch --depth 1 {repo} '{venv_dir}/src'"
        .format(**locals()))

    # installing `cffi` (required by some dependency of `paramiko`) on
    # Python 2.6 requires that `pycparser` is *already* installed on
    # the system, otherwise the latest version will be unconditionally
    # downloaded and installed, so we need to install it in a separate
    # step *before* installation of the main code...
    if (major, minor) == (2, 6):
        logging.info("Installing pre-requirements for Python 2.6 ...")
        run_in_virtualenv(
            venv_dir, "pip install 'pycparser<2.19' 'pytest==3.2.5'")

    # fix for a stupid boto/pbr dependency issue
    if 'openstack' in features:
        run_in_virtualenv(venv_dir, "pip install pbr")

    if features:
        features_for_pip = '[' + str.join(',', features) + ']'
    else:
        features_for_pip = ''
    # see https://bitbucket.org/tarek/distribute/issue/130 as to why
    # `pip install -e .` is a better `python setup.py develop`
    run_in_virtualenv(
        venv_dir,
        "cd {venv_dir}/src && pip install -e '.{FEATURES}'"
        .format(FEATURES=features_for_pip, **locals()))

    bindir = path.join(venv_dir, 'bin')
    logging.info("Installing extra applications into '%s' ...", bindir)
    apps = [
        "gc3.uzh.ch/gridrun.py",
        "zods/gzods.py",
        "geotop/ggeotop.py",
        "geotop/ggeotop_utils.py",
        "rosetta/gdocking.py",
        "rosetta/grosetta.py",
        "codeml/gcodeml.py",
        "gamess/ggamess.py",
    ]
    gc3apps_dir = path.join(venv_dir, 'src/gc3apps')
    if path.isdir(gc3apps_dir):
        for app in apps:
            src = path.join(gc3apps_dir, app)
            dst = path.join(bindir, path.basename(app))
            logging.info('  %s -> %s', src, dst)
            os.symlink(src, dst)


def install_gc3pie_from_pypi(venv_dir, features):
    # installing `cffi` (required by some dependency of `paramiko`) on
    # Python 2.6 requires that `pycparser` is *already* installed on
    # the system, otherwise the latest version will be unconditionally
    # downloaded and installed, so we need to install it in a separate
    # step *before* installation of the main code...
    if (major, minor) == (2, 6):
        logging.info("Installing pre-requirements for Python 2.6 ...")
        run_in_virtualenv(
            venv_dir, "pip install 'pycparser<2.19' 'pytest==3.2.5'")

    logging.info(
        "Installing GC3Pie from PyPI package with '%s/bin/pip' ...", venv_dir)
    run_in_virtualenv(
        venv_dir,
        "pip install 'gc3pie{features_for_pip}'"
        .format(features_for_pip=str.join(' ', features), **locals()))

    bindir = path.join(venv_dir, 'bin')
    logging.info("Installing extra applications into '%s' ...", bindir)
    gc3apps_dir = path.join(venv_dir, 'gc3apps')
    if path.isdir(gc3apps_dir):
        apps = [path.join(gc3apps_dir, entry)
                for entry in os.listdir(gc3apps_dir)
                if entry.endswith('.py')]
        for app in apps:
            dst = path.join(bindir, path.basename(app))
            logging.info('  %s -> %s', app, dst)
            os.symlink(app, dst)
            # setup.py installs package_data without the 'x' permission
            os.chmod(app, os.stat(app).st_mode|0o755)


def parse_command_line_options():
    import optparse

    cmdline = optparse.OptionParser(usage="usage: %prog [options]",
                                    version=(PROG +' '+ __version__))
    cmdline.add_option("-a", "--feature",
                       action="store",
                       dest="features",
                       default='none',
                       help=(
                           "Install these optional features (comma-separated list)."
                           " Currently defined features are:"
                           "  - openstack: support running jobs in VMs on OpenStack clouds"
                           "  - ec2:       support running jobs in VMs on OpenStack clouds"
                           "  - optimizer: install math libraries needed by the optimizer library"
                           " For instance, to install all features use '-a openstack,ec2,optimizer'."
                           " To install no optional feature, use '-a none' (default)."
                       ))
    cmdline.add_option("-d", "--target",
                       action="store", # optional because action defaults to "store"
                       dest="target",
                       default=default.TARGET,
                       help=("Install GC3Pie virtual environment into this path."))
    cmdline.add_option("-f", "--overwrite", "--remove-target-dir",
                       action="store_true",
                       dest='overwrite',
                       default=None,
                       help=("Remove target directory if it already exists."))
    cmdline.add_option("-y", "--yes",
                       action="store_true",
                       dest='assume_yes',
                       default=False,
                       help=("Do not ask for confirmation: assume a 'yes' reply to every question"))
    cmdline.add_option("-D", "--unreleased", "--development",
                       action="store_true",
                       dest='unreleased',
                       default=False,
                       help=("Install development version."))
    cmdline.add_option("-N", "--no-gc3apps",
                       action="store_false",
                       dest="with_apps",
                       default=True,
                       help=("Do not install example applications, like `gcodeml`, `grosetta`, or `ggames`."))
    (options, args) = cmdline.parse_args()

    if args:
        cmdline.error("No positional arguments allowed!")
        sys.exit(os.EX_USAGE)

    if options.features.lower() == 'none':
        options.features = []
    else:
        options.features = options.features.split(',')

    global DO_NOT_ASK_AND_ASSUME_YES
    DO_NOT_ASK_AND_ASSUME_YES = options.assume_yes

    options.python = sys.executable

    return options


def print_intro_and_options(options):
    print("""
==========================
GC3Pie installation script
==========================

This script installs {version_info} of GC3Pie in '{target}'.

If you encounter any problem running this script, please contact
the GC3Pie team by sending an email to gc3pie@googlegroups.com.

Remember to attach the full output of the script, in order to help us
to identify the problem.

Installation info:

    Destination directory:    {target}
    Overwrite existing dir:   {OVERWRITEDIR}
    Python executable:        {python}
    Ask for confirmation:     {ASK_FOR_CONFIRMATION}
    Install unreleased code:  {UNRELEASED}
    Install gc3apps:          {WITH_APPS}
    Optional features:        {FEATURES}
    Installer script version: {VERSION}

    """.format(
        ASK_FOR_CONFIRMATION=yes_or_no(not DO_NOT_ASK_AND_ASSUME_YES),
        UNRELEASED=yes_or_no(options.unreleased),
        FEATURES=(options.features if options.features else "NONE"),
        OVERWRITEDIR=('ask' if options.overwrite is None else yes_or_no(options.overwrite)),
        VERSION=__version__,
        WITH_APPS=yes_or_no(options.with_apps),
        python=options.python,
        target=options.target,
        version_info=options.version_info,
    ))

    proceed = ask("Are you ready to continue?")
    if not proceed:
          abort(os.EX_TEMPFAIL, "Aborting installation as requested.")


def require_cc():
    """
    Require that a C compiler is runnable.
    """
    found = False
    if 'CC' in os.environ:
        found = have_command(os.environ['CC'])
    if not found:
        found = have_command('cc')
    if not found:
        die(os.EX_UNAVAILABLE, """
To install the GC3Pie development version, a C language compiler
is needed.

Please, install one using the software manager of your distribution.
If this computer already has a C compiler, set the environment variable
CC to the full path to the C compiler command.
        """)


def require_command(cmd):
    """
    Abort execution if command `cmd` cannot be found.
    """
    if not have_command(cmd):
        abort(os.EX_USAGE,
              "Could not find required command '{cmd}' in system PATH."
              " Aborting."
              .format(cmd=cmd))


def require_git():
    if not have_command('git'):
        die(os.EX_UNAVAILABLE, """
To install the GC3Pie development version,
the Git ('git') command is needed.

Please install it using the software manager of your distribution,
or download it from http://git-scm.org/
        """)


def require_sw_prerequisites():
    """Ensure that software prerequisites are installed."""
    # instead of guessing which distribution this is, check for the
    # package manager name as it basically identifies the distro!
    if have_command('dpkg'):
        # Debian/Ubuntu
        required = ['git', 'python-dev', 'gcc', 'g++', 'libffi-dev', 'libssl-dev']
        install_cmd = 'sudo apt-get install'
    elif have_command('yum'):
        install_cmd = 'sudo yum install'
        required = ['git', 'python-devel', 'gcc', 'gcc-c++', 'libffi-devel', 'openssl-devel']
    elif have_command('zypper'):
        # SuSE
        logging.warning(
            "Cannot check if requisite software is installed:"
            " SuSE and compatible Linux distributions are not yet supported."
            " I'm proceeding anyway, but you may run into errors later."
            " Please write to gc3pie@googlegroups.com asking for information.")
        return
    else:
        # MacOSX maybe?
        logging.warning(
            "Cannot determine what package manager this Linux distribution has,"
            " so I cannot check if requisite software is installed."
            " I'm proceeding anyway, but you may run into errors later."
            " Please write to gc3pie@googlegroups.com to get help.")
        return

    missing = which_missing_packages(required)
    if missing:
            print ("""
The following software packages need to be installed
in order for GC3Pie to work: {missing}

There is a small chance that the required software is actually
installed though we failed to detect it, so you may choose to proceed
with GC3Pie installation anyway.  Be warned however, that continuing
is very likely to fail!

""".format(install_cmd=install_cmd, missing=str.join(', ', missing)))
            proceed = ask("Proceed with installation anyway?")
            if proceed:
                logging.warning("Proceeding with installation at your request... keep fingers crossed!")
            else:
                die(os.EX_UNAVAILABLE, "missing software prerequisites",
                    """
Please ask your system administrator to install the missing packages,
or, if you have root access, you can do that by running the following
command from the 'root' account:

        {install_cmd} {missing}

                    """.format(install_cmd=install_cmd,
                               missing=str.join(' ', missing)))


def run(cmd):
    """
    Run `cmd` through the shell.
    This is just a short-cut for `subprocess.check_call`
    """
    check_call(cmd, shell=True)


def run_in_virtualenv(ve_dir, cmd, abort_on_nonzero_exit=True):
    """
Run `cmd` like `check_call` does, but activate Python virtualenv in `ve_dir` first.
    """
    if isinstance(cmd, list):
        cmd = ['.', path.join(ve_dir, 'bin/activate'), ';'] + cmd
    else:
        cmd = ('. {ve_dir}/bin/activate; {cmd}'.format(**locals()))
    try:
        check_call(cmd, shell=True)
        return 0
    except CalledProcessError as err:
        if abort_on_nonzero_exit:
            raise
        else:
            return err.returncode


def search_for(path, filename):
    for rootdir, dirs, filenames in os.walk(path):
        if filename in filenames:
            return os.path.abspath(os.path.join(rootdir, filename))

    # could not find `filename`; return None
    return None


def which_missing_packages(pkgs):
    """
    Return list of packages (from list `pkgs`) that are not installed
    on the system.
    """
    missing = []
    for pkg in pkgs:
        if not have_sw_package(pkg):
            missing.append(pkg)
    return missing


def yes_or_no(value):
    return ('yes' if value else 'no')


if __name__ == '__main__':
    main()
