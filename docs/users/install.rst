.. Hey Emacs, this is -*- rst -*-

   This file follows reStructuredText markup syntax; see
   http://docutils.sf.net/rst.html for more information.

.. include:: ../global.inc


------------------------
 Installation of GC3Pie
------------------------


Quick start
===========

We provide an installation script which automatically tries to install
GC3pie in your home directory.  The quick installation procedure has
only been tested on variants of the GNU/Linux operating system;
however, the script should work on MacOSX as well, provided you follow
the preparation steps outlined in the "MacOSX installation" section
below.

To install GC3Pie: (1) download the installation script into a file
`install.py`_, then (2) type this at your terminal prompt::

    python install.py

The above command creates a directory `$HOME/gc3pie`:file: and installs
the latest release of GC3Pie and all its dependencies into it.

.. _`install.py`: https://raw.githubusercontent.com/uzh/gc3pie/master/scripts/install.py

Alternatively, you can also perform both steps at the terminal prompt::

    # use this if the `wget` command is installed
    wget -O install.py https://raw.githubusercontent.com/uzh/gc3pie/master/scripts/install.py
    python install.py

    # use this if the `curl` command is installed instead
    curl -O https://raw.githubusercontent.com/uzh/gc3pie/master/scripts/install.py
    python install.py

Choose either one of the two methods above, depending on whether
``wget`` or ``curl`` is installed on your system (Linux systems
normally have ``wget``; MacOSX normally uses ``curl``).

In case you have trouble running the installation script, please send
an email to `gc3pie@googlegroups.com
<mailto:gc3pie@googlegroups.com>`_ or post a message on the web forum
`<https://groups.google.com/forum/#!forum/gc3pie>`_.  Include the full
output of the script in your email, in order to help us to identify
the problem.

Now you can check your GC3Pie installation; follow the on-screen
instructions to activate the virtual environment.  Then, just type the
command::

  gc3utils --help

and you should see the following output appear on your screen::

  Usage: gc3utils COMMAND [options]

  Command `gc3utils` is a unified front-end to computing resources.
  You can get more help on a specific sub-command by typing::
    gc3utils COMMAND --help
  where command is one of these:
    clean
    cloud
    get
    info
    kill
    resub
    select
    servers
    stat
    tail

If you get some errors, do not despair!  The `GC3Pie users
mailing-list <gc3pie@googlegroups.com>`_ is there to help you :-)
(You can also post to the same forum using a web interface at
`<https://groups.google.com/forum/#!forum/gc3pie>`_.)

With the default configuration file, GC3Pie is set up to only run
jobs on the computer where it is installed.  To run jobs on remote
resources, you need to edit the configuration file; `the
Configuration file documentation
<http://gc3pie.readthedocs.io/en/latest/users/configuration.html>`_
provides an explanation of the syntax.


Non-standard installation options
=================================

The installation script accept a few options that select alternatives
to the standard behavior.  In order to use these options, you have to:

1. download the installation script into a file named `install.py`:file:::

        wget https://raw.githubusercontent.com/uzh/gc3pie/master/scripts/install.py

2. run the command::

        python install.py [options]

   replacing the string ``[options]`` with the actual options you want
   to pass to the script.  Also, the ``python`` command should be the
   Python executable that you want to use to run GC3Pie applications.

The accepted options are as follows:

  ``--feature LIST``

      Install optional features (comma-separated list).  Currently
      defined features are:

      * ``openstack``: support running jobs in VMs on OpenStack clouds
      * ``ec2``:       support running jobs in VMs on OpenStack clouds
      * ``optimizer``: install math libraries needed by the optimizer library

      For instance, to install all features use ``-a openstack,ec2,optimizer``.
      To install no optional feature, use ``-a none``.

      By default, all cloud-related features are installed.

  ``-d DIRECTORY``

      Install GC3Pie in location ``DIRECTORY`` instead of
      ``$HOME/gc3pie``

  ``--overwrite``

      Overwrite the destination directory if it already
      exists. Default behavior is to abort installation.

  ``--develop``

      Instead of installing the latest *release* of GC3Pie, it will
      install the *master branch* from the GitHub repository.

  ``--yes``

      Run non-interactively, and assume a "yes" reply to every
      question.

  ``--no-gc3apps``

       Do not install any of the GC3Apps, e.g., ``gcodeml``,
       ``grosetta`` and ``ggamess``.


Manual installation
===================

In case you can't or don't want to use the automatic installation
script, the following instructions will guide you through all the
steps needed to manually install GC3Pie on your computer.

These instructions show how to install GC3Pie from the GC3 source
repository into a separate python environment (called `virtualenv`_).
Installation into a virtualenv has two distinct advantages:

  * All code is confined in a single directory,
    and can thus be easily replaced/removed.

  * Better dependency handling: additional Python packages
    that GC3Pie depends upon can be installed even if they
    conflict with system-level packages.

0. Install software prerequisites:

   * On Debian/Ubuntu, install these system packages::

       apt-get install gcc g++ git python-dev libffi-dev libssl-dev

   * On CentOS5, install these packages::

       yum install git python-devel gcc gcc-c++ libffi-devel openssl-devel

   * On other Linux distributions, you will need to install:

     - the ``git`` command (from the Git_ VCS);
     - Python development headers and libraries;
       (for installing extension libraries written in C/C++)
     - a C/C++ compiler (this is usually installed by default);
     - include files for the FFI and OpenSSL libraries.


1. If `virtualenv`_ is not already installed on your system,
   get the Python package and install it::

      wget http://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.7.tar.gz
      tar -xzf virtualenv-1.7.tar.gz && rm virtualenv-1.7.tar.gz
      cd virtualenv-1.7/

   If you are installing as `root`, the following command is all you
   need::

      python setup.py install

   If instead you are installing as a normal, unprivileged user,
   things get more complicated::

      export PYTHONPATH=$HOME/lib64/python:$HOME/lib/python:$PYTHONPATH
      export PATH=$PATH:$HOME/bin
      mkdir -p $HOME/lib/python
      python setup.py install --home $HOME

   You will also *have to* add the two `export` lines above to the:

   * `$HOME/.bashrc` file, if using the `bash` shell or to the
   * `$HOME/.cshrc` file, if using the `tcsh` shell.

   In any case, once `virtualenv` has been installed, you can exit
   its directory and remove it::

      cd ..
      rm -rf virtualenv-1.7


2. Create a virtualenv to host the GC3Pie installation, and ``cd``
   into it::

       virtualenv --system-site-packages $HOME/gc3pie
       cd $HOME/gc3pie/
       source bin/activate

   In this step and in the following ones, the directory
   ``$HOME/gc3pie`` is going to be the installation folder of GC3Pie.
   You can change this to another directory path; any directory that's
   writable by your Linux account will be OK.

   If you are installing system-wide as ``root``, we suggest you
   install GC3Pie into ``/opt/gc3pie`` instead.


3. Check-out the ``gc3pie`` files in a ``src/`` directory::

       git clone https://github.com/uzh/gc3pie.git src


4. Install the ``gc3pie`` in "develop" mode, so any modification
   pulled from GitHub is immediately reflected in the running environment::

       cd src/
       env CC=gcc ./setup.py develop
       cd .. # back into the `gc3pie` directory

   This will place all the GC3Pie command into the ``gc3pie/bin/``
   directory.


5. GC3Pie comes with driver scripts to run and manage large families
   of jobs from a few selected applications.  These scripts are not
   installed by default because not everyone needs them.

   Run the following commands to install the driver scripts for the
   applications you need::

     # if you are insterested in GAMESS, do the following
     ln -s '../src/gc3apps/gamess/ggamess.py' bin/ggamess

     # if you are insterested in Rosetta, do the following
     ln -s '../src/gc3apps/rosetta/gdocking.py' bin/gdocking
     ln -s '../src/gc3apps/rosetta/grosetta.py' bin/grosetta

     # if you are insterested in Codeml, do the following
     ln -s '../src/gc3apps/codeml/gcodeml.py' bin/gcodeml


6. Now you can check your GC3Pie installation; just type the command::

     gc3utils --help

   and you should see the following output appear on your screen::

     Usage: gc3utils COMMAND [options]

     Command `gc3utils` is a unified front-end to computing resources.
     You can get more help on a specific sub-command by typing::
       gc3utils COMMAND --help
     where command is one of these:
       clean
       cloud
       get
       info
       kill
       resub
       select
       servers
       stat
       tail

   If you get some errors, do not despair!  The `GC3Pie users
   mailing-list <gc3pie@googlegroups.com>` is there to help you :-)
   (You can also post to the same forum using the web interface at
   `<https://groups.google.com/forum/#!forum/gc3pie>`_.)

7. With the default configuration file, GC3Pie is set up to only run
   jobs on the computer where it is installed.  To run jobs on remote
   resources, you need to edit the configuration file; `the
   configuration file documentation
   <http://gc3pie.readthedocs.io/en/latest/users/configuration.html>`_
   provides an explanation of the syntax.


.. _upgrade:

Upgrade
=======

If you used the installation script, the fastest way to upgrade is just to reinstall:

0. De-activate the current GC3Pie virtual environment::

     deactivate

   (If you get an error "command not found", do not worry and proceed
   on to the next step; in case of other errors please stop here and
   report to the `GC3Pie users mailing-list
   <mailto:gc3pie.googlegroups.com>`.)

1. Move the `$HOME/gc3pie`:file: directory to another location, e.g.::

     mv $HOME/gc3pie $HOME/gc3pie.OLD

2. Reinstall GC3Pie using the quick-install script (top of this page).

3. Once you have verified that your new installation is working, you
   can remove the `$HOME/gc3pie.OLD`:file: directory.

If instead you installed GC3Pie using the "manual installation" instructions,
then the following steps will update GC3Pie to the latest version
in the code repository:

1. `cd`:command: to the directory containing the GC3Pie virtualenv;
   assuming it is named ``gc3pie`` as in the above installation
   instructions, you can issue the commands::

     cd $HOME/gc3pie # use '/opt/gc3pie' if root

2. Activate the virtualenv::

     source bin/activate

3. Upgrade the `gc3pie` source and run the `setup.py`:file: script again::

     cd src
     svn up
     env CC=gcc ./setup.py develop

*Note:* A major restructuring of the SVN repository took place in
r1124 to r1126 (Feb. 15, 2011); if your sources are older than SVN
r1124, these upgrade instructions will not work, and you must
*reinstall completely*.  You can check what version the SVN sources
are, by running the `svn info` command in the `src` directory: watch
out for the `Revision:` line.


MacOSX Installation
===================

Installation on MacOSX machines is possible, however there are still a
few issues.  If you need MacOSX support, please let us know on the
`GC3Pie users mailing-list <mailto:gc3pie@googlegroups.com>` or by
posting a message using the web interface at
`<https://groups.google.com/forum/#!forum/gc3pie>`_.

1) Standard usage of the installation script (i.e., with no options)
   works, but you have to use `curl` since `wget` is not installed by
   default.

2) In order to install GC3Pie you will need to install `XCode`_ and,
   in some of the MacOSX versions, also the *Command Line Tools for
   XCode*

3) Options can only be given in the abbreviated one-letter form (e.g.,
   ``-d``); the long form (e.g., ``--directory``) will not work.

4) The `shellcmd` backend of GC3Pie depends on the GNU ``time``
   command, which is not installed on MacOSX by default. This means
   that with a standard MacOSX installation the `shellcmd` resource
   will **not** work. However:

   * other resources, like `pbs` via `ssh` transport, will work.

   * you can install the GNU time command either via `MacPorts`_,
     `Fink`_, `Homebrew`_ or from `this url`_. After installing it
     you don't need to update your ``PATH`` environment variable, it's
     enough to set the ``time_cmd`` option in your GC3Pie
     configuration file.


HTML Documentation
==================

HTML documentation for the GC3Libs programming interface can be
read online at:

  http://gc3pie.readthedocs.io/

If you installed GC3Pie manually, or if you installed it using the
``install.sh`` script with the ``--develop`` option, you can also
access a local copy of the documentation from the sources::

  cd $HOME/gc3pie # or wherever the gc3pie virtualenv is installed
  cd src/docs
  make html

Note that you need the Python package `Sphinx`_
in order to build the documentation locally.


.. Local references

.. _sphinx: http://sphinx.pocoo.org/
.. _virtualenv: http://pypi.python.org/pypi/virtualenv/1.7

.. _`MacPorts`: http://www.macports.org/
.. _`Fink`: http://sourceforge.net/projects/fink/
.. _`Homebrew`: http://mxcl.github.com/homebrew/
.. _`this url`: http://mirror.switch.ch/ftp/mirror/gnu/time/
.. _`XCode`: https://developer.apple.com/xcode/
