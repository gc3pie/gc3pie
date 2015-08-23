.. Hey Emacs, this is -*- rst -*-

   This file follows reStructuredText markup syntax; see
   http://docutils.sf.net/rst.html for more information.

.. include:: ../global.inc


.. _troubleshooting:

Troubleshooting GC3Pie
======================

This page lists a number of errors and issues that you might run into,
together with their solution.  Please use the `GC3Pie mailing list`_
for further help and for any problem not reported here!

Each section covers a different Python error; the section is named
after the error name appearing in the *last line* of the Python
traceback.  (See section `What is a Python traceback?`_ below)

.. contents::


What is a Python traceback?
---------------------------

A *traceback* is a long Python error message, detailing the call stack
in the code that lead to a specific error condition.

Tracebacks always look like this one (the number of lines printed, the
files involved and the actual error message will, of course, vary)::

    Traceback (most recent call last):
     File "/home/mpackard/gc3pie/bin/gsub", line 9, in <module>
       load_entry_point('gc3pie==1.0rc7', 'console_scripts', 'gsub')()
     File "/home/mpackard/gc3pie/lib/python2.5/site-packages/gc3pie-1.0rc7-py2.5.egg/gc3utils/frontend.py", line 137, in main
       import gc3utils.commands
     File "/home/mpackard/gc3pie/lib/python2.5/site-packages/gc3pie-1.0rc7-py2.5.egg/gc3utils/commands.py", line 31, in <module>
       import cli.app
     File "/home/mpackard/gc3pie/lib/python2.5/site-packages/pyCLI-2.0.2-py2.5.egg/cli/app.py", line 37, in <module>
       from cli.util import ifelse, ismethodof
     File "/home/mpackard/gc3pie/lib/python2.5/site-packages/pyCLI-2.0.2-py2.5.egg/cli/util.py", line 28, in <module>
       BaseStringIO = StringIO.StringIO
    AttributeError: 'module' object has no attribute 'StringIO'

Let's analyize how a traceback is formed, top to bottom.

A traceback is *always* started by the line::

    Traceback (most recent call last):

Then follow a number of line pairs like this one::

    File "/home/mpackard/gc3pie/lib/python2.5/site-packages/gc3pie-1.0rc7-py2.5.egg/gc3utils/frontend.py", line 137, in main
      import gc3utils.commands

The first line shows the file name and the line number where the
program stopped; the second line displays the instruction that Python
was executing when the error occurred.  *We shall always omit this
part of the traceback in the listings below.*

Finally, the traceback ends with the error message on the *last* line::

    AttributeError: 'module' object has no attribute 'StringIO'

Just look up this error message in the section headers below; if you
cannot find any relevant section, please write to the `GC3Pie mailing
list`_ for help.


Common errors using GC3Pie
--------------------------

This section section lists Python errors that may happen when using
GC3Pie; each section is named after the error name appearing in the
*last line* of the Python traceback.  (See section `What is a Python
traceback?`_ above.)

If you get an error that is not listed here, please get in touch via
the `GC3Pie mailing list`_.


AttributeError: `module` object has no attribute `StringIO`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This error::

    Traceback (most recent call last):
     ...
     File "/home/mpackard/gc3pie/lib/python2.5/site-packages/pyCLI-2.0.2-py2.5.egg/cli/util.py",
    line 28, in <module>
       BaseStringIO = StringIO.StringIO
    AttributeError: 'module' object has no attribute 'StringIO'

is due to a conflicts of the `pyCLI library <pycli>`_ (prior to version 2.0.3) and the
`Debian/Ubuntu package *python-stats* <python-stats>`_

.. _pycli: http://pypi.python.org/pypi/pyCLI
.. _python-stats: http://packages.debian.org/squeeze/python-stats

There are three ways to get rid of the error:

1. Uninstall the `*python-stats* package <python-stats>` (run the command ``apt-get remove python-stats`` as user ``root``)
2. Upgrade `pyCLI`_ to version 2.0.3 at least.
3. `Upgrade`:ref: GC3Pie, which will force an upgrade of pyCLI.


DistributionNotFound
~~~~~~~~~~~~~~~~~~~~

If you get this error::

    Traceback (most recent call last):
        ...
    pkg_resources.DistributionNotFound: gc3pie==1.0rc2

It usually means that you didn't run ``source ../bin/activate;
./setup.py develop`` when upgrading GC3Pie.

Please re-do the steps in the `GC3Pie Upgrade instructions
<upgrade>`:ref: to fix the error.


ImportError: No module named ``pstats``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This error only occurs on Debian and Ubuntu GNU/Linux::

    Traceback (most recent call last):
    File ".../pyCLI-2.0.2-py2.6.egg/cli/util.py", line 19, in <module>
       import pstats
    ImportError: No module named pstats

To solve the issue: install the `*python-profiler* package <python-profiler>`::

  apt-get install python-profiler # as `root` user

.. _python-profiler: http://packages.debian.org/squeeze/python-profiler


NoResources: Could not initialize any computational resource - please check log and configuration file.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This error::

    Traceback (most recent call last):
      ...
      File ".../src/gc3libs/core.py", line 150, in submit
        raise gc3libs.exceptions.NoResources("Could not initialize any computational resource"
    gc3libs.exceptions.NoResources: Could not initialize any computational resource - please check log and configuration file.

can have two different causes:

1. You didn't create a configuration file, or you did not list any resource in it.
2. Some other error prevented the resources from being initialized, or the configuration file from being properly read.


ValueError: I/O operation on closed file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sample error traceback (may be repeated multiple times over)::

    Traceback (most recent call last):
      File "/usr/lib/python2.5/logging/__init__.py", line 750, in emit
        self.stream.write(fs % msg)
    ValueError: I/O operation on closed file


This is discussed in `Issue 182`_; a fix have been committed to
release 1.0, so if you are seeing this error, you are running a
pre-release version of GC3Pie and should `upgrade`:ref:.

.. _`Issue 182`: https://github.com/uzh/gc3pie/issues/182
