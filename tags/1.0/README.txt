GC3Pie 1.0
==========

GC3Pie is an open source suite of Python classes (and command-line
tools built upon them) to aid in submitting and controlling batch jobs
to clusters and grid resources seamlessly.  GC3Pie provides the
building blocks for the development of Python scripts that combine
several applications into a dynamic workflow.

This document describes GC3Pie version 1.0, released May 25, 2011.

GC3Pie is free software, released under the GNU Lesser General Public
License version 3.


Overview of GC3Pie
------------------

The GC3Pie suite is comprised of three main components:

  * GC3Libs: A python package for controlling the life-cycle of a Grid
    or batch computational job

  * GC3Utils: Command-line tools exposing the basic functionality
    provided by GC3Libs 

  * GC3Apps: Driver scripts to run large job campaigns 

The analysis of very large datasets with various interdependent
applications is becoming a need for more and more scientific
communities. The single-job level of control exposed by many existing
computing infrastructures (being them grid or clouds) in this case is
often not enough: users have to implement "glue scripts" to control
hundreds or thousand jobs at once.

GC3Pie abstracts the generic code out of this picture, in the form of
re-usable Python classes that implement a single point of control for
application collections. GC3Pie provides a simple model for expressing
the way an application should behave (how it should be invoked, what
input data it requires, etc.).

Specific support is provided in the 1.0 release for the following
applications: 

  * GAMESS-US
  * Rosetta (`minirosetta` and `docking_protocol`)
  * codeml (part of the PAML suite, http://abacus.gene.ucl.ac.uk/software/paml.html)

New ones can be easily added by subclassing the generic
`gc3libs.Application` class.

More details can be found on the project's website:

  http://gc3pie.googlecode.com/


Installation and system requirements
------------------------------------

GC3Pie has been tested and is supported on the following GNU/Linux
distributions (both 32- and 64-bit x86 processors):

   * Ubuntu: 11.04, 10.10, 10.04 
   * Debian: 6.0 (squeeze), 5.0 (lenny)
   * CentOS: 5.5, 5.4, 5.3 

You can find source releases and installation instructions for the 1.0
release at:
  
  http://code.google.com/p/gc3pie/wiki/InstallGC3Pie


Upgrading from previous releases
--------------------------------

Those who are using pre-release version of GC3Pie are encouraged to
upgrade; upgrade instructions are available at:

  http://code.google.com/p/gc3pie/wiki/InstallGC3Pie#Upgrade

Here's a brief list of the main changes; please consult the full list
at http://code.google.com/p/gc3pie/wiki/News before upgrading.

Configuration file changes
~~~~~~~~~~~~~~~~~~~~~~~~~~
  * Renamed configuration file to gc3pie.conf: the file gc3utils.conf will no longer be read!
  * SGE clusters must now have type = sge in the configuration file (instead of type = ssh-sge)
  * All computational resource must have an architecture = ... line; see the ConfigurationFile wiki page for details

Command-line utilities changes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  * GC3Utils and GC3Apps (grosetta/ggamess/etc.) now all accept a -s/--session option for locating the job storage directory: this allows grouping jobs into folders instead of shoveling them all into ~/.gc3/jobs.
  * GC3Apps (grosetta/ggames/etc.): replaced option -t/--table with -l/--states. The new option prints a table of submitted jobs in addition to the summary stats; if a comma-separated list of job states follows the option, only job in those states are printed.
  * The command `gstat` will now print a summary of the job states if the list is too long to fit on screen; use the -v option to get the full job listing regardless of its length.


Support and Contact
-------------------

Please direct all support requests to the mailing-list first.  You can
subscribe, view recent posts and search the archives at:

  http://groups.google.com/group/gc3pie


Known issues, together with their likely causes and remedies, have
been listed at:

  http://code.google.com/p/gc3pie/wiki/TroubleShooting


Bug reports, feature and enhancement requests
can be reported at: 

  http://code.google.com/p/gc3pie/issues


Further information
-------------------

More details can be found on the project's website:

  http://gc3pie.googlecode.com/


The GC3Pie team:

    Riccardo Murri <riccardo.murri@uzh.ch>
    Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>
    Mike Packard <mike.packard@uzh.ch>


