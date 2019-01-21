========================================================================
    GC3Pie |gitter|
========================================================================

.. |gitter| image:: https://badges.gitter.im/gc3pie/chat.svg
   :alt: Join the chat at https://gitter.im/gc3pie/chat
   :target: https://gitter.im/gc3pie/chat?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

.. This file follows reStructuredText markup syntax; see
   http://docutils.sf.net/rst.html for more information

GC3Pie is a python package for executing computational workflows
consisting of tasks with complex inter-dependencies. GC3Pie accomplishes
this by defining a run-time task list in which a task can only be
executed once all upstream task dependencies have been successfullly
completed. In contrast to other workflow managers, GC3Pie accplications
are written in python and not a markup language. The advantage is that
this makes it trivial to write highly complex workflows. GC3Pies roots
are in shared-nothing achitectures (server-less for example) and can be
configured to use backends such as batch clusters or clouds.

GC3Pie is a suite of Python classes (and command-line tools built
upon them) to aid in submitting and controlling batch jobs to clusters
and grid resources seamlessly. GC3Pie aims at providing the
building blocks by which Python scripts that combine several
applications in a dynamic workflow can be quickly developed.

The GC3Pie suite is comprised of three main components:

 * GC3Libs: A python package for controlling the life-cycle of a Grid or batch computational job
 * GC3Utils: Command-line tools exposing the main functionality provided by GC3Libs
 * GC3Apps: Driver scripts to run large job campaigns


GC3Libs
=======

GC3Libs provides services for submitting computational jobs to Grids
and batch systems and controlling their execution, persisting job
information, and retrieving the final output.

GC3Libs takes an application-oriented approach to batch computing. A
generic ``Application`` class provides the basic operations for
controlling remote computations, but different ``Application``
subclasses can expose adapted interfaces, focusing on the most
relevant aspects of the application being represented. Specific
interfaces are already provided for the GAMESS_ and Rosetta_ suites;
new ones can be easily created by subclassing the generic
``Application`` class.


GC3Utils
========

Most of the time users have lots of different accounts on several
diverse resources. The idea underlying GC3Utils is that a user can
submit and control a computational job from one single place with a few
simple commands.

Commands are provided to submit a job (``gsub``), check its running
status (``gstat``), get a snapshot of the output files (``gget``,
``gtail``), or cancel it (``gkill``).


GC3Apps
=======

There is a need in some scientific communities, to run large job
campaigns to analyze a vast number of data files with the same
application. The single-job level of control implemented by GC3Utils
in this case is not enough: you would have to implement "glue scripts"
to control hundreds or thousand scripts at once. GC3Pie has provisons
for this, in the form of re-usable Python classes that implement a
single point of control for job families.

The GC3Apps scripts are driver scripts that run job campaigns using
the supported applications on a large set of input data. They can be
used in production as-is, or adapted to suit your data processing needs.

A Simple Example
================

There are several examples and a tutorial in the examples directory.

.. code-block:: python

    import sys
    import time
    import gc3libs

    class GdemoSimpleApp(gc3libs.Application):
        """
        This simple application will run `/bin/hostname`:file: on the remote host,
        and retrieve the output in a file named `stdout.txt`:file: into a
        directory `GdemoSimpleApp_output`:file: inside the current directory.
        """
        def __init__(self):
            gc3libs.Application.__init__(
                self,
                # the following arguments are mandatory:
                arguments = ["/bin/hostname"],
                inputs = [],
                outputs = [],
                output_dir = "./GdemoSimpleApp_output",
                # the rest is optional and has reasonable defaults:
                stdout = "stdout.txt",)

    # Create an instance of GdemoSimpleApp
    app = GdemoSimpleApp()

    # Create an instance of `Engine` using the configuration file present
    # in your home directory.
    engine = gc3libs.create_engine()

    # Add your application to the engine. This will NOT submit your
    # application yet, but will make the engine awere *aware* of the
    # application.
    engine.add(app)

    # in case you want to select a specific resource, call
    # `Engine.select_resource(<resource_name>)`
    if len(sys.argv)>1:
        engine.select_resource(sys.argv[1])

    # Periodically check the status of your application.
    while app.execution.state != gc3libs.Run.State.TERMINATED:
        print "Job in status %s " % app.execution.state
        # `Engine.progress()` will do the GC3Pie magic:
        # submit new jobs, update status of submitted jobs, get
        # results of terminating jobs etc...
        engine.progress()

        # Wait a few seconds...
        time.sleep(1)

    print "Job is now terminated."
    print "The output of the application is in `%s`." %  app.output_dir

This is what it looks like when the code is run:

.. code-block:: text

    $ python gdemo_simple.py localhost
    gdemo_simple.py: [2019-01-21 14:37:53] INFO    : Computational resource 'localhost' initialized successfully.
    Job in status NEW
    gdemo_simple.py: [2019-01-21 14:37:55] INFO    : Successfully submitted GdemoSimpleApp@7f07aa094a90 to: localhost
    Job in status SUBMITTED
    Job is now terminated.
    The output of the application is in `./GdemoSimpleApp_output`.

The output file looks as follows:

.. code-block:: text

    $ cat GdemoSimpleApp_output/stdout.txt
    93607d089233


Installation instructions and further reading
=============================================

For up-to-date information, please read the GC3Pie documentation at:
http://gc3pie.readthedocs.io/

Installation instructions are in the `INSTALL.rst`_ file (in this
same directory), or can be read online at:
http://gc3pie.readthedocs.io/en/latest/users/install.html

.. _`INSTALL.rst`: https://github.com/uzh/gc3pie/blob/master/docs/users/install.rst


.. References

.. _GC3Pie: http://gc3pie.googlecode.com/
.. _GAMESS: http://www.msg.chem.iastate.edu/gamess/
.. _Rosetta: http://www.rosettacommons.org/


.. (for Emacs only)
..
  Local variables:
  mode: rst
  End:
