.. Hey Emacs, this is -*- rst -*-

   This file follows reStructuredText markup syntax; see
   http://docutils.sf.net/rst.html for more information.

.. include:: ../../global.inc



================================
  GC3Pie programming tutorials
================================

.. contents::


Implementing scientific workflows with GC3Pie
---------------------------------------------

This is the course material prepared for the `"GC3Pie for Programmers"
training`__, held at the University of Zurich for the first time on July
11-14, 2016. (The slides presented here are revised at each course re-run.)

.. __: http://www.s3it.uzh.ch/en/scienceit/support/training/gc3pie/programmers.html

The course aims at showing how to implement patterns commonly seen
in scientific computational workflows using Python and GC3Pie, and
provide users with enough knowledge of the tools available in GC3Pie
to extend and adapt the examples provided.

`Introduction to the training`__

  A presentation of the training material and outline of the course.
  Probably not much useful unless you're actually sitting in class.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/workflows/part00.pdf

`Overview of GC3Pie use cases`__

  A quick overview of the kind of computational use cases that GC3Pie
  can easily solve.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/workflows/part01.pdf

`GC3Pie basics`__

  The basics needed to write simple GC3Pie scripts: the minimal
  session-based script scaffolding, and the properties and features of
  the `Application`:class: object.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/workflows/part02.pdf

`Useful debugging commands`__

  Recall a few GC3Pie utilities that are especially useful when
  debugging code.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/workflows/part03.pdf

`Customizing command-line processing`__

  How to set up command-line argument and option processing in
  GC3Pie's `SessionBasedScript`:class:

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/workflows/part04.pdf

`Application requirements`__

  How to specify running requirements for `Application`:class: tasks,
  e.g., how much memory is needed to run.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/workflows/part05.pdf

`Application control and post-processing`__

  How to check and react on the termination status of a GC3Pie
  Task/Application.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/workflows/part06.pdf

`Introduction to workflows`__

  A worked-out example of a many-step workflow.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/workflows/part07.pdf

`Running tasks in a sequence`__

  How to run tasks in sequence: basic usage of
  `SequentialTaskCollection`:class: and `StagedTaskCollection`:class:

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/workflows/part08.pdf

`Running tasks in parallel`__

  How to run independent tasks in parallel: the `ParallelTaskCollection`:class:

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/workflows/part09.pdf

`Automated construction of task dependency graphs`__

  How to use the `DependentTaskCollection`:class: for automated
  arrangement of tasks given their dependencies.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/workflows/part10.pdf

`Dynamic and Unbounded Sequences of Tasks`__

  How to construct `SequentialTaskCollection`:class: classes that
  change the sequence of tasks while being run.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/workflows/part11.pdf


A bottom-up introduction to programming with GC3Pie
---------------------------------------------------

This is the course material made for the `GC3Pie 2012 Training event`_
held at the University of Zurich on October 1-2, 2012.

The presentation starts with low-level concepts (e.g., the
`Application`:class: and how to do manual task submission) and then
gradually introduces more sophisticated tools (e.g., the
`SessionBasedScript`:class: and workflows).

This order of introducing concepts will likely appeal most to those
already familiar with batch-computing and grid computing, as it
provides an immediate map of the job submission and monitoring
commands to GC3Pie equivalents.

`Introduction to GC3Pie`__

    Introduction to the software: what is GC3Pie, what is it for, and
    an overview of its features for writing high-throughput computing
    scripts.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/bottom-up/part01.pdf

`Basic GC3Pie programming`__

    The `Application` class, the smallest building block of
    GC3Pie. Introduction to the concept of Job, states of an
    application and to the `Core` class.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/bottom-up/part03.pdf

`Application requirements`__

    How to define extra requirements for an application, such as the
    minimum amount of memory it will use, the number of cores needed
    or the architecture of the CPUs.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/bottom-up/part04.pdf

`Managing applications: the SessionBasedScript class`__

    Introduction to the highest-level interface to build applications
    with GC3Pie, the `SessionBasedScript`. Information on how to
    create simple scripts that take care of the execution of your
    applications, from submission to getting back the final results.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/bottom-up/part05.pdf

`The GC3Utils commands`__

    Low-level tools to aid debugging the scripts.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/bottom-up/part06.pdf

`Introduction to Workflows with GC3Pie`__

    Using a practical example (the :ref:`warholize tutorial`) we show
    how workflows are implemented with GC3Pie. The following slides
    will cover in more details the single steps needed to produce a
    complex workflow.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/bottom-up/part08.pdf

`ParallelTaskCollection`__

    Description of the `ParallelTaskCollection` class, used to run
    tasks in parallel.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/bottom-up/part09.pdf

`StagedTaskCollection`__

    Description of the `StagedTaskCollection` class, used to run a
    sequence of a fixed number of jobs.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/bottom-up/part10.pdf

`SequentialTaskCollection`__

    Description of the `SequentialTaskCollection` class, used to run a
    sequence of jobs that can be altered during runtime.

.. __: https://github.com/uzh/gc3pie/tree/master/docs/programmers/tutorials/bottom-up/part11.pdf


.. _`Warholize Tutorial`:

The "Warholize" Workflow Tutorial
---------------------------------

In this tutorial we show how to use the GC3Pie libraries in order
to build a command line script which runs a complex workflow with
both parallelly- and sequentially-executing tasks.

The tutorial itself contains the complete source code of the
application (see `Literate Programming`_ on Wikipedia), so that you
will be able to test/modify it and produce a working ``warholize.py``
script by downloading the ``pylit.py``:file: script from the `PyLit
Homepage`_ and running the following command on the
``docs/programmers/tutorials/warholize/warholize.rst`` file,
from within the source tree of GC3Pie::

  $ ./pylit warholize.rst warholize.py


.. toctree::

  warholize/warholize


Example scripts
---------------

A collection of small example scripts highlighting different features
of GC3Pie is available in the source distribution, in folder
``examples/``:file:

`gdemo_simple.py`_

    Simplest script you can create. It only uses `Application` and
    `Engine` classes to create an application, submit it, check its
    status and retrieve its output.

`grun.py`_

    a `SessionBasedScript` that executes its argument as command. It
    can also run it multiple times by wrapping it in a
    ParallelTaskCollection or a SequentialTaskCollection, depending on
    a command line option. Useful for testing a configured resource.

`gdemo_session.py`_

    a simple `SessionBasedScript` that sums two values by customizing
    a `SequentialTaskCollection`.

`warholize.py`_

    an enhanced version of the `warholize` script proposed in the
    :ref:`Warholize Tutorial`

.. _`gdemo_simple.py`: https://github.com/uzh/gc3pie/tree/master/examples/gdemo_simple.py
.. _`gdemo_session.py`: https://github.com/uzh/gc3pie/tree/master/examples/gdemo_session.py
.. _`grun.py`: https://github.com/uzh/gc3pie/tree/master/examples/grun.py
.. _`warholize.py`: https://github.com/uzh/gc3pie/tree/master/examples/warholize.py



.. References:

.. _`GC3Pie 2012 Training event`: https://www.gc3.uzh.ch/edu/gc3pie2012/
.. _`Literate Programming`: http://en.wikipedia.org/wiki/Literate_programming
.. _`PyLit Homepage`: https://github.com/gmilde/PyLit
