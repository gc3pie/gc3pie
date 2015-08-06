.. Hey Emacs, this is -*- rst -*-

   This file follows reStructuredText markup syntax; see
   http://docutils.sf.net/rst.html for more information.

.. include:: global.inc


.. _glossary:

Glossary
========

.. glossary::

  API
    Acronym of *Application Programming Interface.* An API is a
    description of the way one piece of software asks another program
    to perform a service (quoted from:
    http://www.computerworld.com/s/article/43487/Application_Programming_Interface
    which see for a more detailed explanation).

  Command-line
    The sequence of words typed at the terminal prompt in
    order to run a specified application.

  Command-line option
    Arguments to a command (i.e., words on the command line) that
    select variants to the usual behavior of the command.  For
    instance, a command-line option can request more verbose reporting.

    Traditionally, UNIX command-line options consist of a dash
    (``-``), followed by one or more lowercase letters, or a
    double-dash (``--``) followed by a complete word or compound word.

    For example, the words ``-h`` or ``--help`` usually instruct a
    command to print a short usage message and exit immediately after.

  Core
    A single computing unit. This was called a *CPU* until manufacturers
    started packing many processing units into a single package: now the
    term CPU is used for the package, and *core* is one of the several
    independent processing units within the package.

  CPU Time
    The total time that computing units (processor `core`:term:) are
    actively executing a `job`:term:.  For single-threaded jobs, this
    is normally *less* then the actual duration ('wall-clock time' or
    `walltime`:term:), because some time is lost in I/O and system
    operations.  For parallel jobs the CPU time is normally larger
    than the duration, because several processor cores are active on
    the job at the same time; the quotient of the CPU time and the
    duration measures the efficiency of the parallel job.

  Job
    A computational job is a single run of a non-interactive
    application.  The prototypical example is a run of GAMESS_ on a
    single input file.

  Persistent
    Used in the sense of *preserved across program stops and system
    reboots*.  In practice, it just means that the relevant data is
    stored on disk or in some database.

  Resource
    Short for *computational resource*: any cluster or Grid where a job
    can run.

  State
    A one-word indication of a computational `job`:term: execution
    status (e.g., ``RUNNING`` or ``TERMINATED``).  The terms *state*
    and *status* are used interchangeably in GC3Pie_ documentation.

  STDERR 
    Abbreviation for "standard error stream"; it is the sequence of
    all text messages that a command prints to inform the user of
    problems or to report on operations progress.  The Linux/UNIX
    system allows two separate output streams, one for output proper,
    named `STDOUT`:term:, and STDERR for "error messages".  It is
    entirely up to the command to tag a message as "standard output"
    or "standard error".

  STDOUT
    Abbreviation for "standard output stream".  It is the sequence of
    all characters that constitute the output of a command.  The Linux/UNIX
    system allows two separate output streams, one for output proper, and
    one for "error messages", dubbed `STDERR`:term:.  It is entirely up 
    to the command to tag a message as "standard output" or "standard error".

  Session
    A :term:`persistent` collection of GC3Pie tasks and jobs. Sessions
    are used by `GC3Apps`:ref: to store job status across program runs.

  Walltime
    Short for *wall-clock time*: indicates the total running time of a
    :term:`job`.
  
