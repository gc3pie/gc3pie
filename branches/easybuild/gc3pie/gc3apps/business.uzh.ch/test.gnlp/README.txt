.. Hey Emacs, this is -*- rst -*-

   This file follows reStructuredText markup syntax; see
   http://docutils.sf.net/rst.html for more information.

.. include:: ../global.inc

.. _gnlp:

The ``input/`` directory contains the reference data:
``input.xml``

-----------------------
Execution requirements
-----------------------

A valid input file will have the following XML structure:
<?xml version="1.0"?>
<ROWSET>
<ROW>
  <PostId>
  <Content>
</ROW>
</ROWSET>

The CoreNLP program will apply Natural Language Processing to classify
the Sentiment of the <Comment> field.

_Note_: On the Hobbes could infrastructure, a dedicated Appliance has
been created ``coreNLP-2014-06-16``.
Openstack image id: ``e3d5ddea-3540-4fc8-91c1-77e6102a9534``

:command:`gnlp` can run on relatively small flavors; so far it has been
tested with 1core_4ram_5disk.

Testing :command:`gnlp`
=======================

To launch a :command:`gnlp` use the following syntax::

    $ python gnlp.py input/input.xml -k [chunk_size]

The :command:`gnlp` takes as input argument an xml file.

Optionally, it is possible to specify what chunk_size the input file
should be chunked.

:command:`gnlp` chunkes the input file using ``chunk_size`` (default: 1000).
``chunk_size`` is applied to the number of <ROW> elements found in the
input .xml file. So ``chunk_size``= 1000 means that each chunked file
will contain 1000 <ROW> elements.

For each chunked input file, :command:`gnlp` runs the Stanford CoreNLP
suite.

Invocation of :command:`gnlp` follows the usual session-based script
conventions::

    $ python ../gnlp.py -s <TEST_SESSION_NAME> -C 120 -vvv
    ./input/input.xml -o ./results -k 2

When all the jobs are done, the _results_ directory will contain
the merged result file.

Using :command:`gnlp` with :command:`gsession`
==============================================

Each :command:`gnlp` execution is called a `session`. 
When launching :command:`gnlp` it is possible to specify session name using the
`-s` flag. For example::

    $ python gnlp.py -s mysession

:command:`gnlp` controls the normal execution flow of a given session: Creation
of collection of executing units (``Tasks``), execution of the whole
collection, monitor the execution progress of each ``Task`` within a
collection, retrieve results of a finished ``Task`` within a colletion, if
needed terminate the resources that have been used by a given
collection (e.g. in cloud environment terminate the VM associated to a
given session).

:command:``gsession`` provides fine grained control over a running session.
For detailed information on :command:`gsession` please check the GC3Pie
documentation at http://gc3pie.readthedocs.org/en/latest/users/gc3utils.html?highlight=gsession#gsession-manage-sessions.



