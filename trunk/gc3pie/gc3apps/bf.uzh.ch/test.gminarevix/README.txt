The ``data/`` directory contains an example of structure data
to be used as input

.. Main application developers: "Pascal Marco Caversaccio
<pascalmarco.caversaccio@bf.uzh.ch>"


Testing ``gminarevix``
================================

The ``gminarevix`` application needs:

* a directory with the "structure_data" input files;
* a list of ``models`` to run. Each model correspond to a specific 
  parameter configuration the ``minarevix_cloud`` binary will use.
* Optionally, a specific version of the ``minarevix_cloud`` binary
  which is a compiled version of the Matlab script written by the
  author.

On the S3IT cloud, the application should be executed using the generic
Matlab MCR image (for more information check
http://www.s3it.uzh.ch/infrastructure/hobbes/appliances/)


Invocation of ``gminarevix`` follows the usual session-based script
conventions::

    python ../gminarevix.py Wishart data/ -b ./bin/minarevix_cloud -s
    <TEST_SESSION_NAME> -o ./results -C 120 -vvv 

When all the jobs are done, the _results_ directory will contains one
result folder for each combination of ``model`` and ``data``.

