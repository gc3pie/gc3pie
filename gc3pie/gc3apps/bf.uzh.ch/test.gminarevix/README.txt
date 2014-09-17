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

Running ``gminarevix`` on login.gc3.uzh.ch
==========================================

* Log-in on the head node
    $ ssh <username>@login.gc3.uzh.ch

* Run gminarevix on all available data
    $ srun screen -L `which gminarevix` Wishart,SVJJ data/ -C 60 -o data.results

This will launch a ``screen`` session where gminarevix will run continuously.

## How to detach from a running ``screen`` session ?

Type Ctrl-A Ctrl-D to detach

## How to attach on a running ``screen`` session ?

At the prompt run
    $ screen -r

## How to terminate a running ``screen`` session

Once attached to the running ``screen`` session, type Ctrl-C

Note: this command will terminate the running session and will also
close the related execution of ``gminarevix``.
In order to restart the ``gminarevix`` execution you just need to
re-run the same ``srun`` command.




