The ``bin/`` directory contains the master driver script: ``run.R`` and
the weight function: ``f_get_weight.r``.

The ``data/`` directory contains the reference data:
``two_mode_network.rda`` and ``edgelist.one.mode.100.csv``

.. Main developers: "Radu Tanase" <radu.tanase@business.uzh.ch>


Execution requirements
======================

At the remote end, it is required to have R (r-base) package
and the R ``multicore`` library installed.

_Note_: On the Hobbes could infrastructure, a dedicated VM with R and
other component installed is available (image_id=_ami-00000084_).



Testing ``gweight``
===================

    gweight.py ./data/edgelist.one.mode.100.csv -k 25 -o out
    -M ./bin/run.R

The ``gweight`` takes as input argument a _csv_ file containing the
entries the ``weight`` function will be applied to.

Example:
$ head -n 3 ./data/edgelist.one.mode.100.csv
"id1" "id2"
"id1" "id3"
"id1" "id4"

The ``k`` value is used to chunck the input file into smaller chunks;
each of them will be the input file for a ``GWeightApplication``.

Each ``GWeightApplication`` applies runs the ``master`` R script
(``run.R`` in the example) and generates, as output file, a modified
list with ``weight`` values associated

Example:
id1 id1338 0.00284495021337127
id1 id1351 0.00213371266002845
id1 id1392 0.00142247510668563

The file is created in the Application's ``output`` folder

Once all chuncked files have been processed, the corresponding results
are merged into a single ``csv`` result file.

Invocation of ``gweight`` follows the usual session-based script
conventions::

    gweight.py -s TEST_SESSION_NAME -C 120 -vvv
    ./data/edgelist.one.mode.100.csv -o ./out -k 25

When all the jobs are done, the current directory will contain
the merged result file.

``out/.computation`` will contain the individual Application's
subfolders with logs information (``gweight.log``)

