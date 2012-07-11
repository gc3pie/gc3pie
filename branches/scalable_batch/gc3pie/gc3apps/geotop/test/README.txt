The ``data/`` directory contains sample data provided by Stephan
Gruber of the University of Zurich.


Testing ``ggeotop``
===================

The ``ggeotop`` application will scan its arguments for directories
where a ``geotop.inpts`` file is present.

Therefore, to run a test of ``ggeotop`` it is sufficient to provide
the path to a directory containing valid input files.  One such
directories are provided here, as directory ``data/public_test``.

Invocation of ``ggeotop`` follows the usual session-based script
conventions; in addition, the path to a statically-compiled GEOtop
binary must be provided as argument to option ``-x``::

    ggeotop -s TEST_SESSION_NAME -C 120 -x test/GEOtop_1_224_20120227_static -vvv data/public_test

The test data should generate only one GEOtop job::

    Status of jobs in the '...' session: (at 13:08:08, 02/28/12)
            NEW   0/1    (0.0%)  
        RUNNING   0/1    (0.0%)  
        STOPPED   0/1    (0.0%)  
      SUBMITTED   1/1   (100.0%) 
     TERMINATED   0/1    (0.0%)  
    TERMINATING   0/1    (0.0%)  
          total   1/1   (100.0%) 

The processing ends when TERMINATED has reached 100% jobs.

When all the jobs are done, each input *directory* will contain
the output files, in the ``out/`` subfolder.

You can check that the output is there by running the ``ggeotop_util``
script:: 

    ggeotop_util clean test/data/public_test --simulate
    Removing file test/data/GEOtop_public_test/input.tgz

.. note::

   When you want to run ``ggeotop`` again, you need to run the
   ``ggeotop_util clean test/data/public_test`` *without* the
   ``--simulate`` option, to remove the output files.
