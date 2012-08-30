Sample data for testing GAMESS is in the ``data/`` directory.

To execute tests, just call ``ggamess`` with one or more input files
from the ``data/`` directory::

    ggamess.py -s XXX -C45 -vvv data/exam01.inp

If the log level is DEBUG, you should see that the produced xRSL looks
like the following:

    Application provided XRSL: & (executable="/$GAMESS_LOCATION/nggms") (gmlog=".arc")(arguments="exam01.inp")(join="yes")(stdout="exam01.out")(inputFiles=("exam01.inp" "file:///home/rmurri/gc3/gc3pie.googlecode.com/gc3pie/gc3apps/gamess/test/data/exam01.inp"))(outputFiles=("exam01.dat" ""))(runTimeEnvironment="APPS/CHEM/GAMESS-2010")(wallTime="8 hours")(memory="2000")(count="1")(jobname="exam01")(cache="yes")

Like all session-based scripts, ``ggamess`` starts with a status
report like this one::

    Status of jobs in the 'XXX' session: (at 13:08:08, 02/28/12)
            NEW   0/1    (0.0%)  
        RUNNING   0/1    (0.0%)  
        STOPPED   0/1    (0.0%)  
      SUBMITTED   1/1   (100.0%) 
     TERMINATED   0/1    (0.0%)  
    TERMINATING   0/1    (0.0%)  
          total   1/1   (100.0%) 

The number of jobs should be equal to the number of input files.
 
The processing ends when TERMINATED has reached 100% jobs:

    Status of jobs in the 'XXX' session: (at 13:13:00, 02/28/12)
            NEW   0/1    (0.0%)  
        RUNNING   0/1    (0.0%)  
        STOPPED   0/1    (0.0%)  
      SUBMITTED   0/1    (0.0%)  
     TERMINATED   1/1   (100.0%) 
    TERMINATING   0/1    (0.0%)  
             ok   1/1   (100.0%) 
          total   1/1   (100.0%) 


For each ``exam*.inp`` file, an output directory ``exam*/`` will be
created.  Each directory contains the following files:

* ``exam*.out``: GAMESS execution log file.
* ``exam*.dat``: Corresponding ``.dat`` file, providing some
  information in machine-parsable format.

Example::

    $ ls exam01/ -l
    totale 72
    -rw-r--r-- 1 rmurri rmurri 14141 2012-02-28 13:12 exam01.dat
    -rw-r--r-- 1 rmurri rmurri 57053 2012-02-28 13:12 exam01.out

The ``GamessApplication`` object already performs some correctness
testing on the execution log, so if any job has failed, this fact
should be reflected in the ok/failed job count.  As a rule of thumb,
an execution terminated correctly if the GAMESS log file contains::

    EXECUTION OF GAMESS TERMINATED NORMALLY Tue Feb 28 13:08:25 2012
