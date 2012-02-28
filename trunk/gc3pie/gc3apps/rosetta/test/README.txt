The ``grosetta`` and ``gdocking`` programs run multiple invocations of the
``minirosetta`` and ``docking_protocol`` programs from the Rosetta suite.

Testing ``grosetta``
--------------------

Execution of ``grosetta`` is basically controlled by the "flags" file,
which must be the first argument.  A list of input files follows, then
(optionally) a colon and a list of output files.

The ``data/`` directory contains file needed for a sample run
(courtesy of Lars Malmstroem)::

    grosetta -s TEST_SESSION_NAME -C 120 --total-decoys 5 --decoys-per-job 2 data/flags data/alignment.filt data/boinc_aaquery0* data/query.* data/*.pdb

If everything is OK, the invocation should end with::

    Status of jobs in the 'XXX' session: (at 10:53:46, 02/28/12)
            NEW   0/3    (0.0%)  
        RUNNING   0/3    (0.0%)  
        STOPPED   0/3    (0.0%)  
      SUBMITTED   3/3   (100.0%) 
     TERMINATED   0/3    (0.0%)  
    TERMINATING   0/3    (0.0%)  
          total   3/3   (100.0%) 

On successful completion (takes a few minutes of run time), the
command prints::

    Status of jobs in the 'XXX' session: (at 11:05:50, 02/28/12)
            NEW   0/3    (0.0%)  
        RUNNING   0/3    (0.0%)  
        STOPPED   0/3    (0.0%)  
      SUBMITTED   0/3    (0.0%)  
     TERMINATED   3/3   (100.0%) 
    TERMINATING   0/3    (0.0%)  
             ok   3/3   (100.0%) 
          total   3/3   (100.0%) 


The three jobs should be named ``0--1``, ``2--3`` and ``4--5``; each
of these jobs will create an output directory named after the job.
Upon successful completion, the output directory of each job contains: 

* A copy of the submitted ``*.pdb``
* Additional ``.pdb`` files named ``S_"almost random string".pdb``
* A file ``score.sc``.
* Files ``minirosetta.static.log``, ``minirosetta.static.stdout.txt``
  and ``minirosetta.static.stderr.txt``.

The ``minirosetta.static.log`` file contains the output log of the
``minirosetta`` execution.  For each of the ``S_*.pdb`` files above, a
line like the following should be present in the log file::

    protocols.jd2.JobDistributor: S_1CA7A_1_0001 reported success in 124 seconds

The ``minirosetta.static.stdout.txt`` contains a copy of the
``minirosetta`` output log, plus the output of the wrapper script,
which should terminate with::

    minirosetta.static: All done, exitcode: 0



Testing ``gdocking``
--------------------

Execution of ``gdocking`` requires (at least) one  ``.pdb`` file; any
one of the ``.pdb`` files contained in the ``data/`` directory will do::

    ../gdocking.py -s XXX -C45 -vvv --decoys-per-file 5 --decoys-per-job 2 data/1bjpA.pdb

The ``gdocking`` application should produce this xRSL (look for it
into the DEBUG-level logfile)::

    Application provided XRSL: & (executable="docking_protocol.sh") (gmlog=".arc")(arguments="--tar" "*.pdb *.sc *.fasc" "-in:file:s" "1bjpA.pdb" "-in:file:native" "1bjpA.pdb" "-out:file:o" "1bjpA" "-out:nstruct" "2")(executables="docking_protocol.sh")(join="no")(stdout="docking_protocol.stdout.txt")(stderr="docking_protocol.stderr.txt")(inputFiles=("docking_protocol.flags" "file:///home/rmurri/.gc3/docking_protocol.flags") ("1bjpA.pdb" "file:///home/rmurri/gc3/gc3pie.googlecode.com/gc3pie/gc3apps/rosetta/test/data/1bjpA.pdb") ("docking_protocol.sh" "file:///home/rmurri/gc3/gc3pie.googlecode.com/gc3pie/gc3libs/etc/rosetta.sh"))(outputFiles=("docking_protocol.tar.gz" "") ("docking_protocol.log" ""))(runTimeEnvironment="APPS/BIO/ROSETTA-3.1")(wallTime="8 hours")(memory="2000")(count="1")(jobname="1bjpA.5--5")(cache="yes")

As with all session-based scripts, ``gdocking`` starts with this report::

    Status of jobs in the 'XXX' session: (at 11:37:41, 02/28/12)
            NEW   0/3    (0.0%)  
        RUNNING   0/3    (0.0%)  
        STOPPED   0/3    (0.0%)  
      SUBMITTED   3/3   (100.0%) 
     TERMINATED   0/3    (0.0%)  
    TERMINATING   0/3    (0.0%)  
          total   3/3   (100.0%) 

All is done when the TERMINATED jobs count reaches 100%::

            NEW   0/3    (0.0%)  
        RUNNING   0/3    (0.0%)  
        STOPPED   0/3    (0.0%)  
      SUBMITTED   0/3    (0.0%)  
     TERMINATED   3/3   (100.0%) 
    TERMINATING   0/3    (0.0%)  
         failed   3/3   (100.0%) 
          total   3/3   (100.0%) 

Jobs are named after the input file, with a `.N--M` suffix added;
e.g., for the ``1bjpa.pdb`` input, three jobs ``1bjpa.1--2``,
``1bjpa.3--4`` and ``1bjpa.5--5`` are created.

Execution of ``gdocking`` yields the following output:

* For each ``.pdb`` input file, a ``.decoys.tar`` file (e.g., for
  ``1bjpa.pdb`` input, a ``1bjpa.decoys.tar`` output is produced),
  which contains the ``.pdb`` files of the decoys produced by
  ``gdocking``.
* For each successful job, a `.N--M` directory: e.g., for the
  ``1bjpa.1--2`` job, a ``1bjpa.1--2/`` directory is created, with the
  following content:

  - ``docking_protocol.log``: output of Rosetta's ``docking_protocol`` program;
  - ``docking_protocol.stderr.txt``, ``docking_protocol.stdout.txt``: obvoius meaning.  The "stdout" file contains a copy of the ``docking_protocol.log`` contents, plus the output from the wrapper script.
  - ``docking_protocol.tar.gz``: the ``.pdb`` decoy files produced by the job.


  
