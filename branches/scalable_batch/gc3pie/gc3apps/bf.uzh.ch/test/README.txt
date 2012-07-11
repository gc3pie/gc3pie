The above `bf.uzh.ch` directory contains code developed by B. Jonen
and S. Scheuring in support of their academic work.

The programs are maintained by their authors and most of them follow
the same pattern.  Only instructions for testing ``gidRiskUML`` are
given here.


Testing ``gidRiskParaSearchUML``
================================

The ``gidRiskParaSearchUML`` application needs:

* a directory with the "base" input files (set with ``-b`` option);
* a ``para.loop`` file to define parameter substitutions to be
  performed on the base input files: for each valid combination of
  parameter substitutions, one GC3Pie job is generated.
* the path to the ``idRisk`` executable to run (set with the ``-x``
  option)
* a number of other parameters related to the simulation.

Invocation of ``gidRiskParaSearchUML`` follows the usual session-based script
conventions::

    cd gammaLoop
    ../gidRiskParaSearchUML.py -x ../idRiskOut -b ../base para.loop -xVars 'wBarLower' -xVarsDom '-0.6 -0.3 -0.1 0. 0.05' -targetVars 'iBar' --makePlots True -target_fx -0.1 -yC 4.9e-3 -sv info -C 10 -N -A "apppot0+ben.diskUpd.img"

The appPot image can be dropped once it is preloaded on the grid. 

Execution of ``gidRiskParaSearchUML`` starts with a report like this one::

    2012-03-05 16:49:37 - gc3.gc3utils: redirected gc3 log to gidRiskParaSearchUML.log.
    initialized new instance of costlyOptimization for jobname = para_beta=0.9500000000_gamma=2.0000000000_nAgent=4_psi=-1driver
    initialized new instance of costlyOptimization for jobname = para_beta=0.9500000000_gamma=2.1000000000_nAgent=4_psi=-1driver
    [...]
    Status of jobs in the 'XXX' session: (at 13:08:08, 02/28/12)
            NEW    0/2    (0.0%)  
        RUNNING    0/2    (0.0%)  
        STOPPED    0/2    (0.0%)  
      SUBMITTED    2/2   (100.0%) 
     TERMINATED    0/2    (0.0%)  
    TERMINATING    0/2    (0.0%)  
          total   10/2   (100.0%) 

There are 2 jobs generated initially; the number will vary over time
as the optimization proceeds.
 
The processing ends when TERMINATED has reached 100% jobs; the whole
optimization process takes about 1 hour.  

When executed finished correctly, ``gidRiskParaSearchUML`` script will
print a large table of values just before exiting.  This table should
match the one in the file `gammaLoop/optimalRuns`.

For each of the two initial jobs (see above), there will be one
directory created::

    $ ls para_beta=0.9500000000_gamma=2.0000000000_nAgent=4_psi=-1/op
    optimalRun      optimwBarLower/ 

The ``optimwBarLower`` contains more files and one directory per
optimization job tried::

    $ ls para_beta=0.9500000000_gamma=2.0000000000_nAgent=4_psi=para_beta\=0.9500000000_gamma\=2.0000000000_nAgent\=4_psi\=-1/optimwBarLower/
    costlyOpt.log
    lagrangeApprox_1.eps
    lagrangeApprox_2.eps
    overviewSimu
    para_beta=0.9500000000_gamma=2.0000000000_nAgent=4_psi=-1_wBarLower=0.0/
    para_beta=0.9500000000_gamma=2.0000000000_nAgent=4_psi=-1_wBarLower=0.05/
    para_beta=0.9500000000_gamma=2.0000000000_nAgent=4_psi=-1_wBarLower=-0.1/
    para_beta=0.9500000000_gamma=2.0000000000_nAgent=4_psi=-1_wBarLower=-0.3/
    para_beta=0.9500000000_gamma=2.0000000000_nAgent=4_psi=-1_wBarLower=-0.353607353607/
    para_beta=0.9500000000_gamma=2.0000000000_nAgent=4_psi=-1_wBarLower=-0.6/

And again, each of the sub-jobs subdirectories host the actual log
files, an `input/` directory with a copy of the (modified) input
files, and an `output/` directory with the actual output results.
