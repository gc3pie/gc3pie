.. Main application developers: "Doris"

Requirements for running ``gpredict_PopContCC``
===============================================

Takes a matlab function file as first argument, then MatPredictor .mat
file then a VecResponse .mat file, then numberOfSamples and
numberOfTrees. Additionally takes the number of repetitions as
option. For each repetitions, the script executed the function
specified in the matlab function.

expected function signature:
predict_PopContCC(matPredictor,vecResponse,numberOfSamples,numberOfTrees,resultFolder)

The ``gpredict_PopContCC`` command keeps a record of jobs (submitted,
executed and pending) in a session file (set name with the ``-s``
option); at each invocation of the command, the status of all recorded
jobs is updated, output from finished jobs is collected, and a summary
table of all known jobs is printed.
 
Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; ``gpredict_PopContCC`` will delay
submission of newly-created jobs so that this limit is never
exceeded. Once the processing of all chunked files has been completed,
``gpredict_PopContCC`` aggregates them into a single larger output
file located in 'self.params.output'.


Running ``gpredict_PopContCC``
===============================

## Step 1: Activate ScienceCloud API authentication
    $ source ~/gc3pie/bin/sc-authenticate.sh

Provide UZH Webpass shortname and password at prompt as requested.

## Step 2: Execute gpredict_PopContCC in `screen`

usage: gpredict_PopContCC [OPTIONS] Mfunct MatPredictor VecResponse numberOfSamples numberOfTrees

positional arguments:
  Mfunct                Path to the gpredict_PopContCC file.
  MatPredictor          Path to the MatPredictor file.
  VecResponse           Path to the VecResponse file.
  numberOfSamples       Number of samples.
  numberOfTrees         Number of trees.

Example:
    $ screen -L `which gpredict_PopContCC` predict_PopContCC.m
    testMatPredictor.mat testVecResponse.mat 10000 10 -R 3 -o results
    -C 10 -s today

From the provided example, one could customize:
* location of the Matlab script containing the predict_PopContCC function.
* location of both the MatPredictor and VecResponse Matlab file.
* Number of Samples (change the 10000 argument).
* Number of Trees (change the 10 argument).
* Number of repetitions (change the -R 3 option).
* Location of result folder (chage the -o results optiob).

Other GC3Pie specific options that can be customized:
* session name: the value passed to the `-s` option (`today` in the
example)

Note: the `-N` option removes all previous executions. Use it **only**
when you need to start a new simulation and forget about previous
ones.

## Step 3: Detach from running `screen` session 

Once ``gpredict_PopContCC`` runs (and it has been launched with the `-C`
option, it will continuously supervise the execution of all provided
input parameters until completion. In order to properly detach from
the running session and let the ``gpredict_PopContCC`` run in background, it
is necessary to detach from the running `screen` session.

* How to detach form a running `screen` session
    $ Ctrl-A Ctrl-D

## Step 4: periodically check the progress of the ``gpredict_PopContCC``
   execution

Either re-attach to the running `screen` session by:
    $ screen -r
Note: remember to detach from the running screen session using the:
    ``$ Ctrl-A Ctrl-D`` command

...or check the content of the `screen` log file:
    $ less screen.log

Temporally stop a running ``gpredict_PopContCC`` session
================================================

Re-attach to the running `screen` session
    $ screen -r

and stop the execution by:
    $ Ctrl-C

This will interrupt the execution of ``gpredict_PopContCC`` but will **not**
terminate the instances running on ScienceCloud.

Resume ``gpredict_PopContCC`` execution
==============================
Just re-run the `gpredict_PopContCC` command. It will resume from the point it
was stopped.

Note: Unless you run `gpredict_PopContCC` command with the `-N` option. In that
case it will start a new session and loose all previous executions.

Terminating the instances on ScienceCloud started by ``gpredict_PopContCC``
===================================================================

$ gsession terminate <session path>
