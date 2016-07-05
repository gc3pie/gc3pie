.. Main application developers: "Ladina, Franz"

Requirements for running ``gtrac``
====================================

`gtrac` requires as input folder containing all subjects information.
Note: expected folder structure:
* 1 subfodler for each subject.
* In each subject folder, 
* 1 subfolder for each TimePoint.
* Each TimePoint folder should contain 2 input NFTI files. 

``gtrac`` help
===============

    $ gtrac -h

Running ``gtrac``
==================

## Step 1: Activate ScienceCloud API authentication
    $ source .virtualenvs/s3it/bin/sc-authenticate.sh

Provide UZH Webpass shortname and password at prompt as requested.

## Step 2: Execute gtrac in `screen`
Example:
    $ screen -L `which gtrac` data/in/ -C 30 -s 20151104 -o data/out -N

From the provided example, one could customize:

* location of input folder: instead of `data/in/`, provide the
full path of an alternative input folder where the NIFTI files are.

* Location of result folder: instead of `data/out` provide the
alternative path to the result folder (where all the results will be
stored)

Other GC3Pie specific options that can be customized:
* session name: the value passed to the `-s` option (20151104 in the
example)

Note: the `-N` option removes all previous executions. Use it **only**
when you need to start a new simulation and forget about previous
ones.

## Step 3: Detach from running `screen` session 

Once ``gtrac`` runs (and it has been launched with the `-C`
option, it will continuously supervise the execution of all provided
input parameters until completion. In order to properly detach from
the running session and let the ``gtrac`` run in background, it
is necessary to detach from the running `screen` session.

* How to detach form a running `screen` session
    $ Ctrl-A Ctrl-D

## Step 4: periodically check the progress of the ``gtrac``
   execution

Either re-attach to the running `screen` session by:
    $ screen -r
Note: remember to detach from the running screen session using the:
    ``$ Ctrl-A Ctrl-D`` command

...or check the content of the `screen` log file:
    $ less screen.log

Temporally stop a running ``gtrac`` session
================================================

Re-attach to the running `screen` session
    $ screen -r

and stop the execution by:
    $ Ctrl-C

This will interrupt the execution of ``gtrac`` but will **not**
terminate the instances running on ScienceCloud.

Resume ``gtrac`` execution
==============================
Just re-run the `gtrac` command. It will resume from the point it
was stopped.

Note: Unless you run `gtrac` command with the `-N` option. In that
case it will start a new session and loose all previous executions.

Terminating the instances on ScienceCloud started by ``gtrac``
===================================================================

$ gsession terminate <session path>

How to control running `gtrac` sessions ?
=========================================

Please take the time to go through the GC3Pie tools training available at:
https://github.com/uzh/gc3pie/blob/master/docs/users/tutorial/slides.pdf

