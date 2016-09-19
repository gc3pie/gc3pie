.. Main application developers: "Maria, San Roman Rincon"

Requirements for running ``gthechemostat``
========================================

`gthechemostat` requires as input argument an .csv file containing the
list of all the parameters that will have to be passed to the Matlab
script containing the `theChemostat` function.

Additionally, `gthechemostat` requires the location of the Matlab scripts
to be executed to be specified in the `-R PATH` option.

Running ``gthechemostat``
======================

## Step 1: Activate ScienceCloud API authentication
    $ source .virtualenvs/s3it/bin/sc-authenticate.sh

Provide UZH Webpass shortname and password at prompt as requested.

## Step 2: Execute gthechemostat in `screen`
Example:
    $ screen -L `which gthechemostat` data/in/input.csv -d data/src/ -C 30 -s 20151104 -o data/out -N

From the provided example, one could customize:
* location of input.csv file: instead of `data/in/inp.csv`, provide the
full path of an alternative input .csv file
* location of Matlab scripts (where MainFunction.m is located):
instead of using `data/src`, provide the full path of an alternative
folder where the MainFunction and its related Matlab scripts are
located.
* Location of result folder: instead of `data/out` provide the
alternative path to the result folder (where all the results will be stored)

Other GC3Pie specific options that can be customized:
* session name: the value passed to the `-s` option (20151104 in the
example)

Note: the `-N` option removes all previous executions. Use it **only**
when you need to start a new simulation and forget about previous
ones.

## Step 3: Detach from running `screen` session 

Once ``gthechemostat`` runs (and it has been launched with the `-C`
option, it will continuously supervise the execution of all provided
input parameters until completion. In order to properly detach from
the running session and let the ``gthechemostat`` run in background, it
is necessary to detach from the running `screen` session.

* How to detach form a running `screen` session
    $ Ctrl-A Ctrl-D

## Step 4: periodically check the progress of the ``gthechemostat``
   execution

Either re-attach to the running `screen` session by:
    $ screen -r
Note: remember to detach from the running screen session using the:
    ``$ Ctrl-A Ctrl-D`` command

...or check the content of the `screen` log file:
    $ less screen.log

Temporally stop a running ``gthechemostat`` session
================================================

Re-attach to the running `screen` session
    $ screen -r

and stop the execution by:
    $ Ctrl-C

This will interrupt the execution of ``gthechemostat`` but will **not**
terminate the instances running on ScienceCloud.

Resume ``gthechemostat`` execution
==============================
Just re-run the `gthechemostat` command. It will resume from the point it
was stopped.

Note: Unless you run `gthechemostat` command with the `-N` option. In that
case it will start a new session and loose all previous executions.

Terminating the instances on ScienceCloud started by ``gthechemostat``
===================================================================

$ gsession terminate <session path>





