.. Main application developers: "Jody"

Requirements for running ``gqhg``
==================================

`gqhg` requires as input argument a folder reference containing
the list of valid input files in .qdf formats.

Additionally, `gqhg` requires the location of the Grid file
to be passed together with each valid input file.
Grid file can be specified with the `-G PATH` option.

Running ``gqhg``
================

## Step 1: Activate ScienceCloud API authentication
    $ source .virtualenvs/s3it/bin/sc-authenticate.sh

Provide UZH Webpass shortname and password at prompt as requested.
Note: the location of the authenticate script may depend on your
GC3Pie installation.

## Step 2: Execute gqhg in `screen`
Example:
    $ screen `which python` gqhg.py in/ -G grid_files/Grid_20k_256.qdf -I 10 -n 10 -C
    20 -vvvvv -s today -o out

From the provided example, one could customize:
* location of the input folder.
* location of Grid file
* Number of time a single simulation will have to be repeated. In the
example the `-I 10` option will create 10 simulations for each valid
input file. Note: each simulation will use a different `seed` value.
* Number of steps each simulation will perform. In the example
the `-n 10` option sets only 10 simulation steps.

Other GC3Pie specific options that can be customized:
* session name: the value passed to the `-s` option (`today` in the
example)

Note: the `-N` option removes all previous executions. Use it **only**
when you need to start a new simulation and forget about previous
ones.

## Step 3: Detach from running `screen` session 

Once ``gqhg`` runs (and it has been launched with the `-C`
option, it will continuously supervise the execution of all provided
input parameters until completion. In order to properly detach from
the running session and let the ``gqhg`` run in background, it
is necessary to detach from the running `screen` session.

* How to detach form a running `screen` session
    $ Ctrl-A Ctrl-D

## Step 4: periodically check the progress of the ``gqhg``
   execution

Either re-attach to the running `screen` session by:
    $ screen -r
Note: remember to detach from the running screen session using the:
    ``$ Ctrl-A Ctrl-D`` command

...or check the content of the `screen` log file:
    $ less screen.log

Temporally stop a running ``gqhg`` session
================================================

Re-attach to the running `screen` session
    $ screen -r

and stop the execution by:
    $ Ctrl-C

This will interrupt the execution of ``gqhg`` but will **not**
terminate the instances running on ScienceCloud.

Resume ``gqhg`` execution
==============================
Just re-run the `gqhg` command. It will resume from the point it
was stopped.

Note: Unless you run `gqhg` command with the `-N` option. In that
case it will start a new session and loose all previous executions.

Terminating the instances on ScienceCloud started by ``gqhg``
===================================================================

$ gsession terminate <session path>
