.. Main application developers: "Francesca"

Requirements for running ``gbraunian``
==================================

`gbraunian` requires as input argument a folder reference containing
the list of valid input files in .qdf formats.

Additionally, `gbraunian` requires the location of the Grid file
to be passed together with each valid input file.
Grid file can be specified with the `-G PATH` option.

Running ``gbraunian``
================

## Step 1: Activate ScienceCloud API authentication
    $ source .virtualenvs/s3it/bin/sc-authenticate.sh

Provide UZH Webpass shortname and password at prompt as requested.
Note: the location of the authenticate script may depend on your
GC3Pie installation.

## Step 2: Execute gbraunian in `screen`
Example:
    $ screen `which python` ./gbraunian.py 100 ./brownian_cloud_s3it.m ./3d_case1.txt -s 20160714 -vvvv

From the provided example, one could customize:
* Number of total events: 100
* Location of braunian Matlab function: brownian_cloud-s3it.m
* Location of the case file: 3d_case1.txt

Other GC3Pie specific options that can be customized:
* session name: the value passed to the `-s` option (`today` in the
example)

Note: the `-N` option removes all previous executions. Use it **only**
when you need to start a new simulation and forget about previous
ones.

## Step 3: Detach from running `screen` session 

Once ``gbraunian`` runs (and it has been launched with the `-C`
option, it will continuously supervise the execution of all provided
input parameters until completion. In order to properly detach from
the running session and let the ``gbraunian`` run in background, it
is necessary to detach from the running `screen` session.

* How to detach form a running `screen` session
    $ Ctrl-A Ctrl-D

## Step 4: periodically check the progress of the ``gbraunian``
   execution

Either re-attach to the running `screen` session by:
    $ screen -r
Note: remember to detach from the running screen session using the:
    ``$ Ctrl-A Ctrl-D`` command

...or check the content of the `screen` log file:
    $ less screen.log

Temporally stop a running ``gbraunian`` session
================================================

Re-attach to the running `screen` session
    $ screen -r

and stop the execution by:
    $ Ctrl-C

This will interrupt the execution of ``gbraunian`` but will **not**
terminate the instances running on ScienceCloud.

Resume ``gbraunian`` execution
==============================
Just re-run the `gbraunian` command. It will resume from the point it
was stopped.

Note: Unless you run `gbraunian` command with the `-N` option. In that
case it will start a new session and loose all previous executions.

Terminating the instances on ScienceCloud started by ``gbraunian``
===================================================================

$ gsession terminate <session path>
