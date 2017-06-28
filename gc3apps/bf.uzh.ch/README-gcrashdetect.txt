.. Main application developers: "Anastasiia Sokko"

Requirements for running ``gcrashdetect``
==========================================

`gcrashdetect` is a frontend script to run CrashDetect Matlab function.

``gcrashdetect`` help
=====================

    $ gcrashdetect -h

Running ``gcrashdetect``
=========================

## Step 1: Activate GC3Pie and ScienceCloud API authentication
    $ . gc3pie/bin/activate
    $ source gc3pie/bin/sc-authenticate.sh

Note: Provide UZH Webpass shortname and password at prompt as requested.

## Step 2: Execute gcrashdetect in `screen`
Example:
    $ screen -L `which gcrashdetect` S3IT_CrashDetector S3IT/CDparams.csv -d S3IT/ -s 20170626 -C 20 -o results -N

From the provided example, one could customize:
* folder containing Matlab scripts as well as .cpp CrashDetect code to be compiled ('S3IT/')
* name of the Matlab function to be invoked ('S3IT_CrashDetector')
* Input .csv

Other GC3Pie specific options that can be customized:
* session name: the value passed to the `-s` option (20170626 in the
example)

Note: the `-N` option removes all previous executions. Use it **only**
when you need to start a new simulation and forget about previous
ones.

## Step 3: Detach from running `screen` session 

Once ``gcrashdetect`` runs (and it has been launched with the `-C`
option, it will continuously supervise the execution of all provided
input parameters until completion. In order to properly detach from
the running session and let the ``gcrashdetect`` run in background, it
is necessary to detach from the running `screen` session.

* How to detach form a running `screen` session
    $ Ctrl-A Ctrl-D

## Step 4: periodically check the progress of the ``gcrashdetect``
   execution

Either re-attach to the running `screen` session by:
    $ screen -r
Note: remember to detach from the running screen session using the:
    ``$ Ctrl-A Ctrl-D`` command

...or check the content of the `screen` log file:
    $ less screen.log

Temporally stop a running ``gcrashdetect`` session
===================================================

Re-attach to the running `screen` session
    $ screen -r

and stop the execution by:
    $ Ctrl-C

This will interrupt the execution of ``gcrashdetect`` but will **not**
terminate the instances running on ScienceCloud.

Resume ``gcrashdetect`` execution
==================================

Just re-run the `gcrashdetect` command. It will resume from the point it
was stopped.

Note: Unless you run `gcrashdetect` command with the `-N` option. In that
case it will start a new session and loose all previous executions.

Terminating the instances on ScienceCloud started by ``gcrashdetect``
======================================================================

$ gsession abort <session path>
$ gsession delete <session path>

Note: <session path> is the folder created by GC3Pie after the option -s.
In the provided example <session path would be: '$HOME/20170626'.

For more information on how to use the GC3Pie session commands, please
attend one of the next S3IT training events or consult the online
documentation at: http://gc3pie.readthedocs.org/en/master/users/gc3utils.html




