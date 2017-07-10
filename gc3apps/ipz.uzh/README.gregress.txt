.. Main application developers: "Paula Castro"

Requirements for running ``gregress``
==========================================

`gregress` is a frontend script to run CrashDetect Matlab function.

``gregress`` help
=====================

    $ gregress -h

Running ``gregress``
=========================

## Step 1: Activate GC3Pie and ScienceCloud API authentication
    $ source gc3pie/bin/sc-authenticate.sh

Note: Provide UZH Webpass shortname and password at prompt as requested.

## Step 2: Execute gregress in `screen`
Example:
    $ screen -L `which gregress` data/ -x src --result results -s 20170707 -C 10 -o outputs -N
  
From the provided example, one could customize:
* data folder where to find the initial RData DataFrame 
* source folder where R scripts - included the main workflow - are located (use the `-x` option)
* where results are stored (use the `--result` option)
* where log information for each step of the pipeline are stored (use the `-o` option)

Other GC3Pie specific options that can be customized:
* session name: the value passed to the `-s` option (20170707 in the
example)

Note: the `-N` option removes all previous executions. Use it **only**
when you need to start a new simulation and forget about previous
ones.

## Step 3: Detach from running `screen` session 

Once ``gregress`` runs (and it has been launched with the `-C`
option, it will continuously supervise the execution of all provided
input parameters until completion. In order to properly detach from
the running session and let the ``gregress`` run in background, it
is necessary to detach from the running `screen` session.

* How to detach form a running `screen` session
    $ Ctrl-A Ctrl-D

## Step 4: periodically check the progress of the ``gregress``
   execution

Either re-attach to the running `screen` session by:
    $ screen -r
Note: remember to detach from the running screen session using the:
    ``$ Ctrl-A Ctrl-D`` command

...or check the content of the `screen` log file:
    $ less screen.log

Temporally stop a running ``gregress`` session
===================================================

Re-attach to the running `screen` session
    $ screen -r

and stop the execution by:
    $ Ctrl-C

This will interrupt the execution of ``gregress`` but will **not**
terminate the instances running on ScienceCloud.

Resume ``gregress`` execution
==================================

Just re-run the `gregress` command. It will resume from the point it
was stopped.

Note: Unless you run `gregress` command with the `-N` option. In that
case it will start a new session and loose all previous executions.

Terminating the instances on ScienceCloud started by ``gregress``
======================================================================

$ gsession abort <session path>
$ gsession delete <session path>

Note: <session path> is the folder created by GC3Pie after the option -s.
In the provided example <session path would be: '$HOME/20170707'.

For more information on how to use the GC3Pie session commands, please
attend one of the next S3IT training events or consult the online
documentation at: http://gc3pie.readthedocs.org/en/master/users/gc3utils.html
