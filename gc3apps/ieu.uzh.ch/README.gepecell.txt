.. Main application developers: "Charles"

Requirements for running ``gepecell``
===========================================================

`gepecell` requires an input .csv file containing a list of
parameters - one line per parameter combination.

``gepecell`` help
=======================================

    $ gepecell -h

Running ``gepecell``
==========================================

## Step 1: Activate ScienceCloud API authentication
    $ source ~/gc3pie/bin/sc-authenticate.sh

Provide UZH Webpass shortname and password at prompt as requested.

## Step 2: Execute gepecell in `screen`
Example:
    $ screen -L `which gepecell` input.csv -B bin/robustnessintime -s 20160405 -C 20 -N -o results

From the provided example, one could customize:
* input .csv file
* location of `robustnessintime` binary file.
Note: `robustnessintime` binary file MUST be statically linked.

Other GC3Pie specific options that can be customized:
* session name: the value passed to the `-s` option (20160405 in the
example)

Note: the `-N` option removes all previous executions. Use it **only**
when you need to start a new simulation and forget about previous
ones.

## Step 3: Detach from running `screen` session

Once ``gepecell`` runs (and it has been launched with the `-C`
option, it will continuously supervise the execution of all provided
input parameters until completion. In order to properly detach from
the running session and let the ``gepecell`` run in background, it
is necessary to detach from the running `screen` session.

* How to detach form a running `screen` session
    $ Ctrl-A Ctrl-D

## Step 4: periodically check the progress of the ``gepecell``
   execution

Either re-attach to the running `screen` session by:
    $ screen -r
Note: remember to detach from the running screen session using the:
    ``$ Ctrl-A Ctrl-D`` command

...or check the content of the `screen` log file:
    $ less screen.log

Why all these ERROR messages ?
==============================

If you see ERROR messages like the follwoing:
gc3.gc3libs: ERROR: Got error in submitting task
... ResourceNotReady: Delaying submission until some
of the VMs currently pending is ready.

or:
gc3.gc3libs: ERROR: Could not create ssh connection to 172.23....:
NoValidConnectionsError: [Errno None] Unable to connect to port 22 on
or 172.23...

these are to be considered as transient errors: Errors that are
induced during the resouce provisioning step of the workflow and that
are supposed to be resolved as soon as the resources and ready to be used.

Temporally stop a running ``gepecell`` session
====================================================================

Re-attach to the running `screen` session
    $ screen -r

and stop the execution by:
    $ Ctrl-C

This will interrupt the execution of ``gepecell`` but will **not**
terminate the instances running on ScienceCloud.

Resume ``gepecell`` execution
==================================================
Just re-run the `gepecell` command. It will resume from the point it
was stopped.

Note: Unless you run `gepecell` command with the `-N` option. In that
case it will start a new session and loose all previous executions.

Terminating the instances on ScienceCloud started by ``gepecell``
======================================================================================

$ gsession terminate <session path>


For more information on how to use the GC3Pie session commands, please
attend one of the next S3IT training events or consult the online
documentation at: http://gc3pie.readthedocs.org/en/master/users/gc3utils.html
