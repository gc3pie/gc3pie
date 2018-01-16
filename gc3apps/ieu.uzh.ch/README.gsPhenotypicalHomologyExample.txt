.. Main application developers: "Susanne Schindler"

Requirements for running ``gsPhenotypicalHomologyExample``
===========================================================

`gsPhenotypicalHomologyExample` requires an input range for the
hunting pressures value to be passed to the running simulations.
`gsPhenotypicalHomologyExample` will create for each input value in
the range a single execution of the StartSimulation java program.

Additionally, `gsPhenotypicalHomologyExample` can be configured with a
location of the local - i.e. to the computer where
`gsPhenotypicalHomologyExample` is running - java source code (the -S
option) or of the existing pre-package .jar file (the -j option), of
the local path where the input parameter file (e.g. the
param_SheepRiver specified with the -P option), of the seed file (the
-M option) and of the number of iterations each StartSimulation
execution will go through (the -I option).

``gsPhenotypicalHomologyExample`` help
=======================================

    $ gsPhenotypicalHomologyExample -h

Running ``gsPhenotypicalHomologyExample``
==========================================

## Step 0: Activate GC3Pie virtual environment
    $ source ~/gc3pie/bin/activate

## Step 1: Activate ScienceCloud API authentication
    $ source ~/gc3pie/bin/sc-authenticate.sh

Provide UZH Webpass shortname and password at prompt as requested.

## Step 2: Execute gsPhenotypicalHomologyExample in `screen`
Example:
    $ screen -L `which gsPhenotypicalHomologyExample` 67,66 -S src/ -P
    data/in/param_SheepRiver_wear -M data/in/listOfRandomSeeds -I 10
    -s 20160405b -C 20 -N

From the provided example, one could customize:
* hunting pressures input range (valid values are:
[int],[int]|[int]:[int]. E.g 1:432|3|6,9)
* location of Java source scripts (where StartSimulation.java is
located on your local drive). Note: in order to properly compile and
execute the java source code, a MANIFEST.MF file needs to be present
in the java source folder. To the very minumum, a MANIFEST.MF file
should contain:
    Manifest-Version: 1.0
    Main-Class: [Name of the class where the main method is defined]
    (e.g. StartSimulation)
* Location of .jar package if already prepared. Default: None
* Location of the 'param_SheepRiver_wear' input file. Default: None
* Location of the seeds file. Default: None
* Number of repeating iterations. Default: 10000

Other GC3Pie specific options that can be customized:
* session name: the value passed to the `-s` option (20151104 in the
example)

Note: the `-N` option removes all previous executions. Use it **only**
when you need to start a new simulation and forget about previous
ones.

## Step 3: Detach from running `screen` session

Once ``gsPhenotypicalHomologyExample`` runs (and it has been launched with the `-C`
option, it will continuously supervise the execution of all provided
input parameters until completion. In order to properly detach from
the running session and let the ``gsPhenotypicalHomologyExample`` run in background, it
is necessary to detach from the running `screen` session.

* How to detach form a running `screen` session
    $ Ctrl-A Ctrl-D

## Step 4: periodically check the progress of the ``gsPhenotypicalHomologyExample``
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

Temporally stop a running ``gsPhenotypicalHomologyExample`` session
====================================================================

Re-attach to the running `screen` session
    $ screen -r

and stop the execution by:
    $ Ctrl-C

This will interrupt the execution of ``gsPhenotypicalHomologyExample`` but will **not**
terminate the instances running on ScienceCloud.

Resume ``gsPhenotypicalHomologyExample`` execution
==================================================
Just re-run the `gsPhenotypicalHomologyExample` command. It will resume from the point it
was stopped.

Note: Unless you run `gsPhenotypicalHomologyExample` command with the `-N` option. In that
case it will start a new session and loose all previous executions.

Terminating the instances on ScienceCloud started by ``gsPhenotypicalHomologyExample``
======================================================================================

$ gsession terminate <session path>


For more information on how to use the GC3Pie session commands, please
attend one of the next S3IT training events or consult the online
documentation at: http://gc3pie.readthedocs.org/en/master/users/gc3utils.html
