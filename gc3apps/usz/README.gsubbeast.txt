.. Main application developers: "Denise Kühnert"

Requirements for running ``gsubbeast``
===============================================

Takes an input folder containing 1 or more .XML files, for each of the
found .XML files runs a java application (specified in a .jar file
that could also be passed as input option_), and repeats the execution
a given number of time.

The ``gsubbeast`` command keeps a record of jobs (submitted,
executed and pending) in a session file (set name with the ``-s``
option); at each invocation of the command, the status of all recorded
jobs is updated, output from finished jobs is collected, and a summary
table of all known jobs is printed.
 
Running ``gsubbeast``
===============================

## Step 1: Activate ScienceCloud API authentication
    $ source ~/gc3pie/bin/sc-authenticate.sh

Provide UZH Webpass shortname and password at prompt as requested.

## Step 2: Execute gsubbeast in `screen`

usage: gsubbeast [OPTIONS] input_folder

positional arguments:
  input_folder          Path to input folder containing valid input .xml
                        files.
Example:
    $ screen -L `which gsubbeast` -R 2 -B ~/jar/bdmm.jar -s today -o
  results -C 20 -N data/ 

From the provided example, one could customize:
* Location of the input folder containing the .XML files ('~/data' in
the example_).
* Location of the .jar file containing the simulation application
(change the -B option)
* Number of repetitions for each valid input .XML file (change the -R
3 option).
* Location of result folder (chage the -o results optiob).

Other GC3Pie specific options that can be customized:
* session name: the value passed to the `-s` option (`today` in the
example)

Note: the `-N` option removes all previous executions. Use it **only**
when you need to start a new simulation and forget about previous
ones.

## Step 3: Detach from running `screen` session 

Once ``gsubbeast`` runs (and it has been launched with the `-C`
option, it will continuously supervise the execution of all provided
input parameters until completion. In order to properly detach from
the running session and let the ``gsubbeast`` run in background, it
is necessary to detach from the running `screen` session.

* How to detach form a running `screen` session
    $ Ctrl-A Ctrl-D

## Step 4: periodically check the progress of the ``gsubbeast``
   execution

Either re-attach to the running `screen` session by:
    $ screen -r
Note: remember to detach from the running screen session using the:
    ``$ Ctrl-A Ctrl-D`` command

...or check the content of the `screen` log file:
    $ less screen.log

Temporally stop a running ``gsubbeast`` session
================================================

Re-attach to the running `screen` session
    $ screen -r

and stop the execution by:
    $ Ctrl-C

This will interrupt the execution of ``gsubbeast`` but will **not**
terminate the instances running on ScienceCloud.

Resume ``gsubbeast`` execution
==============================
Just re-run the `gsubbeast` command. It will resume from the point it
was stopped.

Note: Unless you run `gsubbeast` command with the `-N` option. In that
case it will start a new session and loose all previous executions.

Terminating the instances on ScienceCloud started by ``gsubbeast``
===================================================================

$ gsession terminate <session path>
