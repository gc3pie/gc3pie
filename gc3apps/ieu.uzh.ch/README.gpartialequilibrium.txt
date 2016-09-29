.. Main application developers: "Maria, San Roman Rincon"

Requirements for running ``gpartialequilibrium``
==============================================

`gpartialequilibrium` requires as input argument a list of .csv files
each of them containing the list of all the parameters that will have
to be passed to the Matlab script containing the main function to be
executed.

Expected signature of the Matlab main function:
<function name> <input csv> <result folder>

Example:
partialequilibrium('chunk_1.csv','./results')

Additionally, `gpartialequilibrium` requires the location of the
Matlab scripts to be executed to be specified in the `-R PATH` option.

For each line of each input .csv file a GpartialequilibriumApplication
needs to be generated (depends on chunk value passed as part of the
input options). Splits input .csv file into smaller chunks, each of
them of size 'self.params.chunk_size'. Then submits one execution for
each of the created chunked files.

The ``gpartialequilibrium`` command keeps a record of jobs (submitted,
executed and pending) in a session file (set name with the ``-s``
option); at each invocation of the command, the status of all recorded
jobs is updated, output from finished jobs is collected, and a summary
table of all known jobs is printed.

Results of each simulation is availabe in the 'result' folder.

Running ``gpartialequilibrium``
===============================

## Step 0: Activate GC3Pie environment (optional depending on
   installation)
    $ source ~/gc3pie/src/activate

## Step 1: Activate ScienceCloud API authentication
    $ source ~/gc3pie/bin/sc-authenticate.sh

Provide UZH Webpass shortname and password at prompt as requested.

## Step 2: Execute gpartialequilibrium in `screen`
Example:
    $ screen -L `which gpartialequilibrium` input1.csv input2.csv -k
    10 -R matlab_src/ -f partialequilibrium -C 30 -s 20151104 -o
    results -w 15GB -N

From the provided example, one could customize:
* location of input .csv files.
* location of Matlab scripts where the main function is located (using
the `-R` option).
* Location of result folder (using the `-o` option).
* Name of the Matlab main function to execute (using the `-f` option).
* Chunk size for the input .csv file (use the `-k` option).
Other GC3Pie specific options that can be customized:
* session name: the value passed to the `-s` option (20151104 in the
example)

Note: the `-N` option removes all previous executions. Use it **only**
when you need to start a new simulation and forget about previous
ones.

## Step 3: Detach from running `screen` session 

Once ``gpartialequilibrium`` runs (and it has been launched with the `-C`
option, it will continuously supervise the execution of all provided
input parameters until completion. In order to properly detach from
the running session and let the ``gpartialequilibrium`` run in background, it
is necessary to detach from the running `screen` session.

* How to detach form a running `screen` session
    $ Ctrl-A Ctrl-D

## Step 4: periodically check the progress of the ``gpartialequilibrium``
   execution

Either re-attach to the running `screen` session by:
    $ screen -r
Note: remember to detach from the running screen session using the:
    ``$ Ctrl-A Ctrl-D`` command

...or check the content of the `screen` log file:
    $ less screen.log

Temporally stop a running ``gpartialequilibrium`` session
==========================================================

Re-attach to the running `screen` session
    $ screen -r

and stop the execution by:
    $ Ctrl-C

This will interrupt the execution of ``gpartialequilibrium`` but will **not**
terminate the instances running on ScienceCloud.

Resume ``gpartialequilibrium`` execution
=========================================
Just re-run the `gpartialequilibrium` command. It will resume from the point it
was stopped.

Note: Unless you run `gpartialequilibrium` command with the `-N` option. In that
case it will start a new session and loose all previous executions.

How to control a running session and debug errors ?
===================================================

Please refer to the handout material of the `GC3Pie tools` training
available at: 
https://github.com/uzh/gc3pie/blob/master/docs/users/tutorial/slides.pdf
