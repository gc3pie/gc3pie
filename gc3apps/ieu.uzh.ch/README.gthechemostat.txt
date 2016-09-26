.. Main application developers: "Maria, San Roman Rincon"

Requirements for running ``gthechemostat``
========================================

`gthechemostat` requires as input argument an .csv file containing the
list of all the parameters that will have to be passed to the Matlab
script containing the `theChemostat` function.

Expected signature of the main function:
<function name> <input csv> <result folder>

Example:
thechemostat('chunk_1.csv','./results')

Additionally, `gthechemostat` requires the location of the Matlab scripts
to be executed to be specified in the `-R PATH` option.

Running ``gthechemostat``
======================

## Step 0: activate GC3Pie environemnt
    $ source ~/gc3pie/bin/activate

## Step 1: Activate ScienceCloud API authentication
    $ source ~/gc3pie/bin/sc-authenticate.sh

Provide UZH Webpass shortname and password at prompt as requested.

Note: environmental variables set by the sc-authenticate command are
only valid for the running shell session. If you log-out and log-in
again, or simply connect through another shell session, you will have
to re-run the sc-authenticate script again before being able to run
any GC3Pie command.

### Step 1.1: Verify access to ScienceCloud resource
    $ gservers

Check for output:
    (Accessible? ) | True

## Step 2: Execute gthechemostat in `screen`
Example:
    $ screen -L `which gthechemostat` input.csv -R sources -f thechemostat -C 30 -s 20151104 -o results -N

From the provided example, one could customize:
* location of input.csv file (instead of `input.csv`),
* location of Matlab scripts, where the main function is located (use
the `-R` option),
* name of the Matlab function to call (use the `-f` option). Note:
there must be a cooresponding Matlab file, named after the main
function, in the Matlab scripts folder specified with the `-R` option.
* Location of result folder (use the `-o` option)

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
===================================================

Re-attach to the running `screen` session
    $ screen -r

and stop the execution by:
    $ Ctrl-C

This will interrupt the execution of ``gthechemostat`` but will **not**
terminate the instances running on ScienceCloud.

Resume ``gthechemostat`` execution
===================================

Just re-run the `gthechemostat` command. It will resume from the point it
was stopped.

Note: Unless you run `gthechemostat` command with the `-N` option. In that
case it will start a new session and loose all previous executions.

Terminating the instances on ScienceCloud started by ``gthechemostat``
======================================================================

$ gsession terminate <session path>

How to control a running session and debug errors ?
===================================================

Please refer to the handout material of the `GC3Pie tools` training
available at: 
