The ``gsisp`` application allows to run ``sisp`` binary over a series
of different input paramter files all named ``parameters.in``.
The ``input`` folder contains example of ``parameters.in`` input
files.
The ``bin`` folder contains ``sisp`` binary.

.. Main application developers: "Emanuel Fronhofer"

Requirements for running ``gsisp``
==================================

``gsisp`` requires input parameter files, one for each combination of
the parameters to be tested. Each parameter file shoul dbe placed in
its own input folder, all under a root folder that will be passed to
``gsisp`` as input argument.

``gsisp`` help
==============

    $ gsisp.py -h

Invocation of ``gsisp`` follows the usual session-based script
conventions::
    $ gsisp -s SESSION_NAME -C 120 [parameters input folder] --sisp
    [location of `sisp` binary]

* [parameters input folder] is a root folder containing all
  paramters.in input files, one for each subfolder.
  A valid input subfolder *must* contain a parameter file named ``parameters.in``.

``gsisp`` has the following extra options:
	      
   -S PATH, --sisp PATH  Location of the sisp binary file.

Running ``gsisp``
=================

## Step 1: Activate ScienceCloud API authentication
    $ source ./sc-authenticate.sh

Provide UZH Webpass shortname and password at prompt as requested.

Note: location of the authenticate.sh script may change depending on
local setup.

## Step 2: Execute ``gsisp`` in `screen`
Example:
    $ screen -L `which python` ../gsisp.py inputs/ --sisp bin/sisp -s test -vvvv -o
    results -C 10

From the provided example, one could customize:
* location of the input parameters: instead of using `inputs` folder, provide the
full path of th ealternative folder containing all input parameters.in
files (remember that each input paramter file must be placed in its
own sub-folder).
* location of sisp binary: instead of using `bin/sisp`, provide the
full path of your own sisp binary (Note: sisp must be statically linked).
* Location of result folder: instead of `results` provide the
alternative path to the result folder (where all the results will be stored)
Note: output files will be automatically places in the ``output`` folder of
the corresponding ``input`` folder.

## Step 3: Detach from running `screen` session 

Once ``gsisp`` runs (and it has been launched with the `-C`
option, it will continuously supervise the execution of all provided
input parameters until completion. In order to properly detach from
the running session and let the ``gsisp`` run in background, it
is necessary to detach from the running `screen` session.

* How to detach form a running `screen` session
    $ Ctrl-A Ctrl-D

## Step 4: periodically check the progress of the ``gsisp``
   execution

Either re-attach to the running `screen` session by:
    $ screen -r
Note: remember to detach from the running screen session using the:
    ``$ Ctrl-A Ctrl-D`` command

...or check the content of the `screen` log file:
    $ less screen.log

Temporally stop a running ``gsisp`` session
================================================

Re-attach to the running `screen` session
    $ screen -r

and stop the execution by:
    $ Ctrl-C

This will interrupt the execution of ``gsisp`` but will **not**
terminate the instances running on ScienceCloud.

Resume ``gsisp`` execution
==============================
Just re-run the `gsisp` command. It will resume from the point it
was stopped.

Note: Unless you run `gsisp` command with the `-N` option. In that
case it will start a new session and loose all previous executions.

Terminating the instances on ScienceCloud started by ``gsisp``
===================================================================

$ gsession terminate <session path>
