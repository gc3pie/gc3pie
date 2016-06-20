The ``data/src`` directory contains the current Matlab scripts
required for the execution of the ``gtopology.py`` program.

.. Main application developers: "Alex Grimm"

Requirements for running ``gtopology``
======================================

`gtopology` requires as input argument an .csv file containing the
list of all the parameters that will have to be passed to the main
python function located in the input `source` folder.
As default, the main function searched will be:
`run_topologies_cluster.py`. Another main function name could be set
using the `-M` option.

Running ``gtopology``
======================

## Step 0: prepare the execution environment

Note: the following is an example on how to run any GC3Pie script on
ScienceCloud using one of the provided S3IT toolbox images. If you are
running GC3Pie from your own local workstation, you need to:
* intall GC3Pie following the instructions provided at:
    http://gc3pie.readthedocs.io/en/master/users/install.html#non-standard-installation-options
Please choose the `non-standard` installation option and install
GC3Pie using the `--develop` option.
* Download the ScienceCloud API script: follow the instructions
provided at:
    https://s3itwiki.uzh.ch/display/clouddoc/FAQ#FAQ-rcHowdoIdownloadtheScienceCloudRCfileforcommandlineclientsandAPIauthentication?

## Step 1: Activate ScienceCloud API authentication
    $ source .virtualenvs/s3it/bin/sc-authenticate.sh

Provide UZH Webpass shortname and password at prompt as requested.

Note: if you have downloaded the ScienceCloud API by yourself, the
authentication script will be named: `<your-project>-openrc.sh`

## Step 2: Execute gtopology in `screen`
Example:
    $ screen -L `which gtopology` input.csv sources/ -k 10 -C 30 -s 20151104 -o data/out -N

From the provided example, one could customize:
* location of input.csv file: instead of `input.csv`, provide the
full path of an alternative input .csv file
* location of Python scripts (where main function is located):
instead of using `sources`, provide the full path of an alternative
folder where the main python function and its related scripts are
located.
* Location of result folder: instead of `data/out` provide the
alternative path to the result folder (where all the results will be stored)
* If a different `main function` name is to be used, then one need to
specify it with the `-M` option.
Example:
    $ screen -L `which gtopology` ... -M main.py ...

Other GC3Pie specific options that can be customized:
* session name: the value passed to the `-s` option (20151104 in the
example)

Note: the `-N` option removes all previous executions. Use it **only**
when you need to start a new simulation and forget about previous
ones.

## Step 3: Detach from running `screen` session 

Once ``gtopology`` runs (and it has been launched with the `-C`
option, it will continuously supervise the execution of all provided
input parameters until completion. In order to properly detach from
the running session and let the ``gtopology`` run in background, it
is necessary to detach from the running `screen` session.

* How to detach form a running `screen` session
    $ Ctrl-A Ctrl-D

## Step 4: periodically check the progress of the ``gtopology``
   execution

Either re-attach to the running `screen` session by:
    $ screen -r
Note: remember to detach from the running screen session using the:
    ``$ Ctrl-A Ctrl-D`` command

...or check the content of the `screen` log file:
    $ less screen.log

Temporally stop a running ``gtopology`` session
================================================

Re-attach to the running `screen` session
    $ screen -r

and stop the execution by:
    $ Ctrl-C

This will interrupt the execution of ``gtopology`` but will **not**
terminate the instances running on ScienceCloud.

Resume ``gtopology`` execution
==============================
Just re-run the `gtopology` command. It will resume from the point it
was stopped.

Note: Unless you run `gtopology` command with the `-N` option. In that
case it will start a new session and loose all previous executions.

Terminating the instances on ScienceCloud started by ``gtopology``
===================================================================

$ gsession terminate <session path>





