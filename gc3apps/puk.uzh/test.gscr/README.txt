The ``gscr`` application allows to run ``evaluate_DCM``
Matlab function over a different set of parameters and data.

Testing ``gscr``
================

Invocation of ``gscr`` follows the usual session-based script
conventions::
    gscr -s SESSION_NAME -C 120 [param_folder] [data_folder]

 * [param_folder] is the list of all parameter files that should correspond
   to the data in the data folder
 * [data_folder] is the list of data files

``gscr`` has the following extra options:

	      
    -b [STRING], --binary [STRING]
                        Location of the Matlab script that implements the
			`evaluate_DCM` function. Default: None.

Using gscr
================

To launch al the simulations, just specify the param folder and
the data folder; alternatively the own `evaluate_DCM` function could be specified
using the `-b` option:

    ../gscr.py param/ data/ -b bin/evaluate_DCM.m -C 60 -o results -S aggregated



