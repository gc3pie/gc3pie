#####################################################################
# WARNING 
# This is an experimental version. It is meant for testing a
# web scenario deployment. It will be incorporated into the running
# ``ggeosphere`` Application once clarified the full usecase.
#####################################################################

``ggeosphere.py`` allows to run HydrogeoSphere simulations
(HGS).
Is has two main working modes:
a. CLI (standard SessionBasedScript)
b. Daemon mode (for web integration)

Run ``ggeosphere.py`` in CLI mode
=================================

``ggeosphere.py`` takes as input a folder containing HGS input models
to be analysed. Input folder can be expressed in URI form (for list of
supported protocols please run::

$ ``ggeosphere.py`` -h 

For each of the found input models (each .tgz or .zip file),
``ggeosphere.py`` creates an instance of GgeosphereApplication.
The SessionBasedScript then works as usual: it submits the
Applications to the valid resources, it supervises their execution
and, if necessary, retrieves their output.

Alternatively to the input arguments and options, one can specifiy a
configuration file through the ``-c`` option.
The configuration file contains the same type of information that
could be passed as input arguments and options.
An exampl of the configuration file could be found in
``./etc/a4mesh.cfg``.

Command line arguments override the configuration file.

Example how to run ``ggeosphere.py``:

$ ggeosphere.py -c ./etc/a4mesh.cfg -C 60

Run ``ggeosphere.py`` in Daemon mode
====================================

if the option ``-d | --is-daemon`` is specified. 
The ``ggeosphere.py`` scripts runs continuously checking at the end of
its ``progress`` cycle, the presence of new input files to be analysed
in the specified input folder. For the rest it behaves as a regular
SessionBased script that supervises the execution of the submitted
jobs, retrieves results and re-submit failed jobs if needed.

This mode is usefull for web integration. An example of an
``/etc/init.d/ggeosphere`` init script can be found in
``./etc/init.d/ggeosphere``.

Example how to launch ``ggeosphere_web.py`` as daemon:
 
 $ service ggeosphere start

Usage: service ggeosphere {start|stop|status|restart}
