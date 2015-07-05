#####################################################################
# WARNING 
# This is an experimental verstion. It is meant for testing a
# web scenario deployment. It will be incorporated into the running
# ``ggeosphere`` Application once calrified the full usecase.
#####################################################################

Rus ``ggeosphere_web.py`` as daemon. It is usefull when integrating it into a
web interface. ``ggeosphere_web.py`` is the GC3Pie-based python script
that periodically checks the content of an S3 Object Store, and
launches a new geosphere simulation for each new uploaded model.
``ggeosphere_web.py`` behaves as a regular SessionBased script that
supervises the execution of the submitted jobs, retrieves results and
re-submit failed jobs if needed.

``ggeosphere_web.py`` takes a configuration file as option. Example of
configuration file is available at 'etc/a4meshcfg'.

Example how to run ``ggeosphere_web.py``:

 $ python ../ggeosphere_web.py -c etc/a4mesh.cfg -C 60

Unlike a regular SessionBasedScript, ``ggeosphere_web.py`` does not
terminate when all Applications have been processed.

``ggeosphere_web_daemon.sh`` is the init scriot to launch and control
the ``ggesphere_web.py`` as a daemon.

Example how to launch ``ggeosphere_web.py`` as daemon:
 
 $ ggeosphere_web_daemon.sh start

Usage: ggeosphere_web_daemon.sh {start|stop|status|restart}

