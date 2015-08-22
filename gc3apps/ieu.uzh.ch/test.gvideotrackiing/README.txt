The ``gvideotrack`` application allows to run ``ParticleLinker``
R function over a different set of trajectory files.

python /home/sergio/dev/gc3pie/trunk/gc3pie/gc3apps/ieu.uzh.ch/gvideotrack.py data/in/ -R ./data/src/s3it_articleLinker.R -j data/src/ParticleLinker.jar -r localhost -C 3 -s 20141118 -vvvvv -m 512MB -o results/

Testing ``gvideotrack``
======================

Invocation of ``gvideotrack`` follows the usual session-based script
conventions::
    gvideotrack -s SESSION_NAME -C 120 [trajectory_files_folder]

* [trajectory_files_folder] is a fodler name containing all the trajectory
  files that need to be processed. Each valid trajectory file has to have 
  a compliant file extension ``.ijout.txt``.

``gvideotrack`` has the following extra options:

	      
    -R [STRING], --Rscript [STRING]
                        Location of the R script that implements the
                        'link_particles' function.
    -j [STRING], --jarfile [STRING]
                        Location of the 'ParticleLinker.jar'.

Using gvideotrack
================

To launch al the simulations, just specify the location of the trajectory files,
additionally the ParticleLinker R script as well as the ParticleLinker jar file.

    gvideotrack data/in -R data/src/ParticleLinker.R -j data/src/ParticleLinker.jar 
    -C 60 -o data/out




