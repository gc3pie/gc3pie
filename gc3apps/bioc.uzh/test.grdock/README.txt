The ``grdock`` application allows to run rDock over a list of ligand
files (in .sd format)

Testing ``grdock``
======================

Invocation of ``grdock`` follows the usual session-based script
conventions::
    grdock -s SESSION_NAME -C 120 [ligands]

* [ligands] is a list of ligand files (in .sd format). It can be
expressed in either of the following formats:
    * Comma separated list of .sd files
 
    Example: grdock Docking10.sd, Docking12.sd, Docking1.sd...

    * folder name containing all ligand files to be processed

    Example: grdocl in/
    Where: #ls in/
    Docking10.sd  Docking12.sd  Docking1.sd  Docking2.sd...

``grdock`` has the following extra options:

	      
    -i [NUM],    --iterations [NUM]
                              Number of iterations for rbdock. Default: 20
    -d [STRING], --data [STRING]
                        Path to data folder (e.g. where crebbp-without-water-
                        Tripos.mol2, Pseudo-Ligand-in-pose.sd, Water-in-
                        3P1C.pdb and/or Docking.sd, could be
			retrieved. Default: grdock does not
                        search for any `data` folder/

Using grdock
================

To launch al the simulations, just specify the number of models and
the number of repetitions and the binary to be executed:

    grdock.py -i 1 -d data/ in -s 20150226 -C 60 -vvvvvv -o results -N
