Preparation of the VM image to run the "Tricellular Junction" code
------------------------------------------------------------------

#. Start with a Ubuntu base VM image.

#. Install the MatLab Compiler Runtime in ``/usr/local/MATLAB``

#. Install GC3Pie in directory ``~/gc3pie`` (default for GC3Pie's
   ``install.sh`` script).

#. Add the following to `~/.bashrc`::

        # user's own binary directory
        if test -d $HOME/bin; then
            PATH=$HOME/bin:$PATH
        fi

        # enable GC3Pie
        if test -r $HOME/gc3pie/bin/activate; then
            . $HOME/gc3pie/bin/activate
        fi

        # run MatLab compiled code without loading any env
        mcr_root=/usr/local/MATLAB/R2012a
        LD_LIBRARY_PATH=.:${mcr_root}/runtime/glnxa64
        LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${mcr_root}/bin/glnxa64
        LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${mcr_root}/sys/os/glnxa64
        mcr_jre=${mcr_root}/sys/java/jre/glnxa64/jre/lib/amd64
        LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${mcr_jre}/native_threads
        LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${mcr_jre}/server
        LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${mcr_jre}/client
        LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${mcr_jre}
        export LD_LIBRARY_PATH
        XAPPLRESDIR=${mcr_root}/X11/app-defaults
        export XAPPLRESDIR

#. Store the "Tricellular Junction" source code into ``~/tricellular_junction``

#. Compile the "Tricellular Junction" MatLab code::

        cd ~/tricellular_junction
        mkdir -pv bin
        mcc -m -d bin -o tricellular_junctions -R '-nojvm,-nodisplay' -v main.m

#. Make symlinks so that "Tricellular Junction" can be run without any
   PATH-tweaking::

        mkdir -pv ~/bin
        ln -sv ../tricellular_junctions/bin/tricellular_junctions ~/bin/

#. Check that you can run it; the following should start the
   simulation, which you can stop after ~2000 steps::

        tricellular_junction -2 0 0
