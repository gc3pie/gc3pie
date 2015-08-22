The example input folder is not available for testing as it contains information that the
end-user has not released.

Execution requirements
======================

At the remote end, it is required to have R (r-base) package
and the R ``runjags`` library installed.

_Note_: On the Hobbes could infrastructure, a dedicated VM with R and
other component installed is available (image_id=_1f4091e0-4161-4457-a9b3-9b133fa5be5f_).


Testing ``gndn``
===================

    gndn.py projectTest -vvvvvvv -s 20150218 -C 120 -o results

The ``gndn`` takes as input argument a comma separated list of input folders.
the input folder contains everythig needed to run a single simulation.
Additionally within the input filder there should be a ``command.txt`` file containing 
the exact command line interface to be invocated on the remote end.

Example of command.txt file:
    Rscript --vanilla src/scripts/fitModel.R

Each ``GndnApplication`` takes the input folder as input argument and returns the content 
of the 'result' folder. The content of the result folder is copied within the result folder of the 
corresponding input folder on the client node (where the ``gndn`` command has been issued).

Invocation of ``gndn`` follows the usual session-based script
conventions::

    gndn.py -s TEST_SESSION_NAME -C 120 -vvv
    <Path_to_local_project_folder(s)> -o ./out

``out/<project_folder_name>`` will contain the individual Application's
subfolders with logs information (``gndn.log``)

