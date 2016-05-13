The example input folder is not available for testing as it contains information that the
end-user has not released.

Execution requirements
======================

At the remote end, it is required to have R (r-base) package
and all necessary libraries installed.

_Note_: On the science could infrastructure, a dedicated VM with R and
other component installed is available (image_id=TODO: get Image ID).


Testing ``gtree``
===================

    gtree.py -N 100 dataset -vvvvvvv -o results

The ``gtree`` takes as input argument the path to the data set and the number of
nodes that are wanted to run the simluation in parallel.


Invocation of ``gtree`` follows the usual session-based script
conventions::

    gtree.py -s TEST_SESSION_NAME -C 120 -vvv
    -N 10 -o ./out

``out/<node_number>`` will contain the individual Application's
subfolders with logs information (``gtree.log``)
