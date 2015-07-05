The ``input/`` directory contains sample data provided in fastq format.

Testing ``gpryrad``
===================

``gpyrad`` takes as input argument a folder reference. It will scan
the folder for files with *.fastq extension.
Alternatively, an S3 URL can be passed where the fastq files have been
previously uploaded.
For the S3 variant, gpyrad makes the assumption ``s3cmd`` is available
and properly configured to access the referenced S3 object store.

Therefore, to run a test of ``gpyrad`` it is sufficient to provide
the path to a directory containing valid input files.

Invocation of ``gpyrad`` follows the usual session-based script
conventions; in addition, one can pass an alterantive params.txt file
used to instruct pyRAD; this can be provided with the ``-p`` option.
Another options that can be specified is the clustering threshold
value (a decimal); this can be specified with the -W option::

    gpyrad.py -s TEST_SESSION_NAME -C 120 -vvv -p ./params.txt -W
    0.91 input/ 

The test data should generate three gpryad jobs::

    Status of jobs in the '...' session: (at 13:08:08, 02/28/14)
            NEW   0/3    (0.0%)  
        RUNNING   0/3    (0.0%)  
        STOPPED   0/3    (0.0%)  
      SUBMITTED   3/3   (100.0%) 
     TERMINATED   0/3    (0.0%)  
    TERMINATING   0/3    (0.0%)  
          total   3/3   (100.0%) 

The processing ends when TERMINATED has reached 100% jobs.

When all the jobs are done, the output *directory* will contain
the output folders, named ``clust.[-W value if passed, 0.9 as default]``.

.. note::

   If using Hobbes (the UZH cloud infrastructure) the reference
   uzh.systbot.pyrad image prepared for this exercise has the
   following AMI id: ``ami-000000d5``.

   If S3 URL is used (either as input or output reference) the
   additional option ``-Y`` has to be passed indicating the location
   of the s3cfg file containing the access details for the S3 Object
   Store. On the execution node, s3cmd will have to be already
   deployed (this is the default if using the ``uzh.systbot``
   appliances).

   It is also possible to pass an S3 url as output reference. In such a
   case the output folders will be transferred to the corresponding S3
   Object Store.
