The **ggamess** script
======================

GC3Apps provide a script drive execution of multiple ``gamess`` jobs
each of them with a different input file. 
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

The purpose of GAMESS is to execute *several concurrent
runs* of GAMESS each with separate input file. These runs are performed in
parallel using every available GC3Pie parameters.
 


How to run GAMESS on the Grid
-----------------------------

SSH to `ocikbgtw`, then run the command (it's one single command line,
even if it appears broken in several ones in the mail)::

    ggamess.py -A ~/beckya-dmulti.changes.tar.gz -R 2011R3-beckya-dmulti -s "a_session_name" "input_files_or_directories"

The parts in double quotes should be replaced with actual content:

    ``a_session_name``:

        Used for grouping.  This is a word of your choosing (e.g.,
        "`test1`", "`control_group`"), used as a label to tag a group of
        analyses. Multiple concurrent sessions can exist, and they
        won't interfere one with the other.  Again, note that a single
        session can run many different `.inp` files.

    ``input_files_or_directories``:

        This part consists in the path name of `.inp` files or a
        directory containing `.inp` files.  When a directory is
        specified, all the `.inp` files contained in it are submitted
        as GAMESS jobs.

After running, the program will print a short summary of the session
(how many jobs running, how many queued, how many finished).  Each
finished job creates one directory (whose name is equal to the name of
the input file, minus the trailing `.inp`), which contains the `.out`
and `.dat` files.

For shorter typing, I have defined an alias `ggms` to expand to the
above string ``ggamess.py -A ... 2011R3-beckya-dmulti``, so you could
shorten the command to just::

    ggms -s "a_session_name" "input_files_or_directories"

For instance, to use ``ggames.py`` to analyse a single `.inp` file you
must run::

    ggms -s "single" dmulti/inp/neutral/dmulti_cc4l.inp

while to use ``ggamess.py`` to run several GAMESS jobs in parallel::

    ggms -s "multiple" dmulti/inp/neutral

Tweaking execution
------------------

Command-line options (those that start with a dash character '-') can
be used to alter the behavior of the ``ggamess.py`` command:

  ``-A`` `filename.changes.tar.gz`

      This selects the file containing your customized version of
      GAMESS in a format suitable for running in a virtual machine on
      the Grid.  This file should be created following the procedure
      detailed below.

  ``-R`` `version`

      Select a specific version of GAMESS.  This should have been
      installed in the virtual machine within a directory named
      ``gamess-version``; for example, your modified GAMESS is saved in
      directory ``gamess-2011R3-beckya-dmulti`` so the "`version`" string
      is ``2011R3-beckya-dmulti``.

      If you omit the ``-R`` "`version`" part, you get the default GAMESS
      which is presently 2011R1.

  ``-s`` `session` 

      Group jobs in a named session; see above.

  ``-w`` `NUM` 

      Request a running time of at `NUM` hours.  If you omit this part,
      the default is 8 hours.

  ``-m`` `NUM` 

      Request `NUM` Gigabytes of memory for running each job.  GAMESS'
      memory is measured in words, and each word is 8 bytes; add 1 GB
      to the total to be safe :-)


Updating the GAMESS code
------------------------

For this you will need to launch the AppPot virtual machine, which is
done by running the following command at the command prompt on
`ocikbgtw`::

    apppot-start.sh

After a few seconds, you should find yourself at the same
``user@rootstrap`` prompt that you get on your VirtualBox instance, so
you can use the same commands etc.

The only difference of note is that you can exchange files between the
AppPot virtual machine and `ocikbgtw` via the `job` directory (whereas
it's ``/scratch`` in VirtualBox).  So: files you copy into `job` in the
AppPot VM will appear into your home directory on `ocikbgtw`, and
conversely files from your home directory on `ocikbgtw` can be
read/written as if they were into directory `job` in the AppPot VM.

Once you have compiled a new version of GAMESS that you wish to test,
you need to run this command (at the ``user@rootstrap`` command prompt
in the AppPot VM)::

    sudo apppot-snap changes ~/job/beckya-dmulti.changes.tar.gz

This will overwrite the file ``beckya-dmulti.changes.tar.gz`` with the
new GAMESS version.  If you don't want to overwrite it and instead
create another one, just change the filename above (but it *has to*
end with the string ``.changes.tar.gz``), and the use the new name for
the ``-R`` option to ggamess.py

Exit the AppPot VM by typing ``exit`` at the command prompt.
