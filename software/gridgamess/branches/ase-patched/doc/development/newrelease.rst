.. _newrelease:

===========
New release
===========

When it is time for a new release of the code, here is what you have to do:

* Checkout the :ref:`latest_development_release`.

* :ref:`running_tests`.

* Make a tag in svn, using the current version number
  (to make sure **not** to include changes done by other developers
  in the meantime!)::

    svn copy -r 845 https://svn.fysik.dtu.dk/projects/ase/trunk https://svn.fysik.dtu.dk/projects/ase/tags/3.1.0 -m "Version 3.1.0"

  **Note** the resulting tag's revision ``tags_revision``.

* **Checkout** the source, specyfing the version number in the directory name::

   svn co -r tags_revision https://svn.fysik.dtu.dk/projects/ase/tags/3.1.0 ase-3.1.0

* Create the tar file::

   cd ase-3.1.0
   rm -f MANIFEST ase/svnrevision.py*; python setup.py sdist

  Note that the ``tags_revision`` is put into the name of the
  tar file automatically. Make sure that you are getting only
  ``tags_revision`` in the tar file name! Any changes to the source
  will be reflected as a mixed or modified revision tag!

* Put the tar file on web2 (set it read-able for all)::

   scp dist/python-ase-3.1.0."tags_revision".tar.gz root@web2:/var/www/wiki/ase-files

* Add a link to the new release at :ref:`latest_stable_release`.

* Increase the version number in ase/version.py, and commit the change::

    cd ~/ase
    svn ci -m "Version 3.2.0"

  Now the trunk is ready for work on the new version.

* Send announcement email to the ``ase-users`` mailing list (see :ref:`mailing_lists`).
