.. Hey Emacs, this is -*- rst -*-

   This file follows reStructuredText markup syntax; see
   http://docutils.sf.net/rst.html for more information.

.. include:: ../global.inc


.. _environment:

Environment Variables
=====================

The following environmental variables affect GC3Pie operations.

`GC3PIE_CONF`

  Path to an alternate configuration file, that is read upon
  initialization of GC3Pie.  If defined, this file is read *instead*
  of the default ``$HOME/.gc3/gc3pie.conf``; if undefined or empty,
  the usual configuration file is loaded.

`GC3PIE_ID_FILE`

  Path to the a shared state file, used for recording the "next
  available" job ID number.  By default, it is located at
  ``~/.gc3/next_id.txt``:file:.

`GC3PIE_NO_CATCH_ERRORS`

  Comma-separated list of unexpected/generic error patterns upon which GC3Pie
  will not act (by default, ignoring them).  Each of these "unignored" errors
  will be propagated all the way up to top-level.  This facilitates running
  GC3Pie scripts in a debugger and inspecting the code when some unexpected
  error condition happens.

  You can specify which errors to "unignore" by:

  - Error class name (e.g., ``InputFileError``).  Note that this must be the
    *exact* class name of the error: GC3Pie will not walk the error class
    hierarchy for matches.

  - Function/class/module name: all errors handled in the specified
    function/class/module will be propagated to the caller.

  - Additional keywords describing the error. Please have a look at the source
    code for these keywords.

`GC3PIE_RESOURCE_INIT_ERRORS_ARE_FATAL`

  If this environmental variable is set to ``yes`` or ``1``, GC3Pie
  will abort operations immediately if a configured resource cannot be
  initialized.  The default behavior is instead to ignore
  initialization errors and only abort if *no* resources can be
  initialized.
