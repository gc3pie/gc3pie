.. Hey Emacs, this is -*- rst -*-

   This file follows reStructuredText markup syntax; see
   http://docutils.sf.net/rst.html for more information.

.. include:: ../global.inc


.. _configuration:

Configuration File
==================


Location
--------

All commands in `GC3Apps`:ref: and `GC3Utils`:ref: read a few
configuration files at startup:

  * system-wide one located at :file:`/etc/gc3/gc3pie.conf`,
  * a virtual-environment-wide configuration located at
    :file:`$VIRTUAL_ENV/etc/gc3/gc3pie.conf`, and
  * a user-private one at :file:`$HOME/.gc3/gc3pie.conf`, or,
    alternately, the file in the location pointed to by the
    environmental variable ``GC3PIE_CONF``.

All these files are optional, but at least one of them must exist.

All files use the same format. The system-wide one is read first, so
that users can override the system-level configuration in their private file.
Configuration data from corresponding sections in the
configuration files is merged; the value in files read later
overrides the one from the earler-read configuration.

If you try to start any GC3Utils command without having a
configuration file, a sample one will be copied to the user-private
location :file:`~/.gc3/gc3pie.conf` and an error message will be
displayed, directing you to edit the sample file before retrying.


Configuration file format
-------------------------

The GC3Pie configuration file follows the format understood by
`Python ConfigParser <http://docs.python.org/library/configparser.html>`_;
see http://docs.python.org/library/configparser.html for reference.

Here is an example of what the configuration file looks like::

  [auth/none]
  type=none

  [resource/localhost]
  # change the following to `enabled=no` to quickly disable
  enabled=yes
  type=shellcmd
  transport=local
  auth=none
  max_cores=2
  max_cores_per_job=2
  # ...

You can see that:
  
* The GC3Pie configuration file consists of several configuration
  sections.  Each configuration section starts with a keyword in
  square brackets and continues until the start or the next section or
  the end of the file (whichever happens first).

* A section's body consists of a series of ``word=value`` assignments
  (we call these *configuration items*), each on a line by its own.
  The word before the ``=`` sign is called the *configuration key*,
  and the value given to it is the *configuration value*.

* Lines starting with the ``#`` character are comments: the line is
  meant for human readers and is completely ignored by GC3Pie.


The following sections are used by the GC3Apps/GC3Utils programs:

  - ``[DEFAULT]`` -- this is for global settings.
  - `[auth/{name}]`:file: -- these are for settings related to identity/authentication (identifying yourself to clusters & grids).
  - `[resource/{name}]`:file: -- these are for settings related to a specific computing resource (cluster, grid, etc.)

Sections with other names are allowed but will be ignored.


The ``DEFAULT`` section
-----------------------

The ``[DEFAULT]`` section is optional.

Values defined in the ``[DEFAULT]`` section can be used to insert
values in other sections, using the ``%(name)s`` syntax.  See
documentation of the `Python SafeConfigParser
<http://docs.python.org/library/configparser.html>`_ object at
http://docs.python.org/library/configparser.html for an example.


``auth`` sections
-----------------

There can be more than one ``[auth]`` section.

Each authentication section must begin with a line of the form:

    `[auth/{name}]`:file:

where the `{name}`:file: portion is any alphanumeric string.

You can have as many `[auth/{name}]`:file: sections as you want; any name is
allowed provided it's composed only of letters, numbers and the underscore
character ``_``. (Examples of valid names are: ``[auth/cloud]``,
``[auth/ssh1]``, and ``[auth/user_name]``)

This allows you to define different auth methods for different resources. Each
`[resource/{name}]`:file: section can reference one (and one only)
authentication section, but the same `[auth/{name}]`:file: section can be used
by more than one `[resource/{name}]`:file: section.


Authentication types
~~~~~~~~~~~~~~~~~~~~

Each ``auth`` section *must* specify a ``type`` setting.

``type`` defines the authentication type that will be used to access
a resource. There are three supported authentication types:

================ =============================================================
``type=...``     Use this for ...
================ =============================================================
``ec2``          EC2-compatible cloud resources
---------------- -------------------------------------------------------------
``none``         Resources that need no authentication
---------------- -------------------------------------------------------------
``ssh``          Resources that will be accessed by opening an SSH connection
                 to the front-end node of a cluster
================ =============================================================


``none``-type authentication
++++++++++++++++++++++++++++

This is for resources that actually need no authentication (``transport=local``)
but still need to reference an ``[auth/*]`` section for syntactical reasons.

GC3Pie automatically inserts in every configuration file a section
``[auth/none]``, which you can reference in resource sections with the line
``auth=none``.

Because of the automatically-generated ``[auth/none]``, there is hardly ever a
reason to explicitly write such a section (doing so is not an error, though)::

  [auth/none]
  type=none


``ssh``-type authentication
+++++++++++++++++++++++++++

For the ``ssh``-type auth, the following keys must be provided:

  * ``type``: must be ``ssh``
  * ``username``: must be the username to log in as on the remote machine

The following configuration keys are instead optional:

  * ``port``: TCP port number where the SSH server is listening.  The
    default value 22 is fine for almost all cases; change it only if
    you know what you are doing.
  * ``keyfile``: path to the (private) key file to use for SSH public
    key authentication.
  * ``ssh_config``: path to a SSH configuration file, where to read
    additional options from (default: ``$HOME/ssh/config``:file:.  The
    format of the SSH configuration file is documented in the
    `ssh_config(5)`__ man page.
  * ``timeout``: maximum amount of time (in seconds) that GC3Pie will
    wait for the SSH connection to be established.

.. __: http://www.openbsd.org/cgi-bin/man.cgi/OpenBSD-current/man5/ssh_config.5?query=ssh_config&sec=5

.. note::

  We advise you to use the SSH config file for setting port, key file,
  and connection timeout.  Options ``port``, ``keyfile``, and
  ``timeout`` could be deprecated in future releases.

*Example.* The following configuration sections are used to set up
two different accounts that GC3Pie programs can use.  Which account
should be used on which computational resource is defined in the
`resource sections`_ (see below). ::

    [auth/ssh1]
    type = ssh
    username = murri # your username here

    [auth/ssh2] # I use a different account name on some resources
    type = ssh
    username = rmurri
    # read additional options from this SSH config file
    ssh_config = ~/.ssh/alt-config


``ec2``-type authentication
+++++++++++++++++++++++++++

For the ``ec2``-type auth, the following keys *can* be provided:

  * ``ec2_access_key``: Your personal access key to authenticate against the
    specific cloud endpoint. If not found, the value of the environment variable
    ``EC2_ACCESS_KEY`` will be used; if the environment variable is unset,
    GC3Pie will raise a ``ConfigurationError``.

  * ``ec2_secret_key``: Your personal secret key associated with the above
    ``ec2_access_key``. If not found, the value of the environment variable
    ``EC2_SECRET_KEY`` will be used; if the environment variable is unset,
    GC3Pie will raise a ``ConfigurationError``.

Any other key/value pair will be silently ignored.

*Example.* The following configuration section is used to access an
EC2-compatible resource (access and secret keys are of course invalid)::

    [auth/hobbes]
    type=ec2
    ec2_access_key=1234567890qwertyuiopasdfghjklzxc
    ec2_secret_key=cxzlkjhgfdsapoiuytrewq0987654321


``resource`` sections
---------------------

Each resource section must begin with a line of the form:

    `[resource/{name}]`:file:

You can have as many :file:`[resource/{name}]` sections as you want; this
allows you to define many different resources.  Each `[resource/{name}]`:file:
section must reference one (and one only) `[auth/{name}]`:file:
section (by its ``auth`` key).

Resources currently come in several flavours, distinguished by the value of the
``type`` key. Valid values for the ``type=...`` configuration line are listed in
the table below.

================ =============================================================
``type=...``     The resource is ...
================ =============================================================
``ec2+shellcmd`` a cloud with EC2-compatible APIs:
                 applications are run on Virtual Machines started on the cloud
---------------- -------------------------------------------------------------
``lsf``          an `LSF`_ batch-queuing system
---------------- -------------------------------------------------------------
``pbs``          a `TORQUE`_ or `PBSPro`_ batch-queuing system
---------------- -------------------------------------------------------------
``sge``          a `Grid Engine`_ batch-queuing system
---------------- -------------------------------------------------------------
``shellcmd``     a single Linux or MacOSX computer:
                 applications are executed by spawning a local UNIX process
---------------- -------------------------------------------------------------
``slurm``        a `SLURM`_ batch-queuing system
================ =============================================================


Link to ``[auth/...]`` sections
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All `[resource/{name}]`:file: sections *must* reference a valid
`[auth/{aname}]`:file: section via the `auth={aname}`:file: line. If the
``auth=...`` line is omitted, GC3Pie's default is ``auth=none``.

Type of the resource and the referenced ``[auth/...]`` section must match:

* Resources of type ``ec2+shellcmd`` can only reference ``[auth/...]`` sections
  of type ``ec2``.
* Batch-queuing resources (type is one of ``sge``, ``pbs``, ``lsf``, or
  ``slurm``) and resources of type ``shellcmd`` can reference ``[auth/...]``
  sections of type ``ssh`` (when ``transport=ssh``) or ``[auth/none]`` (when
  ``transport=local``).


Configuration keys common to *all* resource types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following configuration keys are commmon to all resources, regardless of type.

======================= =======================================================
Configuration key       Meaning
======================= =======================================================
``type``                Resource type, see above.
----------------------- -------------------------------------------------------
``auth``                Name of a valid `[auth/{name}]`:file: section;
                        only the authentication section name (after the ``/``)
                        must be specified.
----------------------- -------------------------------------------------------
``max_cores_per_job``   Maximum number of CPU cores that a job can request;
                        a resource will be dropped during the brokering process
                        if a task requests more cores than this.
----------------------- -------------------------------------------------------
``max_memory_per_core`` Maximum amount of memory that a task can request;
                        a resource will be dropped during the brokering process
                        if a task requests more memory than this.
----------------------- -------------------------------------------------------
``max_walltime``        Maximum job running time.
----------------------- -------------------------------------------------------
``max_cores``           Total number of cores provided by the resource.
----------------------- -------------------------------------------------------
``architecture``        Processor architecture.  Should be one of the strings
                        ``x86_64`` (for 64-bit Intel/AMD/VIA processors),
                        ``i686`` (for 32-bit Intel/AMD/VIA x86 processors),
                        or ``x86_64,i686`` if both architectures are available
                        on the resource.
----------------------- -------------------------------------------------------
``time_cmd``            Path to the GNU `time`:command: program.
                        Default is ``/usr/bin/time``
======================= =======================================================


Configuration keys common to batch-queuing resource types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following configuration keys can be used in any resource of type ``pbs``,
``lsf``, ``sge``, or ``slurm``.

  * ``prologue``: Path to a script file, whose contents are *inserted* into the
    submission script of each application that runs on the resource. Commands
    from the *prologue* script are executed before the real application; the
    *prologue* is intended to execute some shell commands needed to setup the
    execution environment before running the application (e.g. running a
    ``module load ...`` command).

    .. note::

       The prologue script **must** be a valid plain `/bin/sh`:command: script;
       `she-bang`_ indications will *not* be honored.

       .. _`she-bang`: https://en.wikipedia.org/wiki/Shebang_(Unix)

  * `{application}_prologue`:file:: Same as ``prologue``, but it is
    used only when `{application}`:file: matches the name of the
    application (as specified by the ``application_name`` attribute on the
    GC3Pie `Application`:class: instance).

  * ``prologue_content``: A (possibly multi-line) string that will be
    *inserted* into the submission script and executed before the
    real application.  Like the ``prologue`` script, commands must be given
    using `/bin/sh`:command: syntax.

  * `{application}_prologue_content`:file:: Same as ``prologue_content``, but
    it is used only when `{application}`:file: matches the name of the
    application (as specified by the ``application_name`` attribute on the
    GC3Pie `Application`:class: instance).

.. warning::

   Errors in a prologue script will prevent any application from running
   on the resource!  Keep prologue commands to a minimum and always check
   their correctness.

If several *prologue*-related options are specified, then commands are inserted
into the submission script in the following order:

- first content of the ``prologue`` script,
- then content of the `{application}_prologue`:file: script,
- then commands from the ``prologue_content`` configuration item,
- finally commands from the `{application}_prologue_content`:file: configuration item.

A similar set of options allow defining commands to be executed *after* an
application has finished running:

  * ``epilogue``: The content of the `epilogue` script will be *inserted* into
    the submission script and is executed after the real application has
    been submitted

    .. note::

       The epilogue script **must** be a valid plain `/bin/sh`:command: script;
       `she-bang`_ indications will *not* be honored.

       .. _`she-bang`: https://en.wikipedia.org/wiki/Shebang_(Unix)

  * `{application}_epilogue`:file: : Same as ``epilogue``, but used only
    when ``{application}`:file: matches the name of the application (as
    specified by the ``application_name`` attribute on the GC3Pie
    `Application`:class: instance).

  * ``epilogue_content``: A (possibly multi-line) string that will be *inserted*
    into the submission script and executed after the real application has
    completed. Like the ``epilogue`` script, commands must be given using
    `/bin/sh`:command: syntax.

  * `{application}_epilogue_content`:file: : Same as ``epilogue_content``, but
    used only when `{application}`:file: matches the name of the application (as
    specified by the ``application_name`` attribute on the GC3Pie
    `Application`:class: instance).

.. warning::

   Errors in an epilogue script prevent GC3Pie from reaping the application's
   exit status. In particular, errors in the epilogue commands can make GC3Pie
   consider the whole application as failed, and use the epilogue's error exit
   as the overall exit code.

If several *epilogue*-related options are specified, then commands are inserted
into the submission script in the following order:

- first contents of the ``epilogue`` script,
- then contents of the `{application}_epilogue`:file: script,
- then commands from the ``epilogue_content`` configuration item,
- finally commands from the `{application}_epilogue_content`:file: configuration item.


``sge`` resources (all batch systems of the `Grid Engine`_ family)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following configuration keys are required in a ``sge``-type resource section:

  * ``frontend``: should contain the `FQDN (Fully-qualified domain name)`:abbr: of the SGE front-end node. An SSH connection will be attempted to this node, in order to submit jobs and retrieve status info.
  * ``transport``: Possible values are: ``ssh`` or ``local``.   If ``ssh``, GC3Pie tries to connect to the host specified in ``frontend`` via SSH in order to execute SGE commands.  If ``local``, the SGE commands are run directly on the machine where GC3Pie is installed.

To submit parallel jobs to SGE, a "parallel environment" name must be
specified.  You can specify the PE to be used with a specific
application using a configuration parameter *application name* +
``_pe`` (e.g., ``gamess_pe``, ``zods_pe``); the ``default_pe``
parameter dictates the parallel environment to use if no
application-specific one is defined.  *If neither the
application-specific, nor the ``default_pe`` parallel environments are
defined, then submission of parallel jobs will fail.*

When a job has finished, the SGE batch system does not (by default)
immediately write its information into the accounting database.  This
creates a time window during which no information is reported about
the job by SGE, as if it never existed.  In order not to mistake this
for a "job lost" error, GC3Libs allow a "grace time": `qacct`:command:
job information lookups are allowed to fail for a certain time span
after the first time `qstat`:command: failed. The duration of this
time span is set with the ``sge_accounting_delay`` parameter, whose
default is 15 seconds (matches the default in SGE, as of release 6.2):

  * ``sge_accounting_delay``: Time (in seconds) a failure in `qacct`:command: will *not* be considered critical.

GC3Pie uses standard command line utilities to interact with the
resource manager. By default these commands are searched using the
``PATH`` environment variable, but you can specify the full path of
these commands and/or add some extra options. The following options
are used by the SGE backend:

  * ``qsub``: submit a job.

  * ``qacct``: get info on resources used by a job.

  * ``qdel``: cancel a job.

  * ``qstat``: get the status of a job or the status of available
    resources.

If ``transport`` is ``ssh``, then the following options are also read
and take precedence above the corresponding options set in the "auth"
section:

  * ``port``: TCP port number where the SSH server is listening.
  * ``keyfile``: path to the (private) key file to use for SSH public
    key authentication.
  * ``ssh_config``: path to a SSH configuration file, where to read
    additional options from.  The format of the SSH configuration file
    is documented in the `ssh_config(5)`__ man page.
  * ``ssh_timeout``: maximum amount of time (in seconds) that GC3Pie will
    wait for the SSH connection to be established.

.. __: http://www.openbsd.org/cgi-bin/man.cgi/OpenBSD-current/man5/ssh_config.5?query=ssh_config&sec=5

.. note::

  We advise you to use the SSH config file for setting port, key file,
  and connection timeout.  Options ``port``, ``keyfile``, and
  ``timeout`` could be deprecated in future releases.


``pbs`` resources (TORQUE_ and PBSPro_ batch-queueing systems)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following configuration keys are required in a ``pbs``-type resource section:

  * ``transport``: Possible values are: ``ssh`` or ``local``.  If
    ``ssh``, GC3Pie tries to connect to the host specified in ``frontend``
    via SSH in order to execute Troque/PBS commands.  If ``local``,
    the TORQUE/PBSPro commands are run directly on the machine where
    GC3Pie is installed.

  * ``frontend``: should contain the `FQDN (Fully-qualified domain
    name)`:abbr: of the TORQUE/PBSPro front-end node. This configuration
    item is only relevant if ``transport`` is ``local``. An SSH
    connection will be attempted to this node, in order to submit jobs
    and retrieve status info.

GC3Pie uses standard command line utilities to interact with the
resource manager. By default these commands are searched using the
``PATH`` environment variable, but you can specify the full path of
these commands and/or add some extra options. The following options
are used by the PBS backend:

  * ``queue``: the name of the queue to which jobs are submitted. If
    empty (the default), no queue will be specified during submission,
    using the resource manager's default.

  * ``qsub``: submit a job.

  * ``qdel``: cancel a job.

  * ``qstat``: get the status of a job or the status of available
    resources.

  * ``tracejob``: get info on resources used by a job.

If ``transport`` is ``ssh``, then the following options are also read
and take precedence above the corresponding options set in the "auth"
section:

  * ``port``: TCP port number where the SSH server is listening.
  * ``keyfile``: path to the (private) key file to use for SSH public
    key authentication.
  * ``ssh_config``: path to a SSH configuration file, where to read
    additional options from.  The format of the SSH configuration file
    is documented in the `ssh_config(5)`__ man page.
  * ``ssh_timeout``: maximum amount of time (in seconds) that GC3Pie will
    wait for the SSH connection to be established.

.. __: http://www.openbsd.org/cgi-bin/man.cgi/OpenBSD-current/man5/ssh_config.5?query=ssh_config&sec=5

.. note::

  We advise you to use the SSH config file for setting port, key file,
  and connection timeout.  Options ``port``, ``keyfile``, and
  ``timeout`` could be deprecated in future releases.


``lsf`` resources (IBM LSF)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following configuration keys are required in a ``lsf``-type resource section:

  * ``transport``: Possible values are: ``ssh`` or ``local``.  If
    ``ssh``, GC3Pie tries to connect to the host specified in ``frontend``
    via SSH in order to execute LSF commands.  If ``local``, the LSF
    commands are run directly on the machine where GC3Pie is
    installed.

  * ``frontend``: should contain the `FQDN (Fully-qualified domain
    name)`:abbr: of the LSF front-end node. This configuration item is
    only relevant if ``transport`` is ``local``. An SSH connection
    will be attempted to this node, in order to submit jobs and
    retrieve status info.

GC3Pie uses standard command line utilities to interact with the
resource manager. By default these commands are searched using the
``PATH`` environment variable, but you can specify the full path of
these commands and/or add some extra options. The following options
are used by the LSF backend:

  * ``bsub``: submit a job.

  * ``bjobs``: get the status and resource usage of a job.

  * ``bkill``: cancel a job.

  * ``lshosts``: get info on available resources.

LSF commands use a weird formatting: lines longer than 79 characters
are wrapped around, and the continuation line starts with a long run
of spaces.  The length of this run of whitespace seems to vary with
LSF version; GC3Pie is normally able to auto-detect it, but there can
be a few unlikely cases where it cannot.  If this ever happens, the
following configuration option is here to help:

  * ``lsf_continuation_line_prefix_length``: length (in characters) of
    the whitespace prefix of continuation lines in ``bjobs`` output.
    This setting is normally not needed.

If ``transport`` is ``ssh``, then the following options are also read
and take precedence above the corresponding options set in the "auth"
section:

  * ``port``: TCP port number where the SSH server is listening.
  * ``keyfile``: path to the (private) key file to use for SSH public
    key authentication.
  * ``ssh_config``: path to a SSH configuration file, where to read
    additional options from.  The format of the SSH configuration file
    is documented in the `ssh_config(5)`__ man page.
  * ``ssh_timeout``: maximum amount of time (in seconds) that GC3Pie will
    wait for the SSH connection to be established.

.. __: http://www.openbsd.org/cgi-bin/man.cgi/OpenBSD-current/man5/ssh_config.5?query=ssh_config&sec=5

.. note::

  We advise you to use the SSH config file for setting port, key file,
  and connection timeout.  Options ``port``, ``keyfile``, and
  ``timeout`` could be deprecated in future releases.


``slurm`` resources
~~~~~~~~~~~~~~~~~~~

The following configuration keys are required in a ``slurm``-type resource section:

  * ``transport``: Possible values are: ``ssh`` or ``local``.  If
    ``ssh``, GC3Pie tries to connect to the host specified in ``frontend``
    via SSH in order to execute SLURM commands.  If ``local``, the
    SLURM commands are run directly on the machine where GC3Pie is
    installed.

  * ``frontend``: should contain the `FQDN (Fully-qualified domain
    name)`:abbr: of the SLURM front-end node. This configuration item
    is only relevant if ``transport`` is ``local``. An SSH connection
    will be attempted to this node, in order to submit jobs and
    retrieve status info.

GC3Pie uses standard command line utilities to interact with the
resource manager. By default these commands are searched using the
``PATH`` environment variable, but you can specify the full path of
these commands and/or add some extra options. The following options
are used by the SLURM backend:

  * ``sbatch``: submit a job.

  * ``scancel``: cancel a job.

  * ``squeue``: get the status of a job or of the available resources.

  * ``sacct``: get info on resources used by a job.

If ``transport`` is ``ssh``, then the following options are also read
and take precedence above the corresponding options set in the "auth"
section:

  * ``port``: TCP port number where the SSH server is listening.
  * ``keyfile``: path to the (private) key file to use for SSH public
    key authentication.
  * ``ssh_config``: path to a SSH configuration file, where to read
    additional options from.  The format of the SSH configuration file
    is documented in the `ssh_config(5)`__ man page.
  * ``ssh_timeout``: maximum amount of time (in seconds) that GC3Pie will
    wait for the SSH connection to be established.

.. __: http://www.openbsd.org/cgi-bin/man.cgi/OpenBSD-current/man5/ssh_config.5?query=ssh_config&sec=5

.. note::

  We advise you to use the SSH config file for setting port, key file,
  and connection timeout.  Options ``port``, ``keyfile``, and
  ``timeout`` could be deprecated in future releases.


``shellcmd`` resources
~~~~~~~~~~~~~~~~~~~~~~

The following optional configuration keys are available in a
``shellcmd``-type resource section:

  * ``transport``: Like any other resources, possible values are
    ``ssh`` or ``local``. Default value is ``local``.

  * ``frontend``: If `transport` is `ssh`, then `frontend` is the
    `FQDN (Fully-qualified domain name)`:abbr: of the remote machine
    where the jobs will be executed.

  * ``time_cmd``: `ShellcmdLrms`:class: needs the GNU implementation
    of the command `time` in order to get resource usage of the
    submitted jobs. ``time_cmd`` must contains the path to the binary
    file if this is different from the standard (``/usr/bin/time``).

  * ``override``: `ShellcmdLrms`:class: by default will try to gather
    information on the system the resource is running on, including
    the number of cores and the available memory. These values may be
    different from the values stored in the configuration file. If
    ``override`` is `True`, then the values automatically discovered
    will be used. If ``override`` is `False`, the values in the
    configuration file will be used regardless of the real values
    discovered by the resource.

  * ``spooldir``: Path to a filesystem location where to create
    temporary working directories for processes executed through this
    backend. The default value `None` means to use ``$TMPDIR`` or
    `/tmp`:file: (see `tempfile.mkftemp` for details).

If ``transport`` is ``ssh``, then the following options are also read
and take precedence above the corresponding options set in the "auth"
section:

  * ``port``: TCP port number where the SSH server is listening.
  * ``keyfile``: path to the (private) key file to use for SSH public
    key authentication.
  * ``ssh_config``: path to a SSH configuration file, where to read
    additional options from.  The format of the SSH configuration file
    is documented in the `ssh_config(5)`__ man page.
  * ``ssh_timeout``: maximum amount of time (in seconds) that GC3Pie will
    wait for the SSH connection to be established.

.. __: http://www.openbsd.org/cgi-bin/man.cgi/OpenBSD-current/man5/ssh_config.5?query=ssh_config&sec=5

.. note::

  We advise you to use the SSH config file for setting port, key file,
  and connection timeout.  Options ``port``, ``keyfile``, and
  ``timeout`` could be deprecated in future releases.


``ec2+shellcmd`` resource
~~~~~~~~~~~~~~~~~~~~~~~~~

.. _boto: https://github.com/boto/boto

The following configuration options are available for a resource of
type ``ec2+shellcmd``. If these options are omitted, then the default
of the `boto`_ python library will be used, which at the time of
writing means use the default region on Amazon.

* ``ec2_url``: The URL of the EC2 frontend. On Amazon's AWS this is
  something like ``https://ec2.us-east-1.amazonaws.com`` (this is
  valid for the zone ``us-east-1`` of course).  If no value is
  specified, the environment variable ``EC2_URL`` will be used, and if
  not found an error is raised.

* ``ec2_region``: the region you want to access to.

* ``keypair_name``: the name of the keypair to use when creating a new
  instance on the cloud. If it's not found, a new keypair with this
  name and the key stored in ``public_key`` will be used. Please note
  that if the keypair exists already on the cloud but the associated
  public key is different from the one stored in ``public_key``, then
  an error is raised and the resource will not be used.

* ``public_key``: public key to use when creating the keypair.
  
  .. note::

    GC3Pie assumes that the corresponding private key is stored on a
    file with the same path but without the ``.pub`` extension. This
    private key is necessary in order to access the virtual machines
    created on the cloud.

  .. note::
   
    **For Amazon AWS users**: Please note that AWS EC2 does not accept
    *DSA* keys; use RSA keys only for AWS resources.

* ``vm_auth``: the name of a valid ``auth`` stanza used to connect to
  the virtual machine.

* ``instance_type``: the instance type (aka *flavor*, aka *size*) you
  want to use for your virtual machines by default.

* ``<application>_instance_type``: you can override the default
  instance type for a specific application by defining an entry in the
  configuration file for that application. For example::

        instance_type=m1.tiny
        gc_gps_instance_type=m1.large

  will use instance type ``m1.large`` for the ``gc_gps`` GC3Pie
  application, and ``m1.tiny`` for all the other applications.

* ``image_id``: the **ami-id** of the image you want to use.

* ``<application>_image_id``: override the generic ``image_id`` for a
  specific application.

  For example::

      image_id=ami-00000048
      gc_gps_image_id=ami-0000002a

  will make GC3Pie use the image ``ami-0000002a`` when running
  ``gc_gps``, and image ``ami-00000048`` for all other applications.

* ``security_group_name``: name of the `security group
  <http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html>`_
  to associate with VMs started by GC3Pie.

  If the named security group cannot be found, it will be created
  using the rules found in ``security_group_rules``. If the security
  group is found but some of the rules in ``security_group_rules`` are
  not present, they will be added to the security groups. Additional
  rules, which are listed in the EC2 console but not included in
  ``security_group_rules``, will *not* be removed from the security
  group.

* ``security_group_rules``: comma separated list of security rules
  the ``security_group`` must have.

  Each rule has the form::

      PROTOCOL:PORT_RANGE_START:PORT_RANGE_END:IP_NETWORK

  where:

  -  ``PROTOCOL`` is one of ``tcp``, ``udp``, ``icmp``;
  - ``PORT_RANGE_START`` and ``PORT_RANGE_END`` are integers and
    define the range of ports to allow. If ``PROTOCOL`` is ``icmp``
    please use ``-1`` for both values since in ``icmp`` there is no
    concept of *port*.
  - ``IP_NETWORK`` is a range of IP to allow in the form
    ``A.B.C.D/N``.

  For instance, to allow SSH access to the virtual machine from *any*
  machine in the internet you can use::

      security_group_rules = tcp:22:22:0.0.0.0/0

  .. note::

     In order to be able to access the virtual machines it created,
     GC3Pie **needs** to be able to connect via SSH, so a rule like
     the above is probably necessary in any GC3Pie configuration. For
     better security, it is wise to only allow the IP address or the
     range of IP addresses in use at your institution.

* ``vm_pool_max_size``: the maximum number of Virtual Machine GC3Pie
  will start on this cloud. If `0` then there is no predefined limit
  to the number of virtual machines that GC3Pie can spawn.

* ``user_data``: the *content* of a script that will run after the
  startup of the machine. For instance, to automatically upgrade a
  ubuntu machine after startup you can use::

      user_data=#!/bin/bash
        aptitude -y update
        aptitude -y safe-upgrade

  .. note::

     When entering multi-line scripts, lines after the first one
     (where ``user_data=`` is) *must* be indented, i.e., begin with
     one or more spaces.

* ``<application>_user_data``: override the generic ``user_data`` for
  a specific application.

  For example::

      # user_data=
      warholize_user_data = #!/bin/bash
        aptitude -y update && aptitude -y install imagemagick

  will install the ``imagemagick`` package only for VMs meant to run
  the ``warholize`` application.


Example ``resource`` sections
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*Example 1.* This configuration stanza defines a resource to submit
jobs to the `Grid Engine`_ cluster whose front-end host is
``ocikbpra.uzh.ch``::

    [resource/ocikbpra]
    # A single SGE cluster, accessed by SSH'ing to the front-end node
    type = sge
    auth = <auth_name> # pick an ``ssh`` type auth, e.g., "ssh1"
    transport = ssh
    frontend = ocikbpra.uzh.ch
    gamess_location = /share/apps/gamess
    max_cores_per_job = 80
    max_memory_per_core = 2
    max_walltime = 2
    ncores = 80

*Example 2.* This configuration stanza defines a resource to submit
jobs on virtual machines that will be automatically started by GC3Pie
on `Hobbes`_, the private OpenStack cloud of the University of Zurich::

    [resource/hobbes]
    enabled=yes
    type=ec2+shellcmd
    ec2_url=http://hobbes.gc3.uzh.ch:8773/services/Cloud
    ec2_region=nova

    auth=ec2hobbes
    # These values my be overwritten by the remote resource
    max_cores_per_job = 8
    max_memory_per_core = 2
    max_walltime = 8
    max_cores = 32
    architecture = x86_64

    keypair_name=my_name
    # If keypair does not exists, a new one will be created starting from
    # `public_key`. Note that if the desired keypair exists, a check is
    # done on its fingerprint and a warning is issued if it does not match
    # with the one in `public_key`
    public_key=~/.ssh/id_dsa.pub
    vm_auth=gc3user_ssh
    instance_type=m1.tiny
    warholize_instance_type = m1.small
    image_id=ami-00000048
    warholize_image_id=ami-00000035
    security_group_name=gc3pie_ssh
    security_group_rules=tcp:22:22:0.0.0.0/0, icmp:-1:-1:0.0.0.0/0
    vm_pool_max_size = 8
    user_data=
    warholize_user_data = #!/bin/bash
        aptitude update && aptitude install -u imagemagick


Enabling/disabling selected resources
-------------------------------------

Any resource can be disabled by adding a line ``enabled = false`` to its
configuration stanza.  Conversely, a line ``enabled = true`` will undo
the effect of an ``enabled = false`` line (possibly found in a different
configuration file).

This way, resources can be temporarily disabled (e.g., the cluster is
down for maintenance) without having to remove them from the
configuration file.

You can selectively disable or enable resources that are defined in
the system-wide configuration file.  Two main use cases are supported:
the system-wide configuration file :file:``/etc/gc3/gc3pie.conf`` lists and
enables all available resources, and users can turn them off in their
private configuration file :file:``~/.gc3/gc3pie.conf``; or the system-wide
configuration can list all available resources but keep them disabled,
and users can enable those they prefer in the private configuration
file.


.. _`Hobbes`: http://www.gc3.uzh.ch/infrastructure/hobbes
