from Exceptions import *
from InformationContainer import *
import os
import os.path
import types
import gc3utils

from pkg_resources import Requirement, resource_filename


# -----------------------------------------------------
# Applications
#

class Application(InformationContainer):

    def __init__(self, executable, arguments, inputs, outputs, **kw):
        """
        Support for running a generic application with the Gc3Utils.
        The following parameters are *required* to create an `Application`
        instance:

        * `executable`: name of the application binary to be launched
          on the remote resource; the specifics of how this is handled
          are dependent on the submission backend, but you may always
          run a script that you upload through the `inputs` mechanism
          by specifying './scriptname' as `executable`.

        * `arguments`: list of command-line arguments to pass to
          `executable`; any object in the list will be converted to
          string via Python ``str()``. Note that, in contrast with the
          UNIX ``execvp()`` usage, the first argument in this list
          will be passed as ``argv[1]``, i.e., ``argv[0]`` will always
          be equal to `executable`.

        * `inputs`: list of files that will be copied from the local
          computer to the remote execution node before execution
          starts. Each item in the list should be a pair
          `(local_file_name, remote_file_name)`; specifying a single
          string `file_name` is allowed as a shortcut and will result
          in both `local_file_name` and `remote_file_name` being
          equal.  If an absolute path name is specified as
          `remote_file_name`, then a warning will be issued and the
          behavior is undefined.

        * `outputs`: list of files that will be copied back from the
          remote execution node back to the local computer after
          execution has completed.  Each item in the list should be a pair
          `(remote_file_name, local_file_name)`; specifying a single
          string `file_name` is allowed as a shortcut and will result
          in both `local_file_name` and `remote_file_name` being
          equal.  If an absolute path name is specified as
          `remote_file_name`, then a warning will be issued and the
          behavior is undefined.

        The following optional parameters may be additionally
        specified as keyword arguments and will be given special
        treatment by the `Application` class logic:

        * `requested_cores`, `requested_memory`, `requested_walltime`:
          specify resource requirements for the application: the
          number of independent execution units (CPU cores), amount of
          memory (in GB; will be converted to a whole number by
          truncating any decimal digits), amount of wall-clock time to
          allocate for the computational job (in hours; will be
          converted to a whole number by truncating any decimal
          digits).

        * `environment`: a list of pairs `(name, value)`: the
          environment variable whose name is given by the contents of
          the string `name` will be defined as the content of string
          `value` (i.e., as if "export name=value" was executed prior
          to starting the application).  Alternately, one can pass in
          a list of strings of the form "name=value".

        * `stdin`: file name of a file whose contents will be fed as
          standard input stream to the remote-executing process.

        * `stdout`: name of a file where the standard output stream of
          the remote executing process will be redirected to; will be
          automatically added to `outputs`.

        * `stderr`: name of a file where the standard error stream of
          the remote executing process will be redirected to; will be
          automatically added to `outputs`.

        * `join`: if this evaluates to `True`, then standard error is
          redirected to the file specified by `stdout` and `stderr` is
          ignored.  (`join` has no effect if `stdout` is not given.)

        The ARC submission backend will also make use of the following
        optional parameter:

        * `rtes`: list of run-time environments to request on the
          remote resource; each RTE is specified by a string (its
          name).

        Any other keyword arguments will be set as instance
        attributes, but otherwise ignored by the `Application`
        constructor.

        After successful construction, an `Application` object is
        guaranteed to have at least the following instance attributes:
        * `executable`: a string specifying the executable name
        * `arguments`: list of strings specifying command-line arguments for executable invocation; possibly empty
        * `inputs`: dictionary mapping local file name (a string) to a remote file name (a string)
        * `outputs`: dictionary mapping remote file name (a string) to a local file name (a string)
        * `environment`: dictionary mapping environment variable names to the requested value (string); possibly empty
        * `stdin`: `None` or a string specifying a (local) file name.  If `stdin` is not None, then it matches a key name in `inputs`
        * `stdout`: `None` or a string specifying a (remote) file name.  If `stdout` is not None, then it matches a key name in `outputs`
        * `stderr`: `None` or a string specifying a (remote) file name.  If `stdout` is not None, then it matches a key name in `outputs`
        * `join`: boolean value, indicating whether `stdout` and `stderr` are collected into the same file
        * `rtes`: list of strings specifying the RTEs to request for ARC submission; possibly empty.
        """
        # required parameters
        self.executable = executable
        self.arguments = [ str(x) for x in arguments ]

        gc3utils.log.debug("Application: on entrance, inputs=%s" % inputs)
        def convert_to_tuple(val):
            if isinstance(val, (str, unicode)):
                l = str(val)
                r = os.path.basename(l)
                return (l, r)
            else: 
                return tuple(val)
        self.inputs = dict([ convert_to_tuple(x) for x in inputs ])
        self.outputs = dict([ convert_to_tuple(x) for x in outputs ])
        gc3utils.log.debug("Application: set inputs=%s" %  self.inputs)

        # optional params
        def get_and_remove(dictionary, key, default=None, verbose=False):
            if dictionary.has_key(key):
                result = dictionary[key]
                del dictionary[key]
            else:
                result = default
            if verbose:
                gc3utils.log.info("Using value '%s' for 'Application.%s'", result, key)
            return result
        # FIXME: should use appropriate unit classes for requested_*
        self.requested_cores = get_and_remove(kw, 'requested_cores')
        self.requested_memory = get_and_remove(kw, 'requested_memory')
        self.requested_walltime = get_and_remove(kw, 'requested_walltime')

        self.environment = get_and_remove(kw, 'environment', {})
        def to_env_pair(val):
            if isinstance(val, tuple):
                return val
            else:
                # assume `val` is a string
                return tuple(val.split('=', 1))
        self.environment = dict([ to_env_pair(x) for x in self.environment.items() ])

        self.join = get_and_remove(kw, 'join', False)
        self.stdin = get_and_remove(kw, 'stdin')
        if self.stdin and self.stdin not in self.inputs:
            self.input[self.stdin] = os.path.basename(self.stdin)
        self.stdout = get_and_remove(kw, 'stdout')
        if self.stdout and self.stdout not in self.outputs:
            self.outputs[self.stdout] = os.path.basename(self.stdout)
        self.stderr = get_and_remove(kw, 'stderr')
        if self.stderr and self.stderr not in self.outputs:
            self.outputs[self.stderr] = os.path.basename(self.stderr)

        self.rtes = get_and_remove(kw, 'rtes')

        # any additional param
        InformationContainer.__init__(self, **kw)

    def is_valid(self):
        return True

    def xrsl(self, resource):
        """
        Return a string containing an xRSL sequence, suitable for
        submitting an instance of this application through ARC's
        ``ngsub`` command.

        The default implementation produces XRSL content based on 
        the construction parameters; you should override this method
        to produce XRSL tailored to your application.
        """
        xrsl= str.join(' ', [
                '&',
                '(executable="%s")' % self.executable,
                '(arguments=%s)' % str.join(' ', [('"%s"' % x) for x in self.arguments]),
                '(gmlog="gmlog")', # FIXME: should check if conflicts with any input/output files
                ])
        if (os.path.basename(self.executable) in self.inputs
            or './'+os.path.basename(self.executable) in self.inputs):
            xrsl += '(executables="%s")' % os.path.basename(self.executable)
        if self.stdin:
            xrsl += '(stdin="%s")' % self.stdin
        if self.join:
            xrsl += '(join="yes")'
        else:
            xrsl += '(join="no")'
        if self.stdout:
            xrsl += '(stdout="%s")' % self.stdout
        if self.stderr and not self.join:
            xrsl += '(stderr="%s")' % self.stderr
        if len(self.inputs) > 0:
            xrsl += ('(inputFiles=%s)' 
                     % str.join(' ', [ ('("%s" "%s")' % (r,l)) for (l,r) in self.inputs.items() ]))
        if len(self.outputs) > 0:
            xrsl += ('(outputFiles=%s)' 
                     % str.join(' ', [ ('("%s" "%s")' % rl) for rl in self.outputs.items() ]))
        if len(self.rtes) > 0:
            xrsl += str.join('\n', [
                    ('(runTimeEnvironment="%s")' % rte) for rte in self.rtes ])
        if len(self.environment) > 0:
            xrsl += ('(environment=%s)' % 
                     str.join(' ', [ ('("%s" "%s")' % kv) for kv in self.environment ]))
        if self.requested_walltime:
            xrsl += '(wallTime="%d hours")' % self.requested_walltime
        if self.requested_memory:
            xrsl += '(memory="%d")' % (1000 * self.requested_memory)
        if self.requested_cores:
            xrsl += '(count="%d")' % self.requested_cores

        return xrsl


    def cmdline(self, resource):
        """
        Get an SGE ``qsub`` command-line invocation for submitting an
        instance of this application.  Return a pair `(cmd, script)`,
        where `cmd` is the command to run to submit an instance of
        this application to the SGE batch system, and `script` -if
        it's not `None`- is written to a new file, whose name is then
        substituted into `cmd` using Python's ``%`` operator.

        In the construction of the command-line invocation, one should
        assume that all the input files (as named in `Application.inputs`)
        have been copied to the current working directory, and that output
        files should be created in this same directory.

        As this is highly application-specific, the default
        implementation just raises a `NotImplemented` exception; you
        should override this method in derived classes to provide
        appropriate invocation templates.
        """
        raise NotImplementedError("Abstract method `Application.cmdline()` called - this should have been defined in a derived class.")


class GamessApplication(Application):
    """
    Specialized `Application` object to submit computational jobs running GAMESS-US.

    The only required parameter for construction is the input file
    name; for a list of other optional construction parameters, see
    `Application`.  Note that a GAMESS-US job is *always* submitted 
    with `join = True`, therefore any `stderr` setting is ignored.
    """
    def __init__(self, input_file_path, **kw):
        input_file_name = os.path.basename(input_file_path)
        input_file_name_sans = os.path.splitext(input_file_name)[0]
        output_file_name = input_file_name_sans + '.dat'
        # add defaults to `kw` if not already present
        def set_if_unset(key, value):
            if not kw.has_key(key):
                kw[key] = value
        set_if_unset('stdout', input_file_name_sans + '.out')
        set_if_unset('application_tag', "gamess")
        if kw.has_key('rtes'):
            kw['rtes'].append("APPS/CHEM/GAMESS-2009")
        else:
            kw['rtes'] = [ "APPS/CHEM/GAMESS-2009" ]
        arguments = [ input_file_name ] + (kw.get('arguments') or [ ])
        if kw.has_key('arguments'):
            del kw['arguments']
        # build generic `Application` obj
        Application.__init__(self, 
                             executable = "$GAMESS_LOCATION/nggms",
                             arguments = arguments,
                             inputs = [ (input_file_path, input_file_name) ],
                             outputs = [ output_file_name ],
                             join=True,
                             **kw)
                             

    def qgms(self, resource):
        """
        Return a `qgms` invocation to run GAMESS-US with the
        parameters embedded in this object.
        """
        qgms = "%s/qgms" % resource.gamess_location

        cores = None
        if self.requested_cores:
            cores = self.requested_cores
        elif resource.ncores > 0:
            cores = resource.ncores
        if cores:
            qgms += ' -n %d' % cores

        wctime_in_seconds = None
        if self.requested_walltime:
            wctime_in_seconds = self.requested_walltime * 60 * 60
        elif resource.walltime > 0:
            wctime_in_seconds = resource.walltime * 60 * 60
        if wctime_in_seconds:
            qgms += ' -i %d' % wctime_in_seconds

        # finally, add the input files
        qgms += str.join(" ", [ os.path.basename(r) for r in self.inputs.values() ])

        return qgms


    # Assume `qgms` is the correct way to run GAMESS on *any* batch system.
    cmdline = qgms
    #pbs = qgms
    #lsf = qgms


class RosettaApplication(Application):
    """
    Specialized `Application` object to submit one run of a single
    application in the Rosetta suite.

    Required parameters for construction:
    * `application`: name of the Rosetta application to call (e.g., "docking_protocol" or "relax")
    * `inputs`: a `dict` instance, keys are Rosetta "-in:file:*" options, values are the (local) path names of the corresponding files.  (Example: `inputs={"-in:file:s":"1brs.pdb"}`) 
    * `outputs`: list of output file names to fetch after Rosetta has finished running.

    Optional parameters:
    * `flags_file`: path to a local file containing additional flags for controlling Rosetta invocation;
      if `None`, a local configuration file will be used.
    * `database`: (local) path to the Rosetta DB; if this is not specified, then it is assumed that
      the correct location will be available at the remote execution site as environment variable
      'ROSETTA_DB_LOCATION'
    * `arguments`: If present, they will be appended to the Rosetta application command line.
    """
    def __init__(self, application, inputs, outputs=[], 
                 flags_file=None, database=None, arguments=[], **kw):
        # ensure `application` has no trailing ".something' (e.g., ".linuxgccrelease")
        application = os.path.splitext(application)[0]
        
        _inputs = list(inputs.values())
        gc3utils.log.debug("RosettaApplication: _inputs=%s" % _inputs)
        _outputs = list(outputs) # make a copy

        # do specific setup required for/by the support script "rosetta.sh"
        src_rosetta_sh = resource_filename(Requirement.parse("gc3utils"), 
                                           "gc3utils/etc/rosetta.sh")
        gc3utils.log.debug("RosettaApplication: src_rosetta_sh=%s" % src_rosetta_sh)
        rosetta_sh = application + '.sh'
        _inputs.append((src_rosetta_sh, rosetta_sh))
        _outputs.append(application + '.log')
        _outputs.append(application + '.tar.gz')

        _arguments = [ ]
        for opt, file in inputs.items():
            _arguments.append(opt)
            _arguments.append(os.path.basename(file))

        if flags_file:
            _inputs.append((flags_file, application + '.flags'))
            # the `rosetta.sh` driver will do this automatically
            #_arguments.append("@" + os.path.basename(flags_file))
        else:
            gc3utils.log.info("Using flags file: %s", 
                              gc3utils.Default.RCDIR + '/' + application + '.flags')
            gc3utils.utils.deploy_configuration_file(application + '.flags')
            _inputs.append(gc3utils.Default.RCDIR + '/' + application + '.flags')

        if database:
            _inputs.append(database)
            _arguments.append("-database")
            _arguments.append(os.path.basename(database))

        if len(arguments) > 0:
            _arguments.extend(arguments)

        kw['application_tag'] = 'rosetta'
        if kw.has_key('rtes'):
            kw['rtes'].append("APPS/BIO/ROSETTA-3.1")
        else:
            kw['rtes'] = [ "APPS/BIO/ROSETTA-3.1" ]

        kw.setdefault('stdout', application+'.stdout.txt')
        kw.setdefault('stderr', application+'.stderr.txt')

        Application.__init__(self,
                             executable = "./%s" % rosetta_sh,
                             arguments = _arguments,
                             inputs = _inputs,
                             outputs = _outputs,
                             **kw)


class RosettaDockingApplication(RosettaApplication):
    """
    Specialized `Application` class for executing a single run of the
    Rosetta "docking_protocol" application.

    Currently used in the `grosetta` app.
    """
    def __init__(self, pdb_file_path, native_file_path=None, 
                 number_of_decoys_to_create=1, flags_file=None, **kw):
        pdb_file_name = os.path.basename(pdb_file_path)
        pdb_file_dir = os.path.dirname(pdb_file_path)
        pdb_file_name_sans = os.path.splitext(pdb_file_name)[0]
        if native_file_path is None:
            native_file_path = pdb_file_path
        def get_and_remove(D, k, d):
            if D.has_key(k):
                result = D[k]
                del D[k]
                return result
            else:
                return d
        RosettaApplication.__init__(
            self,
            application = 'docking_protocol',
            inputs = { 
                "-in:file:s":pdb_file_path,
                "-in:file:native":native_file_path,
                },
            outputs = [
                pdb_file_name_sans + '.fasc',
                pdb_file_name_sans + '.sc',
                ],
            flags_file = flags_file,
            arguments = [ 
                "-out:file:o", pdb_file_name_sans,
                "-out:nstruct", number_of_decoys_to_create,
                ] + get_and_remove(kw, 'arguments', []),
            job_local_dir = get_and_remove(kw, 'job_local_dir', pdb_file_dir),
            **kw)





__registered_apps = {
    'gamess': GamessApplication,
    'rosetta': RosettaApplication,
}

def get(tag, *args, **kwargs):
    """
    Return an instance of the specific application class associated
    with `tag`.  Example:

      >>> app = get('gamess')
      >>> isinstance(app, GamessApplication)
      True

    The returned object is always an instance of a sub-class of
    `Application`::

      >>> isinstance(app, Application)
      True
    """
    # FIXME: allow registration of 3rd party app classes
    try:
        return __registered_apps[tag](*args, **kwargs)
    except KeyError:
        raise UnknownApplication("Application '%s' is not unknown to the gc3utils library." % tag)

