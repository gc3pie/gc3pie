from Exceptions import *
from InformationContainer import *
import os
import os.path
import types


# -----------------------------------------------------
# Applications
#

class Application(InformationContainer):

    def is_valid(self):
        if self.has_key('inputs'):
            for input in self.inputs:
                if not os.path.exists(input):
                    raise InputFileError("Input file '%s' does not exist" % input)
        return True

    def xrsl(self, resource):
        """
        Return a string containing an xRSL sequence, suitable for
        submitting an instance of this application through ARC's
        ``ngsub`` command.

        As this is highly application-specific, the default implementation
        just raises a `NotImplemented` exception; you should override
        this method in derived classes to provide appropriate xRSL templates.
        """
        raise NotImplementedError("Abstract method `Application.xrsl()` called - this should have been defined in a derived class.")

    def sge(self, resource):
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
    """
    def xrsl(self, resource):
        # GAMESS only needs 1 input file
        input_file_path = application.inputs[0]
        xrsl = utils.from_template(Default.GAMESS_XRSL_TEMPLATE, 
                                   INPUT_FILE_NAME = os.path.splitext(os.path.basename(input_file_path))[0],
                                   INPUT_FILE_DIR = os.path.dirname(input_file_path))
                
        if int(self.requested_walltime) > 0:
            xrsl += '(cputime="%s")\n' % (int(self.requested_walltime) * 60)
        elif resource.walltime > 0:
            xrsl += '(cputime="%s")\n' % resource.walltime

        if int(self.requested_cores) > 0:
            xrsl += '(count="%s")\n' % int(self.requested_cores)
        elif resource.ncores > 0:
            xrsl += '(count="%s")\n' % resource.ncores

        if int(self.requested_memory) > 0:
            xrsl += '(memory="%s")\n' % (int(self.requested_memory) * 1000)
        elif resource.memory_per_core > 0:
            xrsl += '(memory="%s")\n' % (int(resource.memory_per_core) * 1000)

        return xrsl


    def qgms(self):
        """
        Return a `qgms` invocation to run GAMESS-US with the
        parameters embedded in this object.
        """
        qgms = "%s/qgms" % resource.gamess_location

        cores = None
        if int(self.requested_cores) > 0:
            cores = int(self.requested_cores)
        elif resource.ncores > 0:
            cores = resource.ncores
        if cores:
            qgms += ' -n %d' % cores

        wctime_in_seconds = None
        if int(self.requested_walltime) > 0:
            wctime_in_seconds = int(self.requested_walltime) * 60 * 60
        elif resource.walltime > 0:
            wctime_in_seconds = resource.walltime * 60 * 60
        if wctime_in_seconds:
            qgms += ' -i %d' % wctime_in_seconds

        # finally, add the input file
        qgms += " '%s'" % (os.path.basename(self.inputs[0]))

        return (qgms, None)


    # Assume `qgms` is the correct way to run GAMESS on *any* batch system.
    sge = qgms
    #pbs = qgms
    #lsf = qgms
