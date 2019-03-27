#! /usr/bin/env python
#
"""
"""

from __future__ import absolute_import, print_function

__author__ = 'Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>'
# summary of user-visible changes
__changelog__ = """
  2011-06-27:
    * Defined ABCApplication and basic SessionBasedScript
"""
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == '__main__':
    import cchem_gfit_abc_workflow
    cchem_gfit_abc_workflow.ABC_Workflow().run()


# stdlib imports
import os
import os.path
import sys

## interface to Gc3libs
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, existing_file, executable_file
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection


#######################################################################################

# path UML and APPPOT
ABC_UML_IMAGE="gsiftp://idgc3grid01.uzh.ch/local_repo/ABC/abc.img"
ABC_APPOTRUN="gsiftp://idgc3grid01.uzh.ch/local_repo/ABC/abc_appotrun.sh"
ABC_RUN="/home/acostantini/workflow/script/abc.sh"



#######################################################################################


#
# class MainSequentialABC --> 2 steps
#
class MainSequentialABC(SequentialTaskCollection):
        def __init__(self, name, ABC_UML_IMAGE, output, g3cfile, dimensions, fortran_pes, inputfilelist_abc, **extra_args):

                self.inputfilelist_abc = inputfilelist_abc
                self.output_folder = output
                self.extra_args = extra_args
                self.name = name
# first step --> __init__ source compiling

                first_task = Gfit3C_ABC_uml_Application(abc_uml_image_file=ABC_UML_IMAGE, output_folder=output, g3c_input_file=g3cfile, dimension_file=dimensions, surface_file=fortran_pes, **extra_args)
                SequentialTaskCollection.__init__(self, name, [first_task])

# second step --> __init__ ABC execution (parallel)

        def next(self, done):

                if done == 0:

                        first_task = self.tasks[done]

                        abc_executable = os.path.join(first_task.output_dir, "abc.x") #first_task.outputs["abc.x"].path)

                        second_task = ParallelABC(ABC_RUN, abc_executable, self.inputfilelist_abc, first_task.output_dir, **self.extra_args)
                        self.add(second_task)
                        return Run.State.RUNNING

                else:
                        return Run.State.TERMINATED

#######################################################################################


#
# class ParallelABC --> ABC execution (parallel)
#
class ParallelABC(ParallelTaskCollection):
        def __init__(self, executable, abc_executable, inputfilelist_abc, output_folder, **extra_args):

                parallel_task = []
                for input_file in inputfilelist_abc:
                        name = "ABC_execution_" + os.path.basename(input_file)

                        parallel_task.append(ABC_Application(executable, abc_executable, input_file, output_folder, **extra_args))

                ParallelTaskCollection.__init__(self, name, parallel_task)

        def terminated(self):
                self.execution.returncode = (0, 0)




#######################################################################################




#
# class Gfit3C_ABC_uml_Application --> compile on uml -->  ABC binary
#
class Gfit3C_ABC_uml_Application(Application):
    application_name = 'gfit3c_abc_uml'
    def __init__(self, abc_uml_image_file, output_folder, g3c_input_file=None, dimension_file=None, surface_file=None, **extra_args):
        inputs = [(abc_uml_image_file, "abc.img")]

        if surface_file:
                inputs.append((surface_file,os.path.basename(surface_file)))
                abc_prefix = os.path.basename(surface_file)
        elif g3c_input_file and dimension_file:
                inputs.append((g3c_input_file, os.path.basename(g3c_input_file)))
                inputs.append((dimension_file,"dimensions"))
                abc_prefix = os.path.basename(g3c_input_file)
        else:
                raise gc3libs.exceptions.InvalidArgument("Missing critical argument surface file [%s], g3c_file [%s], dimension_file [%s]" % (surface_file, g3c_input_file, dimension_file))

        extra_args['tags'] = ["TEST/APPPOT-0"]
        extra_args['output_dir'] = os.path.join(output_folder,"abc."+abc_prefix)

        gc3libs.Application.__init__(self,
                                        executable = "$APPPOT_STARTUP",
                                        arguments = ["--apppot", "abc.cow,abc.img"],
                                        inputs = inputs,
                                        outputs = [ ("abc.x", "abc."+abc_prefix), (os.path.basename(abc_prefix).split(".g3c")[0]+"_log.tgz",
                                        os.path.basename(abc_prefix).split(".g3c")[0]+"_log.tgz")],
                                        join = True,
                                        stdout = os.path.basename(abc_prefix).split(".g3c")[0]+".log",
                                        **extra_args
                                        )




#######################################################################################




# class ABC_Application --> single ABC run
#
class ABC_Application(Application):
    application_name = 'abc'
    def __init__(self, executable, abc_executable, input_file, output_folder, **extra_args):


        extra_args['output_dir'] = os.path.join(output_folder, os.path.basename(input_file))

        gc3libs.Application.__init__(self,
                                executable = os.path.basename(executable),
                                arguments = [os.path.basename(input_file)],
                                executables = [abc_executable],
                                inputs = [(abc_executable, os.path.basename(abc_executable)), (input_file, os.path.basename(input_file)), (executable, os.path.basename(executable))],
                                outputs = [],
                                # output_dir = os.path.join(output_folder,os.path.basename(input_file)),
                                join = True,
                                stdout = os.path.basename(input_file)+".out",
                                **extra_args
                                )




#######################################################################################



#
# SessionBasedScript__ ABC_Workflow
#
class ABC_Workflow(SessionBasedScript):
    """
    Main control of the ABC execution workflow.
    Depending on the input parameters, ABC executin is preceeded by ABC binary compilation
    Scan the specified INPUTDIR directories recursively for '.d' files,
    and submit a ABC job for each input file found; job progress is
    monitored and, when a job is done, its output file is retrieved back
    """
    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            #application = Gfit3C_ABC_uml_Application,
            input_filename_pattern = '*.d'
            )


    def setup_options(self):

# inputs file definitions
        self.add_param("--g3c",
                       action="store", dest="g3cfile",
                       type=existing_file, default=None,
                       help="G3C input file.")

        self.add_param("--dim",
                       action="store", dest="dimensions",
                       type=existing_file, default=None,
                       help="Surface file."
                       )
        self.add_param("--exec",
                       action="store", dest="abc_exec",
                       type=executable_file, default=None,
                       help="ABC binary file."
                       )
        self.add_param("--pes", action="store", dest="fortran_pes",
                       type=existing_file, default=None,
                       help="FORTRAN file for the potential energy surface."
                       )
        return


    def parse_args(self):
        self.inputfolder_abc = self.params.args[0]
        self.output_folder = self.params.output

# possible actions depending from input files

    def new_tasks(self, extra):

         extra_args = extra.copy()

# inizializzo la lista di files

         self.inputfilelist_abc = self._search_for_input_files([self.inputfolder_abc])


# dimensions+gc3file --> compiling gfit + abc
         if self.params.dimensions and self.params.g3cfile:
             name = "Gfit3C_"+str(os.path.basename(self.params.g3cfile))
# yield for MainSequential_ABC
             yield (name, MainSequentialABC, [
                     name,
                     ABC_UML_IMAGE,
                     #self.apppot_run,
                     #ABC_APPOTRUN,
                     self.params.output,
                     self.params.g3cfile,
                     self.params.dimensions,
                     self.params.fortran_pes,
                     self.inputfilelist_abc,
                     ], extra_args) # MainSequentialABC(ABC_URL_IMAGE, self.apppot_run, self.params.out, ..., **extra_args)

# pes file --> compiling abc
#         elif self.params.fortran_pes:
#             name = "ABC_"+str(os.path.basename(self.params.fortran_pes))
# yield for MainSequential_ABC
#             yield (name, MainSequentialABC, [
#                     name,
#                    ABC_UML_IMAGE,
#                    #self.apppot_run,
#                     #ABC_APPOTRUN,
#                     self.params.output,
#                    self.params.g3cfile,
#                    self.params.dimensions,
#                     self.params.fortran_pes,
#                     ], extra_args)

# abc executable --> abc execution in parallel
         elif self.params.abc_exec:
             name = "ABC_execution_"+str(os.path.basename(self.params.abc_exec))
# yield for ParallelABC
             yield (name, ParallelABC, [
                     ABC_RUN,
                     self.params.abc_exec,
                     self.inputfilelist_abc,
                     os.path.join(self.params.output, os.path.basename(self.params.abc_exec)),
                     ], extra_args)

         else :
                raise gc3libs.exceptions.InvalidUsage("Invalid use of the command line")
#######################################################################################
