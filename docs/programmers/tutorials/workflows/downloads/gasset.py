#! /usr/bin/env python

import os
import sys
import csv

import gc3libs
from gc3libs.cmdline import SessionBasedScript
from gc3libs import Application, Run, Task
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds

if __name__ == '__main__':
    from gasset import AScript
    AScript().run()

def parse(input_csv):

    input_parameters = []
    
    with open(input_csv,'rb') as fd:
        reader = csv.reader(fd)
        for line in reader:
            input_parameters.append(line)

    return input_parameters
            
class GassetApplication(Application):
    """Run Asset cost evolution function."""

    application_name = 'gasset'
    
    def __init__(self, params, mscript, **extra_args):
        
        args = ""
        for param in params:
            args += " %s " % param

        mfunct = os.path.basename(mscript).split('.')[0]
        arguments = "matlab -nodesktop -nodisplay -nosplash -r \'%s %s;quit()'" % (mfunct,args)
        
        Application.__init__(
            self,
            arguments=arguments,
            inputs=[mscript],
            outputs=['./results/'],
            output_dir="gasset-%s" % params[-1],
            stdout="stdout.txt",
            stderr="stderr.txt",
            # required for running on the cloud, see GC3Pie issue #559
            requested_memory=1*GB,
            requested_walltime=8*hours
        )
    
class AScript(SessionBasedScript):
    """
    Minimal workflow scaffolding.
    """
    
    def __init__(self):
      super(AScript, self).__init__(version='1.0')

    def setup_args(self):
        self.add_param('input_csv', help="Input .csv file.")

        self.add_param('matlab_script', type=str,
                       help="Path to folder containing Matlab scripts.")

    def new_tasks(self, extra):

        tasks = []

        for params_list in parse(self.params.input_csv):

            extra_args = extra.copy()

            tasks.append(GassetApplication(
                params_list,
                self.params.matlab_script,
                **extra_args))            
        
        return tasks
