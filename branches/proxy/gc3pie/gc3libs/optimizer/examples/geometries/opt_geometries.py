#! /usr/bin/env python
#
"""
  Finding minimum energy for a certain set of geometries. 
"""

# Copyright (C) 2011, 2012 University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
__version__ = '$Revision$'
__author__ = 'Benjamin Jonen <benjamin.jonen@bf.uzh.ch>'
# summary of user-visible changes
__changelog__ = """

"""
__docformat__ = 'reStructuredText'

import os
import sys
from gc3libs import Application
sys.path.append('../../')
from support_gc3 import update_parameter_in_file
# optimizer specific imports
from dif_evolution import DifferentialEvolution
from gc3libs.application.gamess import GamessApplication
import numpy as np

float_fmt = '%25.15f'

def compute_target_geometries(pop_task_tuple):
    '''
      Given a list of (population, task), compute and return list of target 
      values. 
    '''
    enrgstr = re.compile(r'FINAL .+ ENERGY IS +(?P<enrgstr>-[0-9]+\.[0-9]+)')
    fxVals = []
    for (pop, task) in pop_task_tuple:
        outputDir = task.output_dir
        f = open(os.path.join(outputDir, 'xyz.out'))
        content = file.read()
        file.close()
        match = enrgstr.search(content)
        if match:
            fxVal = float(match.group('enrgstr'))
        fxVals.append(fxVal)
    return fxVals

def create_gammes_input_file(geom, dirname):
    '''
      geom: 1d numpy array defining the geometry to produce an input file for. 
    '''

    import os
    import numpy as np

    inptmpl = []
    inptmpl.append("""
     $CONTRL SCFTYP=RHF RUNTYP=ENERGY $END
     $BASIS GBASIS=STO NGAUSS=3 $END
     $DATA 
    Title
    C1
    """)
    inptmpl.append('$END')

    inpfl = 'H2CO3'
    natm = 6
    element = ('C', 'O', 'O', 'O', 'H', 'H')
    nchrg = (6.0, 8.0, 8.0, 8.0, 1.0, 1.0)
    ngeom = 1
#    geom = np.array([[ 1.,1.,1.,2.,2.,2.,3.,3.,3.,4.,4.,4.,5.,5.,5.,6.,6.,6.]])
#    dirname = 'blub'

    # creating directory for input files for current set of geometries
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    # GENERATING INPUT FILES FOR GAMESS

    # taking ith geometry / molecule
    geomstr = ''
    for j in xrange(natm):
        # taking jth atom of current molecule
        geomstr = geomstr + element[j] + '  ' + str(nchrg[j]) + \
            '  ' + '%10.8f'%geom[3*j] + '  ' + '%10.8f'%geom[3*j+1] + \
            '  ' + '%11.8f'%geom[3*j+2] + '\n'
    file_name = os.path.join(dirname, inpfl+'.inp')
    file = open(file_name, 'w')
    file.write(inptmpl[0] + geomstr + inptmpl[1])
    file.close()
    return file_name

#create_gammes_input_file(np.array([ 1.,1.,1.,2.,2.,2.,3.,3.,3.,4.,4.,4.,5.,5.,5.,6.,6.,6.]), os.getcwd())
#print 'done'

def task_constructor_geometries(x_vals, iteration_directory, **extra_args):
    '''
      Given solver guess `x_vals`, return an instance of :class:`Application`
      set up to produce the output :def:`target_fun` of :class:`GlobalOptimizer'
      analyzes to produce the corresponding function values. 
    '''
    import shutil

    # Set some initial variables
    path_to_geometries_example = os.getcwd()
    path_to_executable = os.path.join(path_to_rosenbrock_example, 'bin/rosenbrock')
    base_dir = os.path.join(path_to_rosenbrock_example, 'base')

    index = 0 # We are dealing with scalar inputs

    jobname = 'para_' + '_'.join([('%25.15f' % val).strip() for val in x_vals])
    path_to_stage_dir = os.path.join(iteration_directory, jobname)
    path_to_stage_base_dir = os.path.join(path_to_stage_dir, 'base')
    inp_file_path = create_gammes_input_file(x_vals, path_to_stage_base_dir)

    kwargs = extra_args # e.g. architecture
    kwargs['stdout'] = executable + '.log'
    kwargs['join'] = True
    kwargs['output_dir'] =  os.path.join(path_to_stage_dir, 'output')
    gc3libs.log.debug("Output dir: %s" % kwargs['output_dir'])
    kwargs['requested_architecture'] = 'x86_64'
    kwargs['requested_cores'] = 1    

    return GamessApplication(inp_file_path=inp_file_path, **kwargs)

    ### Generate input file in path_to_stage_dir

    ##shutil.copytree(base_dir, path_to_stage_base_dir, ignore=shutil.ignore_patterns('.svn'))
    ##for var, val, para_file, para_file_format in zip(x_vars, x_vals, para_files, para_file_formats):
        ##val = (float_fmt % val).strip() 
        ##update_parameter_in_file(os.path.join(path_to_stage_base_dir, para_file),
                    ##var, index, val, para_file_format)

    #prefix_len = len(path_to_stage_base_dir) + 1        
    ## start the inputs dictionary with syntax: client_path: server_path
    #inputs = {}
    #for dirpath,dirnames,filenames in os.walk(path_to_stage_base_dir):
        #for filename in filenames:
        ## cut the leading part, which is == to path_to_stage_dir
        #relpath = dirpath[prefix_len:]
        ## ignore output directory contents in resubmission
        #if relpath.startswith('output'):
            #continue
        #remote_path = os.path.join(relpath, filename)
        #inputs[os.path.join(dirpath, filename)] = remote_path
    ## all contents of the `output` directory are to be fetched
    ## outputs = { 'output/':'' }
    #outputs = gc3libs.ANY_OUTPUT
    ##{ '*':'' }
    ##kwargs = extra.copy()
    #kwargs = extra_args # e.g. architecture
    #kwargs['stdout'] = executable + '.log'
    #kwargs['join'] = True
    #kwargs['output_dir'] =  os.path.join(path_to_stage_dir, 'output')
    #gc3libs.log.debug("Output dir: %s" % kwargs['output_dir'])
    #kwargs['requested_architecture'] = 'x86_64'
    #kwargs['requested_cores'] = 1
    ## hand over job to create


if __name__ == '__main__':
    import sys
    import gc3libs
    import time    

    print 'Starting: \n%s' % ' '.join(sys.argv)
    # clean up
    path_to_stage_dir = os.path.join(os.getcwd(), 'geometries') # Run the whole thing in the current directory
    # Initialize stage dir
    if not os.path.isdir(path_to_stage_dir):
        os.makedirs(path_to_stage_dir)     
    
    de_solver = DifferentialEvolution(dim = 2, pop_size = 100, de_step_size = 0.85, prob_crossover = 1., itermax = 200, 
                                      y_conv_crit = 0.1, de_strategy = 1, plotting = False, working_dir = path_to_stage_dir, 
                                      lower_bds = [-2, 2], upper_bds = [2,2], x_conv_crit = None, verbosity = 'DEBUG')
    
    initial_pop = []
    if not initial_pop:
        de_solver.newPop = de_solver.drawInitialSample()
    else:
        de_solver.newPop = initial_pop
    
    # create an instance globalObt
    from gc3libs.optimizer.global_opt import GlobalOptimizer
    globalOptObj = GlobalOptimizer(jobname = 'geometries', path_to_stage_dir = path_to_stage_dir, 
                                   optimizer = de_solver, task_constructor = task_constructor_geometries, 
                                   target_fun = compute_target_geometries)
    app = globalOptObj
    
    # create an instance of Core. Read configuration from your default
    # configuration file
    cfg = gc3libs.config.Configuration(*gc3libs.Default.CONFIG_FILE_LOCATIONS,
                                       **{'auto_enable_auth': True})
    g = gc3libs.core.Core(cfg)
    engine = gc3libs.core.Engine(g)
    engine.add(app)
    
    # Periodically check the status of your application.
    while app.execution.state != gc3libs.Run.State.TERMINATED:
        try:
            print "Job in status %s " % app.execution.state
            time.sleep(5)
            engine.progress()
        except:
            raise
    
    print "Job is now in state %s. Fetching output." % app.execution.state
    
    print 'main done'