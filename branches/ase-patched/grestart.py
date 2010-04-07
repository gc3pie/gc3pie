'''
Created on Dec 28, 2009

@author: mmonroe
'''

from gfunction import GFunction, run_function
from optparse import OptionParser
import logging
import os
import copy
import sys

from ase.io.gamess import ReadGamessInp
from ase.calculators.gamess import GamessGridCalc
sys.path.append('/home/mmonroe/apps/gorg')
from gorg_site.gorg_site.lib.mydb import Mydb

class GRestart(GFunction):

    def preprocess(self, atoms, params):
        from gorg_site.gorg_site.model.gridtask import TaskInterface

        a_task = TaskInterface(self.db).create(self.calculator.author, 'GRestart')
        a_task.user_data_dict['restart_number'] = 0
        params.title = 'restart_number_%d'%a_task.user_data_dict['restart_number']
        a_job = self.calculator.generate(atoms, params, a_task)
        return a_task

    def process_loop(self, a_task):
        done = False
        result_list = self.execute_run(a_task.children[-1])
        a_result = result_list[-1]
        params = copy.deepcopy(a_result.params)
        atoms = copy.deepcopy(a_result.atoms)
        if a_result.exit_successful():
            if not a_result.geom_located():                
                params.r_orbitals = a_result.get_orbitals(raw=True)
                params.r_hessian = a_result.get_hessian(raw=True)
                a_task.user_data_dict['restart_number'] += 1
                params.title = 'restart_number_%d'%a_task.user_data_dict['restart_number']
                # Make sure that the orbitals an hessian will be read in the inp file
                params.set_group_param('$GUESS', 'GUESS', 'MOREAD')
                params.set_group_param('$STATPT', 'HESS', 'READ')
                atoms.set_positions(a_result.get_coords())
                a_job = self.calculator.generate(atoms, params, a_task)
                a_job.add_parent(a_result.a_job)
            else:
                done = True
                self.logger.info('Restart sequence task id %s has finished successfully.'%(a_task.id))
        else:
            msg = 'GAMESS returned an error while running job %s.'%(a_result.a_job.id)
            self.logger.critical(msg)
            raise Exception, msg
        return (done, result_list)
    
    def postprocess(self, result_list):
        postprocess_result = result_list
        return postprocess_result

if __name__ == '__main__':
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-d", "--dir", dest="directory",default='~/tasks', 
                      help="directory to save tasks in.")
    parser.add_option("-f", "--file", dest="file",default='exam01.inp', 
                      help="gamess inp to restart from.")
    parser.add_option("-v", "--verbose", dest="verbose", default='', 
                      help="add more v's to increase log output.")
    parser.add_option("-n", "--db_name", dest="db_name", default='gorg_site', 
                      help="add more v's to increase log output.")
    parser.add_option("-l", "--db_loc", dest="db_loc", default='http://127.0.0.1:5984', 
                      help="add more v's to increase log output.")
    (options, args) = parser.parse_args()
    options.directory=os.path.expanduser(options.directory.rstrip('/'))
    
    #Setup logger
    LOGGING_LEVELS = (logging.CRITICAL, logging.ERROR, 
                                    logging.WARNING, logging.INFO, logging.DEBUG)
    logging_level = len(LOGGING_LEVELS) if len(options.verbose) > len(LOGGING_LEVELS) else len(options.verbose)

    #Parse all the parameters to keep track of the file names
    (filepath, filename) = os.path.split(options.file)
    if not filepath:
        filepath =  os.getcwd()
    
    # Connect to the database
    db=Mydb(options.db_name,options.db_loc).cdb()
    
    # Parse the gamess inp file
    myfile = open(options.file, 'rb')
    reader = ReadGamessInp(myfile)
    myfile.close()
    params = reader.params
    atoms = reader.atoms
    
    gamess_calc = GamessGridCalc('mark', db)
    grestart = GRestart(db, gamess_calc, logging_level)
    run_function(atoms, params, grestart)
    sys.exit()
