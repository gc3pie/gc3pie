'''
Created on Dec 28, 2009

@author: mmonroe
'''
from optparse import OptionParser
import logging
import os
import sys
import numpy as np

from gfunction import GFunction, run_function
sys.path.append('/home/mmonroe/apps/gorg')
from gorg_site.gorg_site.lib.mydb import Mydb

from ase.io.gamess import ReadGamessInp, WriteGamessInp
from ase.calculators.gamess import GamessGridCalc

class GHessian(GFunction):
    H_TO_PERTURB = 0.0052918
    GRADIENT_CONVERSION=1.8897161646320724
    
    def preprocess(self, atoms, params):
        from gorg_site.gorg_site.model.gridtask import TaskInterface

        a_task = TaskInterface(self.db).create(self.calculator.author, 'GHessian')
        a_task.user_data_dict['total_jobs'] = 0
        perturbed_postions = self.repackage(atoms.get_positions())
        
        params.title = 'job_number_%d'%a_task.user_data_dict['total_jobs']
        first_job = self.calculator.generate(atoms, params, a_task)
        for a_position in perturbed_postions[1:]:
            a_task.user_data_dict['total_jobs'] += 1
            params.title = 'job_number_%d'%a_task.user_data_dict['total_jobs']
            atoms.set_positions(a_position)
            sec_job = self.calculator.generate(atoms, params, a_task)
            sec_job.add_parent(first_job)
            first_job = sec_job
        return a_task

    def process_loop(self, a_task):
        done = False
        result_list = self.execute_run(a_task)
        for a_result in result_list:
            if not a_result.exit_successful():
                msg = 'GAMESS returned an error while running job %s.'%(a_result.a_job.id)
                self.logger.critical(msg)
                raise Exception, msg
        done = True
        return (done, result_list)
    
    def postprocess(self, result_list):
        num_atoms = len(result_list[-1].atoms.get_positions())
        a_task = result_list[-1].a_job.task
        gradMat = np.zeros((num_atoms*len(result_list), 3), dtype=np.longfloat)
        count = 0
        for a_result in result_list:
            grad = a_result.get_forces()
            for j in range(0, len(grad)):
                gradMat[count]=grad[j]
                count +=1
        mat = self.calculateNumericalHessian(num_atoms, gradMat)
        postprocess_result = mat/self.GRADIENT_CONVERSION
        
        f_hess = open('%s_ghessian.mjm'%(a_task.id), 'w')
        WriteGamessInp.build_gamess_matrix(postprocess_result, f_hess)
        f_hess.close()
        return postprocess_result

    def perturb(self, npCoords):
        stCoords= np.reshape(np.squeeze(npCoords), len(npCoords)*3, 'C')
        E =  np.vstack([np.zeros((1, len(stCoords))), np.eye((len(stCoords)),(len(stCoords)))])
        return self.H_TO_PERTURB*E+stCoords

    def repackage(self, org_coords):
        stCoords = self.perturb(org_coords)
        newCoords = list()
        for i in stCoords:
            i=i.reshape((len(i)/3, 3))
            newCoords.append(i)
        return newCoords
        
    def calculateNumericalHessian(self, sz, gradient):
        gradient= np.reshape(gradient,(len(gradient)/sz,sz*3),'C').T    
        hessian = np.zeros((3*sz, 3*sz), dtype=np.longfloat)
        for i in range(0, 3*sz):
            for j in range(0, 3*sz):
                hessian[i, j] = (1.0/(2.0*self.H_TO_PERTURB))*((gradient[i, j+1]-gradient[i, 0])+(gradient[j, i+1]-gradient[j, 0]))
        return hessian

if __name__ == '__main__':
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-d", "--dir", dest="directory",default='~/tasks', 
                      help="directory to save tasks in.")
    parser.add_option("-f", "--file", dest="file",default='hess.test2.inp', 
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
    ghess = GHessian(db, gamess_calc, logging_level)
    run_function(atoms, params, ghess)
    sys.exit()
