import htpie

from htpie.lib import utils
from htpie.lib.exceptions import *
from htpie import model
from htpie import statemachine
from htpie.application import gamess
from htpie.usertasks import gsingle

import numpy as np
import os

_app_tag_mapping = dict()
_app_tag_mapping['gamess']=gamess.GamessApplication

class States(statemachine.States):
    FIRST_WAIT = u'STATE_WAIT'
    GEN_WAIT = u'STATE_GEN_WAIT'
    GENERATE = u'STATE_GENERATE'
    PROCESS = u'STATE_PROCESS'
    PROCESS_WAIT = u'STATE_PROCESS_WAIT'
    POSTPROCESS = u'STATE_POSTPROCESS'

class Transitions(statemachine.Transitions):
    pass

class GHessian(model.Task):

    structure = {'total_jobs': int,
                         'result': {'hessian':model.MongoMatrix,
                                            'normal_mode':{'atomic_mass':[float], 
                                                                       'frequency':[float], 
                                                                       'mode':model.MongoMatrix, 
                                                                    }} 
    }
    
    default_values = {
        '_type':u'GHessian', 
        'transition': Transitions.HOLD, 
        'total_jobs':0, 
    }
    
    def display(self, long_format=False):
        output = '%s %s %s %s\n'%(self._type, self.id, self.state, self.transition)
        output += 'Task submitted: %s\n'%(self.create_d)
        output += 'Task last ran: %s\n'%(self.last_exec_d)
        output += 'Delta: %s\n'%(self.last_exec_d - self.create_d)
        if self.transition == Transitions.COMPLETE:
            output += 'Frequency:\n'
            output += '%s\n'%(np.array(self.result['normal_mode']['frequency']))
            output += 'Mode:\n'
            output += '%s\n'%(self.result['normal_mode']['mode'].matrix)
        if long_format:
            for child in self.children:
                output += '-' * 80 + '\n'
                output += child.display()
        return output
    
    def retry(self):
        if self.transition == Transitions.ERROR:
            try:
                self.acquire()
            except:
                raise
            else:
                self.transition = Transitions.PAUSED
                self.release()
        for child in self.children:
            child.retry()

    def kill(self):
        try:
            self.acquire()
        except:
            raise
        else:
            self.state = States.KILL
            self.transition = Transitions.PAUSED
            self.release()
            htpie.log.debug('GHessian %s will be killed'%(self.id))
            for child in self.children:
                try:
                    child.kill()
                except:
                    pass

    @classmethod
    def create(cls, f_input,  app_tag='gamess', requested_cores=2, requested_memory=2, requested_walltime=2):
        task = super(GHessian, cls,).create()
        task.app_tag = u'%s'%(app_tag)
        task.result['hessian'] = model.MongoMatrix.create()
        
        task.attach_file(f_input, 'input')
        
        fsm = gsingle.GSingle.create([f_input], app_tag, requested_cores, requested_memory, requested_walltime)
        task.add_child(fsm)
        task.total_jobs += 1
        
        task.transition = Transitions.PAUSED
        task.state = States.FIRST_WAIT
        task.save()
        return task

model.con.register([GHessian])

class GHessianStateMachine(statemachine.StateMachine):
    _cls_task = GHessian
    
    def __init__(self):
        super(GHessianStateMachine, self).__init__()
        self.state_mapping.update({States.FIRST_WAIT: self.handle_first_wait_state, 
                                                      States.GEN_WAIT: self.handle_gen_wait_state, 
                                                      States.GENERATE: self.handle_generate_state, 
                                                      States.PROCESS: self.handle_process_state, 
                                                      States.PROCESS_WAIT: self.handle_process_wait_state, 
                                                      States.POSTPROCESS: self.handle_postprocess_state, 
                                                      States.KILL: self.handle_kill_state, 
                                                    })

    def handle_first_wait_state(self):
        if self._wait_util_done():
            self.state = States.GENERATE

    def handle_generate_state(self):
        task_vec = self.task.children[0]
        app = _app_tag_mapping[self.task.app_tag]
        
        f_input = task_vec.open('input')[0]
        atoms, params = app.parse_input(f_input)

        params.r_orbitals = task_vec.result.vec[-1].matrix
        params.r_orbitals_norb = task_vec.result.num_orbitals
        params.set_group_param('$GUESS', 'GUESS', 'MOREAD')
        params.set_group_param('$GUESS', 'NORB', task_vec.result.num_orbitals)
        
        perturbed_postions = _repackage(atoms.get_positions())[1:]
        
        def str_dict(dic):
            new_dic = {}
            for k, v in dic.items():
                new_dic[str(k)]=v
            return new_dic
        
        gc3_temp = str_dict(task_vec.gc3_temp)
        
        for a_position in perturbed_postions:
            params.title = 'job_number_%d'%(self.task.total_jobs)
            atoms.set_positions(a_position)
            
            dir = utils.generate_temp_dir()
            f_name = '%s/%s.inp'%(dir, params.title)
            app.write_input(f_name, atoms, params)

            fsm = gsingle.GSingle.create([f_name], self.task.app_tag, **gc3_temp)
            self.task.add_child(fsm)
            self.task.total_jobs += 1
        self.state = States.GEN_WAIT
    
    def handle_gen_wait_state(self):
        if self._wait_util_done():
            self.state = States.PROCESS

    def handle_process_state(self):
        children = self.task.children
        app = _app_tag_mapping[self.task.app_tag]
        
        result_list  = list()
        for a_node in children:
            result_list.append(a_node.result)
        num_atoms = len(result_list[-1].gradient[-1].matrix)
        gradMat = np.zeros((num_atoms*len(result_list), 3), dtype=np.longfloat)
        count = 0
        for a_result in result_list:
            grad = a_result.gradient[-1].matrix
            for j in range(0, len(grad)):
                gradMat[count]=grad[j]
                count +=1
        mat = _calculateNumericalHessian(num_atoms, gradMat)
        postprocess_result = mat/_GRADIENT_CONVERSION
        self.task.result['hessian'].matrix = postprocess_result
        
        dir = utils.generate_temp_dir()
        f_list = self.task.open('input')
        
        f_ghessian = '%s/ghessian_%s'%(dir, os.path.basename(f_list[0].name))
        atoms, params = app.parse_input(f_list[0])
        
        params.set_group_param('$CONTRL', 'RUNTYP', 'HESSIAN')
        params.set_group_param('$FORCE',  'RDHESS', '.T.')
        params.r_hessian = self.task.result['hessian'].matrix
        app.write_input(f_ghessian, atoms, params)

        def str_dict(dic):
            new_dic = {}
            for k, v in dic.items():
                new_dic[str(k)]=v
            return new_dic

        gc3_temp = str_dict(self.task.children[0].gc3_temp)
        fsm = gsingle.GSingle.create([f_ghessian], self.task.app_tag, **gc3_temp)
        self.task.add_child(fsm)
        self.state = States.PROCESS_WAIT
    
    def handle_process_wait_state(self):
        if self._wait_util_done():
            self.state = States.POSTPROCESS
    
    def handle_postprocess_state(self):
        child = self.task.children[-1]
        #This will copy the two lists, but the MongoMatrix is a dbref,
        #therefore it will still point to the same doc as the original
        #child.result.normal_mode.mode
        self.task.result['normal_mode'] = child.result['normal_mode']
        
        self.state = States.COMPLETE
        return True
    
    def handle_kill_state(self):
        return True

    def _wait_util_done(self):
        children = self.task.children
        count = 0
        for child in children:
            if child.done():
                if child.successful():
                    count += 1
                else:
                    raise ChildNodeException('Child task %s has been unsuccessful'%(child.id))
        
        if count == len(children):
            return True
        else:
            return False


_H_TO_PERTURB = 0.0052918
_GRADIENT_CONVERSION=1.8897161646320724
    
def _perturb(npCoords):
    stCoords= np.reshape(np.squeeze(npCoords), len(npCoords)*3, 'C')
    E =  np.vstack([np.zeros((1, len(stCoords))), np.eye((len(stCoords)),(len(stCoords)))])
    return _H_TO_PERTURB*E+stCoords

def _repackage(org_coords):
    stCoords = _perturb(org_coords)
    newCoords = list()
    for i in stCoords:
        i=i.reshape((len(i)/3, 3))
        newCoords.append(i)
    return newCoords
    
def _calculateNumericalHessian(sz, gradient):
    gradient= np.reshape(gradient,(len(gradient)/sz,sz*3),'C').T    
    hessian = np.zeros((3*sz, 3*sz), dtype=np.longfloat)
    for i in range(0, 3*sz):
        for j in range(0, 3*sz):
            hessian[i, j] = (1.0/(2.0*_H_TO_PERTURB))*((gradient[i, j+1]-gradient[i, 0])+(gradient[j, i+1]-gradient[j, 0]))
    return hessian


if __name__ == '__main__':
    import sys
    a_run = GHessian.create('examples/water_UHF_gradient.inp')
    fsm = GHessianStateMachine()
    fsm.load(GHessian, a_run.id)
    print a_run.id
    sys.exit(1)
    
