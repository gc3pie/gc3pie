import htpie

from htpie.lib import utils
from htpie.lib.exceptions import *
from htpie import enginemodel as model
from htpie.statemachine import *
from htpie.application import gamess
from htpie.usertasks import gsingle

import numpy as np
import os

_app_tag_mapping = dict()
_app_tag_mapping['gamess']=gamess.GamessApplication

_TASK_CLASS = 'GHessian'
_STATEMACHINE_CLASS = 'GHessianStateMachine'

class GHessianResult(model.EmbeddedDocument):
    hessian = model.PickleField()
    normal_mode = model.EmbeddedDocumentField(gamess.NormalMode)

class GHessian(model.Task):
    total_jobs = model.IntField(default = 0)
    children = model.ListField(model.ReferenceField(gsingle.GSingle))
    result = model.EmbeddedDocumentField(GHessianResult)
    
    
    def display(self, long_format=False):
        output = '%s %s %s %s\n'%(self.cls_name, self.id, self.state, self.status)
        output += 'Task submitted: %s\n'%(self.create_d)
        output += 'Task last ran: %s\n'%(self.last_exec_d)
        output += 'Delta: %s\n'%(self.last_exec_d - self.create_d)
        if self.done():
            if self.successful():
                output += 'Frequency:\n'
                output += '%s\n'%(np.array(self.result['normal_mode']['frequency']))
                output += 'Mode:\n'
                output += '%s\n'%(self.result['normal_mode']['mode'].pickle)
        if long_format:
            output += 'Child Tasks:\n'
            for child in self.children:
                output += '%s %s %s %s\n'%(child.cls_name, child.id, child.state, child.status)
                #output += '-' * 80 + '\n'
                #output += child.display()
        return output
    
    def retry(self):
        super(GHessian, self).retry()
        for child in self.children:
            child.retry()

    def kill(self):
        try:
            self.acquire(120)
        except:
            raise
        else:
            self.setstate(States.KILL, 'kill')
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
        task.result = GHessianResult()
        
        task.attach_file(f_input, 'inputs')
        
        fsm = gsingle.GSingle.create([f_input], app_tag, requested_cores, requested_memory, requested_walltime)
        task.children.append(fsm)
        task.total_jobs += 1
        
        task.setstate(States.FIRST_WAIT, 'init')
        task.save()
        return task
    
    def successful(self):
        if self.state == States.POSTPROCESS:
            return True
    
    @staticmethod
    def cls_fsm():
        return eval(_STATEMACHINE_CLASS)

class States(object):
    FIRST_WAIT = u'STATE_WAIT'
    GEN_WAIT = u'STATE_GEN_WAIT'
    GENERATE = u'STATE_GENERATE'
    PROCESS = u'STATE_PROCESS'
    PROCESS_WAIT = u'STATE_PROCESS_WAIT'
    POSTPROCESS = u'STATE_POSTPROCESS'
    KILL = u'STATE_KILL'

class GHessianStateMachine(StateMachine):
    
    def __init__(self):
        super(GHessianStateMachine, self).__init__()
    
    @state(States.FIRST_WAIT, color=StatePrint.START)
    def handle_first_wait_state(self):
        pass
    
    @fromto(States.FIRST_WAIT, States.GENERATE)
    def handle_first_wait_tran(self):
        if self._wait_util_done():
            return True
    
    @state(States.GENERATE, StateTypes.ONCE)
    def handle_generate_state(self):
        task_vec = self.task.children[0]
        app = _app_tag_mapping[self.task.app_tag]
        
        f_input = task_vec.open('inputs')[0]
        atoms, params = app.parse_input(f_input)

        params.r_orbitals = task_vec.result.vec[-1].pickle
        params.r_orbitals_norb = gamess.select_norb(task_vec.result)
        params.set_group_param('$GUESS', 'GUESS', 'MOREAD')
        params.set_group_param('$GUESS', 'NORB', params.r_orbitals_norb)
        
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
            self.task.children.append(fsm)
            self.task.total_jobs += 1
    
    @fromto(States.GENERATE, States.GEN_WAIT)
    def handle_generate_tran(self):
        return True
    
    @state(States.GEN_WAIT)
    def handle_generate_wait_state(self):
        pass
    
    @fromto(States.GEN_WAIT, States.PROCESS)
    def handle_generate_wait_tran(self):
        if self._wait_util_done():
            return True
    
    @state(States.PROCESS, StateTypes.ONCE)
    def handle_process_state(self):
        children = self.task.children
        app = _app_tag_mapping[self.task.app_tag]
        
        result_list  = list()
        for a_node in children:
            result_list.append(a_node.result)
        num_atoms = len(result_list[-1].gradient[-1].pickle)
        gradMat = np.zeros((num_atoms*len(result_list), 3), dtype=np.longfloat)
        count = 0
        for a_result in result_list:
            grad = a_result.gradient[-1].pickle
            for j in range(0, len(grad)):
                gradMat[count]=grad[j]
                count +=1
        mat = _calculateNumericalHessian(num_atoms, gradMat)
        postprocess_result = mat/_GRADIENT_CONVERSION
        self.task.result['hessian'] = model.PickleProxy()
        self.task.result['hessian'].pickle = postprocess_result
        
        dir = utils.generate_temp_dir()
        f_list = self.task.open('inputs')
        
        f_ghessian = '%s/ghessian_%s'%(dir, os.path.basename(f_list[0].name))
        atoms, params = app.parse_input(f_list[0])
        
        params.set_group_param('$CONTRL', 'RUNTYP', 'HESSIAN')
        params.set_group_param('$FORCE',  'RDHESS', '.T.')
        params.r_hessian = self.task.result['hessian'].pickle
        app.write_input(f_ghessian, atoms, params)

        def str_dict(dic):
            new_dic = {}
            for k, v in dic.items():
                new_dic[str(k)]=v
            return new_dic

        gc3_temp = str_dict(self.task.children[0].gc3_temp)
        fsm = gsingle.GSingle.create([f_ghessian], self.task.app_tag, **gc3_temp)
        self.task.children.append(fsm)
        self.state = States.PROCESS_WAIT
    
    @fromto(States.PROCESS, States.PROCESS_WAIT)
    def handle_process_tran(self):
        return True
    
    @state(States.PROCESS_WAIT)
    def handle_process_wait_state(self):
        pass
    
    @fromto(States.PROCESS_WAIT, States.POSTPROCESS)
    def handle_process_wait_tran(self):
        if self._wait_util_done():
            return True
    
    @state(States.POSTPROCESS, StateTypes.ONCE)
    def handle_postprocess_state(self):
        child = self.task.children[-1]
        #This will copy the two lists, but the MongoMatrix is a dbref,
        #therefore it will still point to the same doc as the original
        #child.result.normal_mode.mode
        self.task.result['normal_mode'] = child.result['normal_mode']
        
        return True
    
    @state(States.KILL, StateTypes.ONCE, color=StatePrint.START)
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
    
    @staticmethod
    def cls_task():
        return eval(_TASK_CLASS)

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
