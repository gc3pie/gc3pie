import htpie

from htpie.lib import utils
from htpie.lib.exceptions import *
from htpie import enginemodel as model
from htpie import statemachine
from htpie.application import gamess
from htpie.usertasks import gsingle, ghessian

import numpy as np
import glob
import os

_app_tag_mapping = dict()
_app_tag_mapping['gamess']=gamess.GamessApplication

class States(statemachine.States):
    WAITING = u'STATE_WAIT'
    PROCESS = u'STATE_PROCESS'

class Transitions(statemachine.Transitions):
    pass

class GHessianResult(model.EmbeddedDocument):
    fname = model.StringField()
    gsingle = model.ReferenceField(gsingle.GSingle)
    ghessian = model.ReferenceField(ghessian.GHessian)
    
class GHessianTest(model.Task):
    result = model.ListField(model.EmbeddedDocumentField(GHessianResult))
    
    def display(self, long_format=False):
        output = '%s %s %s %s\n'%(self.cls_name, self.id, self.state, self.transition)
        output += '-' * 80 + '\n'
        for result in self.result:
            output += 'Filename: %s\n'%(result['fname'])
            if self.successful():
                output += 'Deltas are calculated as htpie - GAMESS\n'
                output += 'Frequency delta:\n'
                delta = np.array(result['ghessian'].result['normal_mode']['frequency']) - np.array(result['gsingle'].result['normal_mode']['frequency'])
                output += '%s\n'%(delta)
                output += 'Mode delta:\n'
                delta = result['ghessian'].result['normal_mode']['mode'].matrix - result['gsingle'].result['normal_mode']['mode'].matrix
                output += '%s\n'%(delta)
            if long_format:
                output += '-' * 80 + '\n'
                output += 'GAMESS normal mode job\n'
                output += result['gsingle'].display()
                if result['gsingle'].successful():
                    output += 'Frequency:\n'
                    output += '%s\n'%np.array(result['gsingle'].result['normal_mode']['frequency'])
                    output += 'Mode:\n'
                    output += '%s\n'%result['gsingle'].result['normal_mode']['mode'].matrix
                    output += '-' * 80 + '\n'
                    output += 'htpie calculated normal mode\n'
                output += '#' * 80 + '\n'
                output += result['ghessian'].display()
            output += '-' * 80 + '\n'
        return output
    
    @classmethod
    def create(cls, dir,  app_tag='gamess', requested_cores=24, requested_memory=2, requested_walltime=24):
        app = _app_tag_mapping[app_tag]
        task = super(GHessianTest, cls,).create()
        task.app_tag = u'%s'%(app_tag)

        for a_file in glob.glob('%s/*.inp'%(dir)):
            result = dict()
            
            try:
                dir = utils.generate_temp_dir()
                f_input = open(a_file, 'r')
                f_gamess_hessian = open('%s/gamess_hessian_%s'%(dir, os.path.basename(a_file)), 'w')
                atoms, params = app.parse_input(f_input)
                
                params.set_group_param('$CONTRL', 'RUNTYP', 'HESSIAN')
                params.set_group_param('$FORCE', 'METHOD', 'SEMINUM')
                params.set_group_param('$FORCE', 'NVIB', '1')
                
                app.write_input(f_gamess_hessian, atoms, params)
            finally:
                f_gamess_hessian.close()
                f_input.close()
            
            result = GHessianResult()
            result['fname'] = u'%s'%(os.path.basename(os.path.basename(f_input.name)))
            result['gsingle'] = gsingle.GSingle.create([f_gamess_hessian.name], app_tag, requested_cores, requested_memory, requested_walltime)
            result['ghessian'] = ghessian.GHessian.create(a_file, app_tag, requested_cores, requested_memory, requested_walltime)

            task.result.append(result)
        
        task.state = States.WAITING
        task.transition = Transitions.PAUSED
        task.save()
        return task
    
    def retry(self):
        if self.transition == Transitions.ERROR:
            try:
                self.acquire(120)
            except:
                raise
            else:
                self.transition = Transitions.PAUSED
                self.release()
        for a_result in self.result:
            a_result['gsingle'].retry()
            a_result['ghessian'].retry()
    
    def kill(self):
        try:
            self.acquire(120)
        except:
            raise
        else:
            self.state = States.KILL
            self.transition = Transitions.PAUSED
            self.release()
            htpie.log.debug('GHessianTest %s will be killed'%(self.id))
            for a_result in self.result:
                try:
                    a_result['gsingle'].kill()
                    a_result['ghessian'].kill()
                except:
                    pass

class GHessianTestStateMachine(statemachine.StateMachine):
    _cls_task = GHessianTest
    
    def __init__(self):
        super(GHessianTestStateMachine, self).__init__()
        self.state_mapping.update({States.WAITING: self.handle_waiting_state, 
                                                    States.PROCESS: self.handle_process_state, 
                                                    States.KILL: self.handle_kill_state, 
                                                    })

    def handle_waiting_state(self):
        
        count = 0
        for a_result in self.task.result:
            if a_result['ghessian'].done() and a_result['gsingle'].done():
                if a_result['ghessian'].successful() and a_result['gsingle'].successful():
                    count += 1
                else:
                    if not a_result['ghessian'].successful():
                        problem_result = a_result['ghessian']
                    else:
                        problem_result = a_result['gsingle']
                    raise ChildNodeException('Child task %s has been unsuccessful'%(problem_result.id))
        
        if count == len(self.task.result):
            self.state = States.PROCESS

    def handle_process_state(self):

        self.state = States.COMPLETE
        return True
    
    def handle_kill_state(self):
        return True
