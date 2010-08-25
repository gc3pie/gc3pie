import htpie

from htpie.lib import utils
from htpie.lib.exceptions import *
from htpie import model
from htpie import statemachine
from htpie.application import gamess
from htpie.usertasks import gsingle

from htpie.optimize import fire, lbfgs, neb

import numpy as np
import os
import copy

_app_tag_mapping = dict()
_app_tag_mapping['gamess']=gamess.GamessApplication

class States(statemachine.States):
    WAIT = u'STATE_WAIT'
    PROCESS = u'STATE_PROCESS'
    POSTPROCESS = u'STATE_POSTPROCESS'

class Transitions(statemachine.Transitions):
    pass

class GString(model.Task):

    structure = {'total_jobs': int,
                         'result': [{'gsingle':[gsingle.GSingle]}], 
                         'neb':model.MongoPickle, 
                         'opt':[model.MongoPickle], 
    }

    default_values = {
        '_type':u'GString', 
        'transition': Transitions.HOLD, 
    }
    
    def __str__(self):
        output = 'GString:\n'
        for a_result in self.result:
            for value in a_result.values():
                output +='Path:\n'
                for single in value:
                    output += '%s %s \n %s\n lock: %s\n'%(single.id, single.state, single.job, single['_lock'])
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
        for path in self.result:
            for children in path.values():
                for child in children:
                    try:
                        child.retry()
                    except:
                        pass

    def kill(self):
        try:
            self.acquire()
        except:
            raise
        else:
            self.state = States.KILL
            self.release()
            htpie.log.debug('GString %s will be killed'%(self.id))
            for path in self.result:
                for children in path.values():
                    for child in children:
                        try:
                            child.kill()
                        except:
                            pass

    @classmethod
    def create(cls, f_list,  app_tag='gamess', requested_cores=1, requested_memory=2, requested_walltime=2):
        task = super(GString, cls,).create()
        task.app_tag = u'%s'%(app_tag)
        app = _app_tag_mapping[task.app_tag]
        
        a_neb = neb.NEB()
        l_opt = list()
        
        for a_file in f_list:
            task.attach_file(a_file, 'input')
        
        atoms_start, params_start = app.parse_input(f_list[0])
        atoms_end, params_end = app.parse_input(f_list[1])
        
        assert params_start == params_end,  'Start and finish need to have the same params.'
        path = neb.interpolate(atoms_start.get_positions(), atoms_end.get_positions())
        
        for i in xrange(len(path)):
            mongo_pickle = model.MongoPickle.create()
            task.opt.append(mongo_pickle)
            task.opt[i].pickle = fire.FIRE()
        
        #path = path[0:3]
        task.result.append({'gsingle':_convert_pos_to_jobs(app_tag, atoms_start, params_start, path, requested_cores, requested_memory, requested_walltime)})
        task.neb = model.MongoPickle.create()
        task.neb.pickle = a_neb
        
        
        task.transition = Transitions.PAUSED
        task.state = States.WAIT
        task.save()
        return task

model.con.register([GString])

def _convert_pos_to_jobs(app_tag, atoms_start, params_start, path, requested_cores, requested_memory,  requested_walltime):
        count = 0
        app = _app_tag_mapping[app_tag]
        gsingle_path = list()
        for image in path:
            atoms_start.set_positions(image)
            
            dir = utils.generate_temp_dir()
            f_gimage = '%s/gimage_%d.inp'%(dir, count)
            app.write_input(f_gimage, atoms_start, params_start)
            fsm = gsingle.GSingle.create([f_gimage], app_tag, requested_cores, requested_memory, requested_walltime)
            gsingle_path.append(fsm)
            count += 1
        return gsingle_path

def _str_dict(dic):
        new_dic = {}
        for k, v in dic.items():
            new_dic[str(k)]=v
        return new_dic

class GStringStateMachine(statemachine.StateMachine):
    _cls_task = GString
    
    def __init__(self):
        super(GStringStateMachine, self).__init__()
        self.state_mapping.update({States.WAIT: self.handle_wait_state,                                                       
                                                      States.PROCESS: self.handle_process_state, 
                                                      States.POSTPROCESS: self.handle_postprocess_state, 
                                                      States.KILL: self.handle_kill_state, 
                                                    })

    def handle_wait_state(self):
        if self._wait_util_done():
            self.state = States.PROCESS

    def handle_process_state(self):
        children = self.task.result[-1]['gsingle']
        app = _app_tag_mapping[self.task.app_tag]
        a_neb = self.task.neb.pickle
        gc3_temp = _str_dict(children[0].gc3_temp)
        
        f_list = self.task.open('input')
        atoms_start, params_start = app.parse_input(f_list[0])
        [f.close() for f in f_list]
        
        path_positions = list()
        path_energies = list()
        path_forces = list()
        for image in children:
            path_positions.append(image.coord.matrix)
            path_energies.append(image.result.energy[-1])
            path_forces.append(image.result.gradient[-1].matrix)
        
        a_neb.forces(path_positions, path_energies, path_forces)
        
        new_pos = list()
        #Initialize fire matrix
        l_opt = list()
        for a_opt in self.task.opt:
            l_opt.append(a_opt.pickle)
            if l_opt[-1].v is None:
                l_opt[-1].step(a_neb.path[0].r, a_neb.path[0].f)
        
        force_converge = .01
        fmax = neb.vmag(a_neb.path[0].f)
        for image in a_neb.path:
            if neb.vmag(image.f) > fmax:
                fmax = neb.vmag(image.f)
        htpie.log.debug('GString %s max force %f'%(self.task.id, fmax))
        
        if fmax > force_converge:
            new_pos.append(a_neb.path[0].r)
            for i in range(1, len(a_neb.path) - 1):
                new_pos.append(l_opt[i].step(a_neb.path[i].r, a_neb.path[i].f))
                self.task.opt[i].pickle = l_opt[i]
                htpie.log.debug('Image force \n%s'%(a_neb.path[i].f))
            new_pos.append(a_neb.path[-1].r)
            
            for i in xrange(len( a_neb.path)):
                htpie.log.debug('GString %d position diff \n%s'%(i, a_neb.path[i].r - new_pos[i]))
            
            path = _convert_pos_to_jobs(self.task.app_tag, atoms_start, params_start, new_pos, **gc3_temp)
            
            self.task.result.append({'gsingle':path})
        
            
            self.task.neb.pickle = a_neb
        
            self.state = States.WAIT
        else:
            self.state = States.POSTPROCESS
    
    def handle_postprocess_state(self):

        self.state = States.COMPLETE
        return True
    
    def handle_kill_state(self):
        return True

    def _wait_util_done(self):
        children =  self.task.result[-1]['gsingle']
        count = 0
        for child in children:
            if child.transition == Transitions.COMPLETE:
                count += 1
            elif child.transition == Transitions.ERROR:
                raise ChildNodeException('Child task %s errored.'%(child.id))
        
        if count == len(children):
            return True
        else:
            return False
