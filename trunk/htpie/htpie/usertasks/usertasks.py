import sys
import string

module_names = {'GSingle':'htpie.usertasks.gsingle',
                              'GHessian':'htpie.usertasks.ghessian',
                              'GHessianTest':'htpie.usertasks.ghessiantest',
                              'GString':'htpie.usertasks.gstring',
                              'GBig':'htpie.usertasks.gbig', 
                              'GLittle':'htpie.usertasks.glittle', 
                            }

fsm_classes = dict()
for task_name, task_module in module_names.items():
    __import__(task_module)
    fsm_classes[task_name] = (eval('sys.modules[task_module].%s'%(task_name)), 
                                            eval('sys.modules[task_module].%sStateMachine'%(task_name)))

def get_fsm(cls_task_name):
    return fsm_classes[cls_task_name][1]

def get_fsm_match_lower(cls_task_name):
    lower_dict = dict(zip(map(string.lower,fsm_classes.keys()),fsm_classes.values()))
    return lower_dict[cls_task_name][1]
