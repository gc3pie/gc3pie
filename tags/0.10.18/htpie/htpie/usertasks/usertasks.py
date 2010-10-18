import sys

module_names = {'GSingle':'htpie.usertasks.gsingle',
                              'GHessian':'htpie.usertasks.ghessian',
                              'GHessianTest':'htpie.usertasks.ghessiantest',
                              'GString':'htpie.usertasks.gstring',
                              'GBig':'htpie.usertasks.gbig', 
                              'GLittle':'htpie.usertasks.glittle', 
                            }

fsm_classes = dict()
for node_name, node_class in module_names.items():
    __import__(node_class)
    fsm_classes[node_name] = (eval('sys.modules[node_class].%s'%(node_name)), 
                                            eval('sys.modules[node_class].%sStateMachine'%(node_name)))   
