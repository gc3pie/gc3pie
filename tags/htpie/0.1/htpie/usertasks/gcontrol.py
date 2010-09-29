import htpie

from htpie.lib import utils
from htpie.lib.exceptions import *
from htpie import model
from htpie import statemachine

import sys

module_names = {'GSingle':'htpie.usertasks.gsingle',
                              'GHessian':'htpie.usertasks.ghessian',
                              'GHessianTest':'htpie.usertasks.ghessiantest',
                              'GString':'htpie.usertasks.gstring',
                            }

fsm_classes = dict()
for node_name, node_class in module_names.items():
    __import__(node_class)

class GControl(object):
    
    @staticmethod
    def kill(id):
        doc = model.Task.load(id)
        doc.kill()
        htpie.log.info('Task %s will be killed'%(id))
    
    @staticmethod
    def retry(id):
        doc = model.Task.load(id)
        doc.retry()
        htpie.log.info('Task %s will be retried'%(id))
    
    @staticmethod
    def info(id, long_format):
        doc = model.Task.load(id)
        output = doc.display(long_format)
        sys.stdout.write(output)
        sys.stdout.flush()
