import htpie

from htpie.lib import utils
from htpie.lib.exceptions import *
from htpie import enginemodel as model
from htpie import statemachine

from htpie.usertasks import usertasks

import datetime
import sys

class GControl(object):
    
    @staticmethod
    def kill(id, long_format):
        doc =  model.Task.objects.with_id(id)
        doc.kill()
        htpie.log.info('Task %s will be killed'%(id))
    
    @staticmethod
    def retry(id, long_format):
        doc =  model.Task.objects.with_id(id)
        doc.retry()
        htpie.log.info('Task %s will be retried'%(id))
    
    @staticmethod
    def info(id, long_format):
        doc = model.Task.objects.with_id(id)
        output = doc.display(long_format)
        sys.stdout.write(output)
        sys.stdout.flush()
    
    @staticmethod
    def states(id, long_format):
        doc = model.Task.objects.with_id(id)
        output = 'States:\n%s\n'%(usertasks.get_fsm(doc.cls_name).states)
        output += 'Transitions:\n%s\n'%(usertasks.get_fsm(doc.cls_name).transitions)
        sys.stdout.write(output)
        sys.stdout.flush()
    
    @staticmethod
    def statediag(id, long_format):
        #doc = model.Task.objects.with_id(id)
        fsm = usertasks.get_fsm_match_lower(id)()
        output = 'Generated state diagram: %s\n'%(fsm.states.display(fsm.name))
        sys.stdout.write(output)
        sys.stdout.flush()
    
    @staticmethod
    def query(type, hours_ago, long_format):
        def match(type):
            for key in usertasks.fsm_classes.keys():
                if key.lower() == type.lower():
                    return key
            raise UnknownTaskException('Task %s is unknown'%(type))
        task_class = usertasks.fsm_classes[match(type)][0]
        if hours_ago:
            delta = datetime.timedelta(hours=hours_ago)
            delta = datetime.datetime.now() - delta 
            docs = task_class.objects(last_exec_d__gte = delta)
        else:
            docs = task_class.objects()
        format_str = '{0:25} {1:25} {2:25} {3:25} {4}\n'
        output = format_str.format('TASK NAME', 'ID', 'STATE', 'STATUS', 'LAST RAN')
        output += '-' * 135 +'\n'
        for doc in docs:
            output += format_str.format(doc.cls_name, doc.id, doc.state, doc.status, doc.last_exec_d)
        sys.stdout.write(output)
        sys.stdout.flush()
