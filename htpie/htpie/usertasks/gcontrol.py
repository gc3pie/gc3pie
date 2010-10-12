import htpie

from htpie.lib import utils
from htpie.lib.exceptions import *
from htpie import enginemodel as model
from htpie import statemachine

from htpie.usertasks.usertasks import *

import datetime

class GControl(object):
    
    @staticmethod
    def kill(id):
        doc =  model.Task.objects.with_id(id)
        doc.kill()
        htpie.log.info('Task %s will be killed'%(id))
    
    @staticmethod
    def retry(id):
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
    def show(type, hours_ago):
        def match(type):
            for key in fsm_classes.keys():
                if key.lower() == type.lower():
                    return key
            raise UnknownTaskException('Task %s is unknown'%(type))
        node_class = fsm_classes[match(type)][0]
        if hours_ago:
            delta = datetime.timedelta(hours=hours_ago)
            delta = datetime.datetime.now() - delta 
            docs = node_class.objects(last_exec_d__gte = delta)
        else:
            docs = node_class.objects()
        format_str = '{0:25} {1:25} {2:25} {3:25} {4}\n'
        output = format_str.format('TASK NAME', 'ID', 'STATE', 'TRANSITION', 'LAST RAN')
        output += '-' * 135 +'\n'
        for doc in docs:
            output += format_str.format(doc.cls_name, doc.id, doc.state, doc.transition, doc.last_exec_d)
        sys.stdout.write(output)
        sys.stdout.flush()
