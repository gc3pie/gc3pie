class States(object):
    COMPLETE = u'STATE_COMPLETED'
    KILL = u'STATE_KILL'
    
    @classmethod
    def terminal(cls):
        term = [cls.COMPLETE, 
                     cls.KILL]
        return term

class Transitions(object):
    ERROR = u'ACTION_ERROR'
    COMPLETE = u'ACTION_COMPLETE'
    RUNNING = u'ACTION_RUNNING'
    PAUSED = u'ACTION_PAUSED'
    HOLD = u'ACTION_HOLD'
    
    @classmethod
    def terminal(cls):
        term = [cls.ERROR, 
                     cls.COMPLETE]
        return term
