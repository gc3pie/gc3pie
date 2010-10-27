class Status(object):
    RUNNING = 'STATUS_RUNNING'
    PAUSED = 'STATUS_PAUSED'
    ONCE = 'STATUS_IGNORE'
    ERROR = 'STATUS_ERROR'
    
    @staticmethod
    def terminal():
        return (Status.ONCE,  Status.ERROR)
