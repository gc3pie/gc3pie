from markstools.lib.utils import format_exception_info, create_file_logger
import logging

_log = logging.getLogger('markstools')

#from string import upper
#
#class StateMachine(object):
#    def __init__(self):
#        self.handlers = {}
#        self.startState = None
#        self.endStates = []
#        self.cargo = {}
#        self.handler = None
#        self.newState = None
# 
#    def add_state(self, name, handler, end_state=0):
#        name = upper(name)
#        self.handlers[name] = handler
#        if end_state:
#            self.endStates.append(name)
#
#    def set_start(self, name):
#        self.startState = upper(name)
#        self.newState = upper(name)
#
#    def iterator(self):
#        try:
#            self.handler = self.handlers[self.startState]
#        except:
#            raise "InitializationError", "must call .set_start() before .run()"
#        if not self.endStates:
#            raise "InitializationError", "at least one state must be an end_state"
#        return self
#
#    def next(self):
#        (newState, updated_cargo) = self.handler(**self.cargo)
#        self.cargo.update(updated_cargo)
#        if upper(newState) in self.endStates:
#            self.newState = upper(newState)
#            return upper(newState) 
#        else:
#            self.handler = self.handlers[upper(newState)]
#        self.newState = upper(newState)
#        return upper(newState)
#

class State(object):
    ON_ENTER = 'OnEnter'
    ON_LEAVE = 'OnLeave'
    ON_MAIN = 'OnMain'
    
    def __init__(self):
        self.type = dict()

    def __str__(self):
        output = 'Functions:\n'
        output += '%s: %s'%(self.ON_ENTER, self.on_enter().__name__)
        return output

    def set_on_enter(self, func):
        self.type[self.ON_ENTER] = func
    
    def on_enter(self, fsm):
        if self.ON_ENTER in self.type:
            result = self.exec_fun(self.type[self.ON_ENTER], fsm)
            assert result is None, 'OnEnter must return None'
            return result
        return None

    def set_on_main(self, func):
        self.type[self.ON_MAIN] = func
    
    def on_main(self, fsm):
        assert self.ON_MAIN in self.type,  'A state must have an OnMain'
        result = self.exec_fun(self.type[self.ON_MAIN], fsm)
        assert isinstance(result, State), 'OnMain must return a state'
        return result
    
    def set_on_leave(self, func):
        self.type[self.ON_LEAVE] = func
    
    def on_leave(self, fsm):
        if self.ON_LEAVE in self.type:
            result = self.exec_fun(self.type[self.ON_LEAVE], fsm)
            assert result is None, 'OnLeave must return None'
            return result
        return None
    
    @staticmethod
    def exec_fun(func, fsm):
        result = None
        try:
            result = func(fsm)
        except:
            msg = format_exception_info()
            print msg
            fsm.logger.critical(msg)
            raise
        return result
    
def on_enter(state):
    def wrapper(func):
        state.set_on_enter(func)
        return func
    return wrapper

def on_main(state):
    def wrapper(func):
        state.set_on_main(func)
        return func
    return wrapper

def on_leave(state):
    def wrapper(func):
        state.set_on_leave(func)
        return func
    return wrapper

class StateMachine(object):
    STOP_STATE_NAME = 'WAIT'
    ERROR = State()
    DONE = State()
    
    def __init__(self):
        self._cur_state = self.ERROR
        self.on_enter_triggered = False
    
    def start(self, init_state):
        self._cur_state = init_state
        self.on_enter_triggered = False
    
    def get_state(self):
        attr_dict = self.__class__.__dict__
        for key, value in attr_dict.iteritems():
            if value == self._cur_state:
                return key
        attr_dict = StateMachine.__dict__
        for key, value in attr_dict.iteritems():
            if value == self._cur_state:
                return key
    
    def save_state(self):
        current_state = self.get_state()        
        result = self._cur_state.on_leave(self)
        self.on_enter_triggered = False
        self._cur_state = self.DONE
        return current_state
    
    @staticmethod
    def stop_state():
        return StateMachine.STOP_STATE_NAME
#    def set_state(self, new_state):
#        self.__cur_state = eval('self.%s'%(new_state))
    
    def run(self):
        state = None
        while state != self.stop_state() and state != self.error_state() and state != self.done_state():
            state = self.step()
        return state
        
    def step(self):
        try:
            next_state = None
            if not self.on_enter_triggered:
                result = self._cur_state.on_enter(self)
                self.on_enter_triggered = True
            next_state = self._cur_state.on_main(self)
            if next_state != self._cur_state:
                result = self._cur_state.on_leave(self)
                self._cur_state = next_state
                self.on_enter_triggered = False
        except:
            self._cur_state = self.ERROR
        return self.get_state()
    
    @on_main(ERROR)
    def on_main_error_state(self):
        print 'I am in an error state'
        return self.ERROR
    
    @staticmethod
    def error_state():
        return 'ERROR'

    @on_main(DONE)
    def on_main_done_state(self):
        print 'I am done.'
        return self.DONE
    
    @staticmethod
    def done_state():
        return 'DONE'    

class Oven(StateMachine):
    FIRE = State()
    WATER = State()
    def __init__(self, cargo, logging_level=1):
        super(Oven, self).__init__(logging_level=1)
        self.cargo = cargo
        
    @on_enter(FIRE)
    def OnEnter_Fire(self):
        self.cargo.append('here is some cargo from on_enter')
        print 'Turn fire on'
    
    @on_leave(FIRE)
    def OnLeave_Fire(self):
        my_cargo = self.cargo.pop()
        print 'Print some cargo: %s'%(my_cargo)
        print 'Turn fire off'
    
    @on_main(FIRE)
    def OnMain_Fire(self):
        print 'FIRE!!'
        #raise NameError('Will it work?')
        return self.WATER
    
    @on_enter(WATER)
    def on_enter_water(self):
        self.cargo.append(0)
    
    @on_main(WATER)
    def OnMain_Water(self):
        self.cargo[0] += 1
        if self.cargo[0] == 4:
            return self.done_state()
        print 'water on the fire'
        return self.WATER
    
#    @on_leave(WATER)
#    def OnMain_Water(self):
#        print 'Fire is out'

if __name__ == "__main__":
    me = Oven([])
    me.get_state()
    try:
        while me.get_state() != 'DONE' and me.get_state() != 'ERROR':
            me.step()
    except:
        print 'what'
    print me.get_state()    
    print 'I\'m Done'
    
    
    
