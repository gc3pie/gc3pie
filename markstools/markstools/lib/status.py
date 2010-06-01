
class State(object):
    def __init__(self, name, desc, pause = False, terminal = False):
        self.name = name
        self.description = desc
        self.pause = pause
        self.terminal = terminal
    
    def __repr__(self):
        return self.description
    @staticmethod
    def name(obj):
        return obj.name

class Status(object):
    def __init__(self, states):
        self.state_names = map(State.name, states)
        self.__dict__.update(zip(self.state_names, states))
    
    def match(self, usertask_name):
        return self.__dict__[usertask_name]
       
    @property
    def all(self):
        states = list()
        for a_state_name in self.state_names:
            states.append(self.__dict__[a_state_name])
        return states
    
    @property
    def pause(self):
        states = list()
        for a_state_name in self.state_names:
            if self.__dict__[a_state_name].pause:
                states.append(self.__dict__[a_state_name])
        return states
    
    @property
    def terminal(self):
        states = list()
        for a_state_name in self.state_names:
            if self.__dict__[a_state_name].terminal:
                states.append(self.__dict__[a_state_name])
        return states
