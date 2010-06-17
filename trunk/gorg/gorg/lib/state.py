class dotdict(dict):
    '''Used to make a dictionary in to a dot dictionary.'''
    def __getattr__(self, attr):
        return self.get(attr, None)
    __setattr__= dict.__setitem__
    __delattr__= dict.__delitem__
    def __hash__(self):
        return hash(tuple(sorted(self.iteritems())))

class State(dotdict):
    def __init__(self, *arg, **kwarg):
        '''The couchdb-python wrapper calls this class and passes it a tuple as arg and nothing 
        in the kwarg. Therefore we need to check the arg value to see if anything is there.
        If something is there, we then need to retrieve the value field and pass that to our
        constructor. If no arg is present, then we know that we are creating the dictionary using 
        a set of kwargs.'''
        if arg:
            dict_of_values = arg[0].value
            self = super(State, self).__init__(**dict_of_values)
        else:
            self = super(State, self).__init__(**kwarg)
    def __repr__(self):
        return super(State, self).__repr__()
    
    def __str__(self):
        return self.description
    
    @property
    def view_key(self):
        return super(State, self).__str__()
    
    @staticmethod
    def enum(obj):
        return obj['index']
    
    @staticmethod
    def create(index, description, pause=False, terminal=False):
        return State(index=index, description = description, pause=pause, terminal=terminal)

class StateContainer(dotdict):
    def __init__(self, states):
        indexes = map(State.enum, states)
        self.update(zip(indexes, states))
    
    @property
    def all(self):
        states = list()
        for index in self:
            states.append(self[index])
        return states
    
    @property
    def pause(self):
        states = list()
        for index in self:
            if self[index].pause:
                states.append(self[index])
        return states
    
    @property
    def terminal(self):
        states = list()
        for index in self:
            if self[index].terminal:
                states.append(self[index])
        return states

'''Here are the default states that are used throughout the system.'''

DEFAULT_READY = State.create('READY', 'READY desc')
DEFAULT_KILL = State.create('KILL', 'KILL desc')
DEFAULT_HOLD = State.create('HOLD', 'HOLD desc', terminal = True)
DEFAULT_KILLED = State.create('KILLED', 'KILLED desc', terminal = True)
DEFAULT_ERROR = State.create('ERROR', 'ERROR desc', terminal = True)
DEFAULT_COMPLETED = State.create('COMPLETED', 'COMPLETED desc', terminal = True)

