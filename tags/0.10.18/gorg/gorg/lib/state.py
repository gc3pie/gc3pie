class dotdict(dict):
    '''Used to make a dictionary in to a dot dictionary.'''
    def __getattr__(self, attr):
        if attr in self:
            return self.get(attr, None)
        else:
            raise AttributeError('does not contain %s'%attr)
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
    
    @staticmethod
    def enum(obj):
        return obj['index']
    
    @staticmethod
    def create(index, description, locked=None, terminal=False):
        return State(index=index, description = description, locked=locked, terminal=terminal)


class StateContainer(dotdict):
    def __init__(self, states):
        indexes = map(State.enum, states)
        self.update(zip(indexes,states))
    
    @property
    def all(self):
        states = list()
        for index in self:
            states.append(self[index])
        return states
    
    @property
    def locked(self):
        states = list()
        for index in self:
            if self[index].locked:
                states.append(self[index])
        return states
    
    @property
    def terminal(self):
        states = list()
        for index in self:
            if self[index].terminal:
                states.append(self[index])
        return states



class StContainer(object):

    def __init__(self, states, user_class = None):
        self.states = dict()
        for a_state in states:
            self.states[a_state.index] = a_state
        self.user_class = user_class
    
    def __getattr__(self, name):
        if name in self.states:
            return  self.states[name]
        else:
            return getattr(self, name) 
    
    @property
    def all(self):
        return self.states
    
    @property
    def locked(self):
        locked_states = list()
        for a_state in states:
            if a_state.locked:
                locked_states.append(a_state)
        return tuple(states)
    
    @property
    def terminal(self):
        terminal_states = list()
        for a_state in states:
            if a_state.terminal:
                terminal_states.append(a_state)
        return tuple(states)
