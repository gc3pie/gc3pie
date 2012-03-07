class States(dict):
    
    def addstate(self, state, fun, type, color):
        self[state] = ((fun, type, color), [])
    
    def addtran(self, start_state, end_state, fun):
        assert start_state in self,  'State State \'%s\' is not in the state dictionary.'%(start_state)
#        assert end_state in self,  'End State \'%s\' is not in the state dictionary.'%(end_state)
        self[start_state][1].append((end_state, fun))
    
    def validate(self):
        states = self.states
        for state, trans in self.transitions.iteritems():
            for tran in trans:
                assert tran[0] in states, 'Transition End State \'%s\' is not in the state dictionary.'%(tran[0])
    
    @property
    def states(self):
        just_states = {}
        for key, value in self.iteritems():
            just_states[key] = value[0]
        return just_states
    
    @property
    def types(self):
        types = {}
        for key, val in self.iteritems():
            types[key] = val[0][1]
        return types
    
    @property
    def transitions(self):
        transitions = {}
        for key, val in self.iteritems():
            transitions[key] = val[1]
        return transitions
    
    def getstate_fun(self, *args, **kwargs):
        got = super(States, self).get(*args, **kwargs)
        if got:
            return got[0][0]
    
    def gettran(self, *args, **kwargs):
        got = super(States, self).get(*args, **kwargs)
        if got:
            return got[1]
    
    def display(self, doc_name='', path='~'):
        import pygraphviz as pgv
        import os
        from htpie.statemachine import StatePrint
        G=pgv.AGraph(strict=False,directed=True, rankdir='TD')
        G.graph_attr['label']=doc_name + ' Diagram'
        G.graph_attr['fontcolor']='blue'
        # By creating a subgraph we can rank the nodes
        # nodes in this subgraph will be given the same hight.
        subgraph = G.add_subgraph('samerank')
        subgraph.graph_attr['rank']='same'
        for state, val in self.states.iteritems():
            if val[2] == StatePrint.START:
                subgraph.add_node(state, **val[2])
            else:
                G.add_node(state, **val[2])
        for start_state, transition in self.transitions.iteritems():
            for pair in transition:
                G.add_edge(start_state, pair[0], label=' ' + pair[1].__name__ + '    ')
        fullpath = os.path.expanduser(path + '/%s.png'%doc_name )
        G.draw(fullpath, prog='dot')
        return fullpath

