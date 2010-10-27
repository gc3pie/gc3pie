class States(dict):
    
    def addstate(self, state, fun, type):
        self[state] = ((fun, type), [])
    
    def addtran(self, start_state, end_state, fun):
        assert start_state in self,  'State State \'%s\' is not in the state dictionary.'%(start_state)
        assert end_state in self,  'End State \'%s\' is not in the state dictionary.'%(end_state)
        self[start_state][1].append((end_state, fun))
    
    @property
    def states(self):
        return self.keys()
    
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
        G=pgv.AGraph(strict=False,directed=True)
        G.graph_attr['label']=doc_name + ' State Machine Diagram'
        G.graph_attr['fontcolor']='blue'
        G.add_nodes_from(self.states)
        for start_state, transition in self.transitions.iteritems():
            for pair in transition:
                G.add_edge(start_state, pair[0], label=pair[1].__name__)
        fullpath = os.path.expanduser(path + '/file.png')
        G.draw(fullpath, prog='dot')
        return fullpath


if __name__ == '__main__':
    def fun():
        return True
    me=States()
    for i in xrange(10):
        me.addstate('state%d'%(i),'fun','type')
    for i in xrange(5):
        me.addtran('state%d'%(i),'state%d'%(i+1), fun)
    me.addtran('state9','state8', fun)
    me.addtran('state9','state7', fun)
    me.display()
    
