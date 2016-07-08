#! /usr/bin/env python

from graphviz import Digraph

desc = [#('UNKNOWN',       ()),
        ('NEW',           ('SUBMITTED', 'RUNNING')),
        ('SUBMITTED',     ('RUNNING',)),
        ('RUNNING',       ('TERMINATING', 'STOPPED',)),
        ('TERMINATING',   ('TERMINATED',)),
        ('TERMINATED',    ()),
        ('STOPPED',       ())]

for n in range(len(desc)):
    refstate = desc[n][0]
    g = Digraph(comment='GC3Pie Application states: ' + refstate)
    for k, (state, transitions) in enumerate(desc):
        attrs = { 'shape': 'box' }
        if not transitions and state != 'UNKNOWN':
            attrs['rank'] = 'max'
            attrs['shape'] = 'house'
        if n == k:
            attrs['style'] = 'filled'
            attrs['fillcolor'] = 'antiquewhite'
        g.node(state, **attrs)
        for next_state in transitions:
            g.edge(state, next_state)
            #if state != 'UNKNOWN':
            #    g.edge(state, 'UNKNOWN', constraint='false')

    out = 'states-' + refstate
    g.render(out, view=False, cleanup=True)
