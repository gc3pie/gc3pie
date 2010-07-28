__all__ = ['lbfgs', 'fire']

import cStringIO as StringIO
import pickle

class Optimize(object):
    
    def initialize(self):
        pass
    def step(self, positions,  f):
        pass
    
    def dump(self):
        f_like = StringIO.StringIO()
        pickle.dump(self, f_like, protocol=2)
        return f_like
    
    def load(self, f_like):
        self = pickle.load(f_like)
