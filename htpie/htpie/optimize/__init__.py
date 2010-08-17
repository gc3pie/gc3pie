__all__ = ['lbfgs', 'fire']

from htpie.lib import utils
import pickle


class Optimize(object):
    
    def initialize(self):
        pass

    def step(self, positions,  f):
        pass
    

def dump(opt, f_container):
    try:
        a_file = utils.verify_file_container(f_container)
        pickle.dump(opt, a_file, protocol=2)
    finally:
        a_file.close()

def load(f_container):
    try:
        a_file = utils.verify_file_container(f_container)
        opt = pickle.load(a_file)
    finally:
        a_file.close()
    return opt

