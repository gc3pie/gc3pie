class CalculatorBase(object):
    """Base class for calculators. 
    
    We might want a calculator that uses the grid and another one that
    runs the applicationon the local computer. Both calculators need to implement 
    the same functions, only how the application is run, and where the result files
   are located would be different.
   """
    def __init__(self):
        pass

    def preexisting_result(self, location):
        assert False, 'Must implement a preexisting_result method'
    
    def get_files(self):
        assert False,  'Must implement a get_files method'
    
    def wait(self, job_id, status='DONE', timeout=60, check_freq=10):
        assert False,  'Must implement a wait method'
    
    def save_queryable(self, job_id, key, value):
        assert False,  'Must implement a push method'

class ResultBase(object):
    def __init__(self, atoms, params):
        import copy
        self.atoms = atoms.copy()
        self.params = copy.deepcopy(params)
