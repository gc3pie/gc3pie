import numpy as np
import copy
import markstools
import ResultBase

def queryable(func):
    func.queryable=True
    return func

class GamessResult(ResultBase):
    
    def __init__(self,  atoms, params, parsed_dat, parsed_out):
        super(GamessResult, self).__init__( atoms, params)
        self.parsed_dat  = parsed_dat
        self.parsed_out = parsed_out
    
    def get_positions(self):
        raw_coords=self.parsed_dat.get_coords()
        coords = np.array(raw_coords[1::2], dtype=float)
        return coords
    
    def get_orbitals(self, raw=False):
        """In GAMESS the $VEC group contains the orbitals."""
        if raw:
            return self.parsed_dat.get_vec()
        return np.array(self.parsed_dat.get_vec(), dtype=float)
        
    def get_hessian(self, raw=False):
        """In GAMESS the $HES group contains the Hessian."""
        if raw:
            return self.parsed_dat.get_hess()
        return np.array(self.parsed_dat.get_hess(), dtype=float)

    def get_forces(self):
        """This returns the gradients."""
        grad = self.parsed_dat.get_forces()
        mat = np.array(np.zeros((len(grad), 3)), dtype=float)
        for i in range(0, len(grad)):
            mat[i] = grad[i][1]
        return mat
    
    @queryable
    def get_potential_energy(self):
        return float(self.parsed_dat.get_energy())
    
    @queryable
    def exit_successful(self):
        return self.parsed_out.is_exit_successful()
    
    @queryable
    def geom_located(self):
        return self.parsed_out.is_geom_located()
    
    def _get_queryable(self):
        queryable = dict()
        for name in dir(self):
            obj = getattr(self, name)
            if hasattr(obj, 'queryable'):
                queryable[name]= obj()
        return queryable
