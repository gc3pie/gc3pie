import sys
import numpy as np
import markstools
import cPickle as pickle

class LBFGS(object):
    """Limited memory BFGS optimizer.
    
    A limited memory version of the bfgs algorithm. Unlike the bfgs algorithm
    used in bfgs.py, the inverse of Hessian matrix is updated.  The inverse
    Hessian is represented only as a diagonal matrix to save memory

    """
    def __init__(self, atoms, maxstep=None, memory=100, damping = 1.0, alpha = 70.0):
        """
        Parameters:

        restart: string
            Pickle file used to store vectors for updating the inverse of Hessian
            matrix. If set, file with such a name will be searched and information
            stored will be used, if the file exists.

        maxstep: float
            How far is a single atom allowed to move. This is useful for DFT
            calculations where wavefunctions can be reused if steps are small.
            Default is 0.04 Angstrom.

        memory: int
            Number of steps to be stored. Default value is 100. Three numpy
            arrays of this length containing floats are stored.

        damping: float
            The calculated step is multiplied with this number before added to
            the positions. 

        alpha: float
            Initial guess for the Hessian (curvature of energy surface). A
            conservative value of 70.0 is the default, but number of needed
            steps to converge might be less if a lower value is used. However,
            a lower value also means risk of instability.
            
        """

        if maxstep is not None:
            if maxstep > 1.0:
                raise ValueError('You are using a much too large value for ' +
                                 'the maximum step size: %.1f Angstrom' % maxstep)
            self.maxstep = maxstep
        else:
            self.maxstep = 0.04

        self.memory = memory
        self.H0 = 1. / alpha  # Initial approximation of inverse Hessian
                            # 1./70. is to emulate the behaviour of BFGS
                            # Note that this is never changed!
        self.damping = damping

    def initialize(self):
        """Initalize everything so no checks have to be done in step"""
        self.iteration = 0
        self.s = []
        self.y = []
        self.rho = [] # Store also rho, to avoid calculationg the dot product
                      # again and again

        self.r0 = None
        self.f0 = None

    def step(self, positions,  f):
        """Take a single step
        
        Use the given forces, update the history and calculate the next step --
        then take it"""
        r = positions
    
        self.update(r, f, self.r0, self.f0)

        s = self.s
        y = self.y
        rho = self.rho
        H0 = self.H0

        loopmax = np.min([self.memory, self.iteration])
        a = np.empty((loopmax,), dtype=np.float64)

        ### The algorithm itself:
        q = - f.reshape(-1) 
        for i in range(loopmax - 1, -1, -1):
            a[i] = rho[i] * np.dot(s[i], q)
            q -= a[i] * y[i]
        z = H0 * q
        
        for i in range(loopmax):
            b = rho[i] * np.dot(y[i], z)
            z += s[i] * (a[i] - b)

        dr = - z.reshape((-1, 3))
        ###

        dr = self.determine_step(dr) * self.damping
        new_positions = r + dr
        
        self.iteration += 1
        # Here is where the data is dumped to a file for use when restarting the optimizer
        self.r0 = r.copy() 
        self.f0 = f.copy()
        return new_positions
    
    def dump(self, filename):
        to_dump = (self.iteration, self.s, self.y, 
               self.rho, self.r0, self.f0)
        try:
            myfile = open(filename, 'wb')
            pickle.dump(to_dump, myfile, protocol=2)
        finally:
            myfile.close()

    def load(self, filename):
        """Load saved arrays to reconstruct the Hessian"""
        try:
            myfile = open(filename, 'rb')
            self.iteration, self.s, self.y, self.rho, self.r0, self.f0 = pickle.load(myfile)
        finally:
            myfile.close()

    def determine_step(self, dr):
        """Determine step to take according to maxstep
        
        Normalize all steps as the largest step. This way
        we still move along the eigendirection.
        """
        steplengths = (dr**2).sum(1)**0.5
        longest_step = np.max(steplengths)
        if longest_step >= self.maxstep:
            dr *= self.maxstep / longest_step
        
        return dr

    def update(self, r, f, r0, f0):
        """Update everything that is kept in memory

        This function is mostly here to allow for replay_trajectory.
        """
        if self.iteration > 0:
            s0 = r.reshape(-1) - r0.reshape(-1)
            self.s.append(s0)

            # We use the gradient which is minus the force!
            y0 = f0.reshape(-1) - f.reshape(-1)
            self.y.append(y0)
            
            rho0 = 1.0 / np.dot(y0, s0)
            self.rho.append(rho0)

        if self.iteration > self.memory:
            self.s.pop(0)
            self.y.pop(0)
            self.rho.pop(0)

class LineSearchLBFGS(LBFGS):
    """Modified version of LBFGS.

    This optimizer uses the LBFGS algorithm, but does a line search for the
    minimum along the search direction. This is done by issuing an additional
    force call for each step, thus doubling the number of calculations.

    Additionally the Hessian is reset if the new guess is not sufficiently
    better than the old one.
    """
    def __init__(self, *args, **kwargs):
        self.dR = kwargs.pop('dR', 0.1)         
        LBFGS.__init__(self, *args, **kwargs)

    def update(self, r, f, r0, f0):
        """Update everything that is kept in memory

        This function is mostly here to allow for replay_trajectory.
        """
        if self.iteration > 0:
            a1 = abs(np.dot(f.reshape(-1), f0.reshape(-1)))
            a2 = np.dot(f0.reshape(-1), f0.reshape(-1))
            if not (a1 <= 0.5 * a2 and a2 != 0):
                # Reset optimization
                self.initialize()

        # Note that the reset above will set self.iteration to 0 again
        # which is why we should check again
        if self.iteration > 0:
            s0 = r.reshape(-1) - r0.reshape(-1)
            self.s.append(s0)

            # We use the gradient which is minus the force!
            y0 = f0.reshape(-1) - f.reshape(-1)
            self.y.append(y0)
            
            rho0 = 1.0 / np.dot(y0, s0)
            self.rho.append(rho0)

        if self.iteration > self.memory:
            self.s.pop(0)
            self.y.pop(0)
            self.rho.pop(0)

    def determine_step(self, dr):
        f = self.atoms.get_forces()
        
        # Unit-vector along the search direction
        du = dr / np.sqrt(np.dot(dr.reshape(-1), dr.reshape(-1)))

        # We keep the old step determination before we figure 
        # out what is the best to do.
        maxstep = self.maxstep * np.sqrt(3 * len(self.atoms))

        # Finite difference step using temporary point
        self.atoms.positions += (du * self.dR)
        # Decide how much to move along the line du
        Fp1 = np.dot(f.reshape(-1), du.reshape(-1))
        Fp2 = np.dot(self.atoms.get_forces().reshape(-1), du.reshape(-1))
        CR = (Fp1 - Fp2) / self.dR
        #RdR = Fp1*0.1
        if CR < 0.0:
            #print "negcurve"
            RdR = maxstep
            #if(abs(RdR) > maxstep):
            #    RdR = self.sign(RdR) * maxstep
        else:
            Fp = (Fp1 + Fp2) * 0.5
            RdR = Fp / CR 
            if abs(RdR) > maxstep:
                RdR = np.sign(RdR) * maxstep
            else:
                RdR += self.dR * 0.5
        return du * RdR
