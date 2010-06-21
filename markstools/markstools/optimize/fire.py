import numpy as np
import markstools
from markstools import optimize

class FIRE(Optimize):
    def __init__(self, dt=0.1, maxmove=0.2, dtmax=1.0, Nmin=5, finc=1.1, fdec=0.5,
                 astart=0.1, fa=0.99, a=0.1):

        self.dt = dt
        self.Nsteps = 0
        self.maxmove = maxmove
        self.dtmax = dtmax
        self.Nmin = Nmin
        self.finc = finc
        self.fdec = fdec
        self.astart = astart
        self.fa = fa
        self.a = a

    def initialize(self):
        self.v = None

    def step(self, positions, f):
        if self.v is None:
            self.v = np.zeros(positions.shape)
        else:
            vf = np.vdot(f, self.v)
            if vf > 0.0:
                self.v = (1.0 - self.a) * self.v + self.a * f / np.sqrt(
                    np.vdot(f, f)) * np.sqrt(np.vdot(self.v, self.v))
                if self.Nsteps > self.Nmin:
                    self.dt = min(self.dt * self.finc, self.dtmax)
                    self.a *= self.fa
                self.Nsteps += 1
            else:
                self.v[:] *= 0.0
                self.a = self.astart
                self.dt *= self.fdec
                self.Nsteps = 0
#            if vf < 0.0:
#                self.v[:] = 0.0
#                self.a = self.astart
#                self.dt *= self.fdec
#                self.Nsteps = 0
#            else:
#                self.v = (1.0 - self.a) * self.v + self.a * f * np.sqrt(
#                    np.vdot(f, f) / np.vdot(self.v, self.v))
#                if self.Nsteps > self.Nmin:
#                    dt = min(dt * self.finc, dtmax)
#                    self.a *= self.fa
#                    self.Nsteps += 1

            self.v += self.dt * f
            dr = self.dt * self.v
            normdr = np.sqrt(np.vdot(dr, dr))
            if normdr > self.maxmove:
                dr = self.maxmove * dr / normdr
            r = positions
            new_positions = r + dr
            return new_positions

######################################################

#from tsse.util import vdot,vmag,vunit,vproj
#from tsse.printf import printf
#from tsse import current
#from minimizer import minimizer
#
#class fire(minimizer):
#
#    def __init__(self,p=current.p, pot=current.pot, maxmove=0.2,dt=0.1,dtmax=1.0,Nmin=5,finc=1.1,fdec=0.5,astart=0.1,fa=0.99):
#        """
#        Fire initializer function, called in script before min
#        Optional arguments:
#          dt:       initial dynamical timestep
#          dtmax:    maximum timestep
#          Nmin:     ???
#          finc:     ???
#          fdec:     ???
#          astart:   ???
#          fa:       ???
#          maxmove:  maximum amount the point can move during optimization
#        """
#
#        minimizer.__init__(self, p, pot)
#        self.maxmove=maxmove
#        self.dt=dt
#        self.dtmax=dtmax
#        self.Nmin=Nmin
#        self.finc=finc
#        self.fdec=fdec
#        self.astart = astart
#        self.a=astart
#        self.fa=fa
#        self.v=p.r*0
#        self.Nsteps=0
#        
#    def step(self):
#        """
#        Fire step
#        """
#
#
#        self.f(self.p)
#        Power = vdot(self.p.f,self.v)
#        if(Power>0.0):
#            self.v = (1.0-self.a)*self.v+self.a*vmag(self.v)*vunit(self.p.f)
#            if(self.Nsteps>self.Nmin):
#                self.dt = min(self.dt*self.finc,self.dtmax)
#                self.a *= self.fa
#            self.Nsteps+=1
#        else:
#            # reset velocity and slow down
#            self.v *= 0.0
#            self.a = self.astart
#            self.dt *= self.fdec
#            self.Nsteps = 0
#
#
#        # Euler step
#        self.v+=self.dt*self.p.f
#        dR=self.dt*self.v
#        # check for max step
#        if(vmag(dR)>self.maxmove):
#            dR=self.maxmove*vunit(dR)
##          print'maxmove'
#    # move R
#        self.p.r+=dR
