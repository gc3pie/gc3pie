"""
The nudged elastic band (neb) module.
"""
import htpie
from htpie.lib import utils

import numpy as np
import copy
from math import sqrt, atan, pi
import ase


# Vector projection
def vproj(v1, v2):
    """
    Returns the projection of v1 onto v2
    Parameters:
        v1, v2: numpy vectors
    """
#    mag2 = sqrt((v2 * v2).sum())
    mag2 = vmag(v2)
    if mag2 == 0:
        print("Can't project onto a zero vector")
        return v1
    return v2 * (vdot(v1, v2) / mag2)

# Unit vector
def vunit(v):
    """
    Returns the unit vector corresponding to v
    Parameters:
        v:  the vector to normalize
    """
    mag = vmag(v)
    if mag == 0:
        print("Can't normalize a zero vector")
        return v
    return v / mag

# Vector magnitude
def vmag(v):
    """
    Returns the magnitude of v
    """
    return sqrt((v * v).sum())

# Vector dot product
def vdot(v1, v2):
    """
    Returns the dot product of v1 and v2
    """
    return (v1 * v2).sum()

class Point(object):
    def __init__(self):
        #position
        self.r = None
        #energy
        self.u = None
        #force
        self.f = None
        #Freeze atoms
        self.free = None

def interpolate(p1, p2, numImages=8):
    path = [None] * numImages
    path[0] = copy.deepcopy(p1)
    path[-1] = copy.deepcopy(p2)
    
    # Do a linear interpolation between the starting and final images
    dR = (p2 - p1) / (numImages - 1.0) # -2.0 ?
    for i in range(1, numImages - 1):
        path[i] = copy.deepcopy(p1)
        path[i] += dR * i
    
    return path

class NEBParam(utils.InformationContainer):
    
    def __init__(self,initializer=None,**keywd):
        if not keywd.has_key('spring_constant'):
            keywd['spring_constant'] = 5.0
        if not keywd.has_key('tangent'):
            keywd['tangent'] = 'new'
        if not keywd.has_key('dneb'):
            keywd['dneb'] = False
        if not keywd.has_key('dnebOrg'):
            keywd['dnebOrg'] = False
        if not keywd.has_key('method'):
            keywd['method'] = 'normal'
        InformationContainer.__init__(self,initializer,**keywd)

class NEB(object):
    """
    The nudged elastic band (neb) class.
    """
    
    def __init__(self, spring_constant = 2.0, tangent = "new",       \
                 dneb = True, dnebOrg = False, method = 'normal'):
        """
        The neb constructor.
        Parameters:
            p1.......... one endpoint of the band
            p2.......... the other endpoint of the band
            numImages... the total number of images in the band, including the 
                         endpoints
            spring_constant........... the spring force constant (k)
            tangent..... the tangent method to use, "new" for the new tangent,
                         anything else for the old tangent
            dneb........ set to true to use the double-nudging method
            dnebOrg..... set to true to use the original double-nudging method
            method...... "ci" for the climbing image method, anything else for
                         normal NEB method 
        """
        
        self.tangent = tangent
        self.dneb = dneb
        self.dnebOrg = dnebOrg
        self.method = method
        self.spring_constant = spring_constant
        self.Umaxi = None
    
    def forces(self, positions, energies, forces):
        """
        Calculate the forces for each image on the band.  Applies the force due
        to the potential and the spring forces.
        Parameters:
            force - the potential energy force.
        """
        
        self.numImages = len(positions)
        self.k = self.spring_constant * self.numImages
        
        self.path = [Point() for i in xrange(self.numImages)]
        for i in xrange(self.numImages) :
            self.path[i].r = positions[i].copy()
            self.path[i].u = energies[i]
            self.path[i].f = forces[i].copy()
            self.path[i].free = np.ones(forces[i].shape)
        
        # Calculate the force due to the potential on the intermediate points.
        self.Umax = self.path[0].u
        self.Umaxi = 0
        for i in range(1, self.numImages - 1):
            #Record the image with the greatest energy
            if self.path[i].u > self.Umax:
                self.Umax = self.path[i].u
                self.Umaxi = i
            
        # Set the tangent direction for the endpoints to be from the endpoint
        # to its neightbor.
        self.path[0].n = self.path[1].r - self.path[0].r
        self.path[-1].n = self.path[-1].r - self.path[-2].r
        
        # Loop over each intermediate point and calculate the tangents.
        for i in range(1, self.numImages - 1):
            
            # If we're using the 'old' tangent, the tangent is defined as the
            # vector from the point behind the current image to the point in
            # front of the current image.
            if self.tangent == 'old':
                self.path[i].n = (self.path[i + 1].r - self.path[i - 1].r)
            
            # Otherwise, we're using the 'new' tangent.
            # Ref:
            # G. Henkelman and H. Jonsson,  Improved tangent estimate in the 
            # nudged elastic band method for finding minimum energy paths and 
            # saddle points, J. Chem. Phys. 113, 9978-9985 (2000)
            else:
            
                # it wouldn't hurt for these names to be more descriptive
                UPm1 = self.path[i - 1].u > self.path[i].u
                UPp1 = self.path[i + 1].u > self.path[i].u
                
                # if V(i+1)>V(i)>V(i-1)
                # or V(i+1)<V(i)<V(i-1)
                # (this is the usual along the MEP)
                if(UPm1 != UPp1):
                    if(UPm1):
                        self.path[i].n = self.path[i].r - self.path[i - 1].r
                    else:
                        self.path[i].n = self.path[i + 1].r - self.path[i].r
                # otherwise, we are near some extremum 
                else:
                    Um1 = self.path[i - 1].u - self.path[i].u
                    Up1 = self.path[i + 1].u - self.path[i].u
                    Umin = min(abs(Up1), abs(Um1))
                    Umax = max(abs(Up1), abs(Um1))
                    if(Um1 > Up1):
                        self.path[i].n = (self.path[i + 1].r - self.path[i].r)\
                                         * Umin
                        self.path[i].n += (self.path[i].r -                   \
                                           self.path[i - 1].r) * Umax
                    else:
                        self.path[i].n = (self.path[i + 1].r - self.path[i].r)\
                                         * Umax
                        self.path[i].n += (self.path[i].r -                   \
                                           self.path[i - 1].r) * Umin
        
        # Normalize the tangents.
        for i in range(self.numImages):
            self.path[i].n = vunit(self.path[i].n)
        
        # Loop over each intermediate image and adjust the potential energy 
        # force and apply the spring force.
        for i in range(1, self.numImages - 1):
        
            # Push the climbing image uphill.
            if self.method == 'ci' and i == self.Umaxi:
                self.path[i].f -= 2.0 * vproj(self.path[i].f, self.path[i].n)
            
            # And for the non-climbing images...
            else:
                
                # Calculate the force perpendicular to the tangent. 
                self.path[i].fPerp = self.path[i].f - vproj(self.path[i].f,   \
                                                            self.path[i].n)
                
                # Calculate the spring force.
                Rm1 = self.path[i - 1].r - self.path[i].r
                Rp1 = self.path[i + 1].r - self.path[i].r
                self.path[i].fsN = (vmag(Rp1) - vmag(Rm1)) * self.k *         \
                                   self.path[i].n
                
                # For dneb use total spring force -spring force in the grad
                # direction.
                if self.dneb:
                    self.path[i].fs = (Rp1 + Rm1) * self.k
                    self.path[i].fsperp = self.path[i].fs - self.k *          \
                                          vproj(Rp1 + Rm1, self.path[i].n)
                    self.path[i].fsdneb = self.path[i].fsperp -               \
                                          vproj(self.path[i].fs,              \
                                                self.path[i].fPerp)
                    
                    # New dneb where dneb force converges with (What?!)
                    if not self.dnebOrg:
                        FperpSQ = vdot(self.path[i].fPerp, self.path[i].fPerp)
                        FsperpSQ = vdot(self.path[i].fsperp,                  \
                                        self.path[i].fsperp)
                        if FsperpSQ > 0:
                            self.path[i].fsdneb *= 2.0 / pi * atan(FperpSQ /  \
                                                                   FsperpSQ)
                
                # Not using double-nudging, so set the double-nudging spring
                # force to zero.
                else:
                    self.path[i].fsdneb = 0
                
                # The final force is the sum of these forces.    
                self.path[i].f = self.path[i].fsdneb + self.path[i].fsN +     \
                                 self.path[i].fPerp
                
                # And freeze the appropriate atoms.
                '''path[i].free is normally set to 1's to freeze, just need to set the force to 0' '''
                self.path[i].f *= self.path[i].free
