
import numpy, scipy, scipy.stats.stats, sys


def fn_var_to_markov(A0,A1,A2,SIGMA,N,random_draws,method):
  '''

  GENERALIZED MARKOV APPROXIMATIONS TO VAR PROCESSES
 
  This function converts a VAR to a discretized Markov process,
  generalizing the approach in Tauchen (1986) by allowing for more general
  var./cov. structure in the error term.  The required multivariate normal
  probabilities are calculated using a Monte Carlo-type technique
  implemented in the function qscmvnv.m, developed by and available on the
  website of Alan Genz: http://www.math.wsu.edu/faculty/genz/homepage.
 
  Original VAR: A0*Z(t) = A1 + A2*Z(t-1) + e(t), e(t) ~ N(0,SIGMA)
 
  INPUTS:
  1 - A0, A1, A2 are the VAR coefficients, as indicated above, with
      A0 assumed non-singular
  2 - N is n x 1, where n = # of vars, N(i) = # grid points for ith var.
  3 - SIGMA is the arbitrary positive semi-definite error var./cov. matrix
  4 - random_draws is the number of random draws used in the required
      Monte Carlo-type integration of the multivariate normal
  5 - method switch determines the grid selection method
        - method = 1 uses a uniformly spaced grid covering a fixed number
          of std. dev. of the relevant component variables.  This is the
          grid spacing strategy proposed in Tauchen (1986).
        - method = 2 selects grid points based on approximately equal
          weighting from the UNIVARIATE normal cdf.  This method is adapted
          from code written by Jonathan Willis.  (Note that method = 2
          requires the use of the MATLAB statistics toolbox.)
 
  OUTPUTS:
  1 - Pr_mat is the Prod(N) x Prod(N) computed transition probability matrix
      for the discretized Markov chain
  2 - Pr_mat_key is n x Prod(N) matrix s.t. if Z* is the discretized Markov
      approximation to the VAR Z, then Z*(state i) = Pr_mat_key(:,i)
  3 - zbar is the n x max(N) matrix s.t. zbar(i,1:N(i)) is the univariate
      grid for the ith component of Z*
 
  03/17/08 - Stephen Terry, function adapted from earlier code by Ed Knotek
  03/18/08 - Stephen Terry, "method=2" capability added, adapting earlier
             functions from Jonathan Willis
  03/21/08 - Stephen Terry, rounding correction and zbar output added
  '''
  # make sure method = 1 is called
  if method == 2:
    print 'method 2 not translated, see matlab code. Exiting... '
    sys.exit()
    
  # Convert input to array
  A0 = numpy.asarray(A0)
  A1 = numpy.asarray(A1)
  A2 = numpy.asarray(A2)
  SIGMA = numpy.asarray(SIGMA)
  N = numpy.asarray(N)

  #  number of variables in VAR
  n = numpy.size(N) 
  
  # compute reduced form parameters & steady-state mean
  A1bar = numpy.dot(numpy.linalg.inv(A0), A1)
  A2bar = numpy.dot(numpy.linalg.inv(A0), A2)
  SIGMAbar = numpy.dot(numpy.dot(numpy.linalg.inv(A0), SIGMA), (numpy.linalg.inv(A0).T))
  
  sstate_mean = numpy.dot(numpy.linalg.inv(numpy.eye(n) - A2bar), A1bar)
  
  m = 2 # number std deviations of the VAR covered by grid
  
  # iterate to obtain var./cov. structure of the PROCESS (not error term)
  SIGMAprocess = SIGMAbar.copy()
  SIGMAprocess_last = SIGMAprocess.copy()
  dif = 1
  while dif > 0.00000001:
    SIGMAprocess = numpy.dot(numpy.dot(A2bar, SIGMAprocess_last), (A2bar.T)) + SIGMAbar
    dif = numpy.max(numpy.max(SIGMAprocess-SIGMAprocess_last))
    SIGMAprocess_last = SIGMAprocess.copy()
  
  #This block equally spaces grid points bounded by m*(std.deviation of
  #process) on either side of the unconditional mean of the process.  Any
  #more sophisticated spacing of the grid points could be implemented by
  #changing the definitio
  #n of zbar below.
  zbar = numpy.zeros( (n,numpy.max(N)) )
  grid_stdev = numpy.diag(SIGMAprocess) ** 0.5
  if method == 1:
    grid_increment = numpy.zeros(n)
    for i in range (n):
      grid_increment[i] = 2*m*grid_stdev[i]/(N[i]-1)
      zbar[i,0] = -m*grid_stdev[i] + sstate_mean[i]
      for j in range(N[i]-1):
        zbar[i,j + 1] = zbar[i,j] + grid_increment[i]
  elif method == 2: # untested, compare matlab code
    d = numpy.zeros( (n, numpy.max(N)) )
    b = numpy.arange(-4, 4, .005)
    c = scipy.stats.norm.cdf(b,0,1)
    for i in range(n):
      a = numpy.arange((1/(2*N[i])), 1, (1/N(i)))
      for j in range(N[i]):
        [d1,d[i,j]] = numpy.min((a[j]-c) ** 2)
      zbar[i,:N[i]] = grid_stdev[i]*b[d[i,:]]+sstate_mean[i]

  
  #compute key matrix & pos matrix
  Pr_mat_key = numpy.zeros( (len(N), numpy.prod(N)) )
  Pr_mat_key_pos = numpy.zeros( (len(N), numpy.prod(N)) )
  Pr_mat_key[len(N)-1,:] = numpy.tile(zbar[len(N)-1,:N[len(N)-1]], (1, (numpy.prod(N)/N[len(N)-1])[0] ) )
  Pr_mat_key_pos[len(N)-1, :] = numpy.tile(numpy.arange(N[len(N)-1]), (1, (numpy.prod(N)/N[len(N)-1])[0]) )
  for i in range(len(N)-2, -1, -1):
    Pr_mat_key[i,:] = numpy.tile(numpy.kron(zbar[i,:N[i]], numpy.ones( ( 1, numpy.prod(N[i+1:len(N)]) ) ) ), (1, numpy.prod(N)/numpy.prod(N[i:len(N)])))
    Pr_mat_key_pos[i,:] = numpy.tile(numpy.kron(numpy.arange(N[i]), numpy.ones( (1, numpy.prod(N[i+1:len(N)]) ))), (1, numpy.prod(N)/numpy.prod(N[i:len(N)])))
    
  nstate = numpy.prod(N)
  Pr_mat_intervals = numpy.zeros( (n,nstate,2) ) # this will store the unadjusted limits of integration for each variable in each state, for input into the Genz code
  if method == 1:
    for i in range(nstate):  #  number of states
      for j in range(n):     # number of variables
        if Pr_mat_key_pos[j,i] == 0:
          Pr_mat_intervals[j,i,0] = -numpy.inf
          Pr_mat_intervals[j,i,1] = zbar[j,Pr_mat_key_pos[j,i]] + (grid_increment[j]/2)
        elif Pr_mat_key_pos[j,i] == N[j] - 1:
          Pr_mat_intervals[j,i,0] = zbar[j,Pr_mat_key_pos[j,i]] - (grid_increment[j]/2)
          Pr_mat_intervals[j,i,1] = numpy.inf
        else:
          Pr_mat_intervals[j,i,0] = zbar[j,Pr_mat_key_pos[j,i]]- (grid_increment[j]/2)
          Pr_mat_intervals[j,i,1] = zbar[j,Pr_mat_key_pos[j,i]] + (grid_increment[j]/2)
  elif method == 2:
    for i in range(nstate):  #  number of states
      for j in range(n):     # number of variables
        if Pr_mat_key_pos[j,i] == 0:
          Pr_mat_intervals[j,i,0] = -numpy.inf
          Pr_mat_intervals[j,i,1] = zbar[j,Pr_mat_key_pos[j,i]] + (zbar[j,Pr_mat_key_pos[j,i]+1]-zbar[j,Pr_mat_key_pos[j,i]])/2
        elif Pr_mat_key_pos[j,i] == N[j] - 1:
          Pr_mat_intervals[j,i,0] = zbar[j,Pr_mat_key_pos[j,i]] - (zbar[j,Pr_mat_key_pos[j,i]]-zbar[j,Pr_mat_key_pos[j,i]-1])/2
          Pr_mat_intervals[j,i,1] = numpy.inf
        else:
          Pr_mat_intervals[j,i,0] = zbar[j,Pr_mat_key_pos[j,i]] - (zbar[j,Pr_mat_key_pos[j,i]]-zbar[j,Pr_mat_key_pos[j,i]-1])/2
          Pr_mat_intervals[j,i,1] = zbar[j,Pr_mat_key_pos[j,i]] + (zbar[j,Pr_mat_key_pos[j,i]+1]-zbar[j,Pr_mat_key_pos[j,i]])/2;

  error_est = numpy.zeros( (nstate,nstate) )
  Pr_mat_intervals_adjusted = numpy.zeros( (n,nstate,2) )
  Pr_mat = numpy.zeros( (nstate,nstate) )
  for i in range(nstate): # ; rows of Pr_mat
    Pr_mat_intervals_adjusted[:,:,0] = Pr_mat_intervals[:,:,0] - numpy.tile((A1bar.T + numpy.dot(A2bar, Pr_mat_key[:,i])).T,(1,nstate))
    Pr_mat_intervals_adjusted[:,:,1] = Pr_mat_intervals[:,:,1] - numpy.tile((A1bar.T + numpy.dot(A2bar, Pr_mat_key[:,i])).T,(1,nstate))
    for j in range(nstate):  # columns of Pr_mat
      #Pr_mat(i,j) = P(state j|state i)
      [Pr_mat[i,j], error_est[i,j]] = qscmvnv(random_draws,SIGMAbar,Pr_mat_intervals_adjusted[:,j,0],numpy.eye(n),Pr_mat_intervals_adjusted[:,j,1])
  
  # rounding error adjustment
  round_sum = numpy.sum(Pr_mat,1)
  for i in range(numpy.size(Pr_mat,1)):
    Pr_mat[i,:] = Pr_mat[i,:]/round_sum[i]

  return [Pr_mat, Pr_mat_key, zbar]



def qscmvnv( m, r, a, cn, b ):
  '''
    [ P E ] = QSCMVNV( M, R, A, CN, B )
      uses a randomized quasi-random rule with m points to estimate an
      MVN probability for positive semi-definite covariance matrix r,
      with constraints a < cn*x < b. If r is nxn and cn is kxn, then
      a and b must be column k-vectors.
     Probability p is output with error estimate e.
      Example usage:
       >> r = [ 4 3 2 1; 3 5 -1 1; 2 -1 4 2; 1 1 2 5 ];
       >> a = [ -inf 1 -5 ]'; b = [ 3 inf 4 ]';
       >> cn = [ 1 2 3 -2; 2 4 1 2; -2 3 4 1 ];
       >> [ p e ] = qscmvnv( 5000, r, a, cn, b ); disp([ p e ])
  
    This function uses an algorithm given in the paper by Alan Genz:
     "Numerical Computation of Multivariate Normal Probabilities", in
       J. of Computational and Graphical Stat., 1(1992), 141-149.
    The primary references for the numerical integration are 
     "On a Number-Theoretical Integration Method"
       H. Niederreiter, Aequationes Mathematicae, 8(1972), 304-11, and
     "Randomization of Number Theoretic Methods for Multiple Integration"
       R. Cranley and T.N.L. Patterson, SIAM J Numer Anal, 13(1976), 904-14.
  
     Alan Genz is the author of this function and following Matlab functions.
            Alan Genz, WSU Math, PO Box 643113, Pullman, WA 99164-3113
            Email : AlanGenz@wsu.edu
  '''
  # Make deep copies of input arrays
  #m = m.copy()
  r = r.copy()
  a = a.copy()
  cn = cn.copy()
  b = b.copy()
  
  numpy.random.seed(5489)
  # Initialization
  x = 3
  asVar, ch, bs, clg, n = chlsrt( r, a, cn, b )
  ci = phi(asVar[0])
  dci = phi(bs[0]) - ci
  p = 0
  e = 0
  ns = 8
  nv = numpy.fix( numpy.max( numpy.vstack( (m/( 2*ns ), 1) ) ) )
  q = 2**( (numpy.arange(n) + 1)/ (n+1.)) # Niederreiter point set generators
  #
  # Randomization loop for ns samples
  #
  xx = numpy.zeros((n, nv))
  for i in range(ns):
    # periodizing transformation 
    xx[:,:nv+1] = numpy.abs( 2*numpy.mod( numpy.kron(q.reshape(q.shape[0], -1), numpy.arange(1, nv + 1)) + numpy.random.rand(n,1)*numpy.ones(nv), 1 ) - 1 )
    vp =   mvndnv( n, asVar, ch, bs, clg, ci, dci,   xx, nv )
    vp = ( mvndnv( n, asVar, ch, bs, clg, ci, dci, 1-xx, nv ) + vp )/ 2. # symmetrize
    d = ( numpy.mean(vp) - p )/ (i + 1)
    p = p + d
    if numpy.abs(d) > 0:
      e = numpy.abs(d)* numpy.sqrt( 1. + ( e/d )**2 *( i - 1)/ ( i + 1) )
    else:
      if i > 1:
        e = e* numpy.sqrt( ( i - 2 )/i )
  e = 3*e # error estimate is 3 x standard error with ns samples.
    
  return [ p, e ]
  
def mvndnv( n, a, ch, b, clg, ci, dci, x, nv ):
  '''

  Transformed integrand for computation of MVN probabilities. 

  '''
  y = numpy.zeros( (n,nv) )
  on = numpy.ones( (1,nv) )
  c = ci*on
  dc = dci*on
  p = dc
  li = 1
  lf = 0
  for i in range(1, n + 1):
    y[i-1,:] = phinv( c + x[i-1,:] * dc )
    lf = lf + clg[i] 
    if lf < li:
      c = 0
      dc = 1
    else:
      s = numpy.dot(ch[li:lf+1,:i], y[:i,:])
      ai = numpy.maximum( numpy.max( a[li:lf+1]*on - s, 0 ), -9 * numpy.ones(nv) ) 
      bi = numpy.maximum( ai, numpy.minimum( numpy.min( b[li:lf+1]*on - s, 0 ),  9 * numpy.ones(nv) ) ) 
      c = phi(ai)
      dc = phi(bi) - c
      p = p * dc 
    li = li + clg[i]
  return p
  
def chlsrt( r, a, cn, b ):
  '''
    Computes permuted lower Cholesky factor ch for covariance r which 
     may be singular, combined with contraints a < cn*x < b, to
     form revised lower triangular constraint set ap < ch*x < bp; 
     clg contains information about structure of ch: clg(1) rows for 
     ch with 1 nonzero, ..., clg(np) rows with np nonzeros.
  '''
  ep = 1e-10 # singularity tolerance
  #n = numpy.size(r) 
  m = numpy.size(cn, 0) 
  n = numpy.size(cn, 1)
  ch = cn 
  np = 0
  ap = a 
  bp = b 
  y = numpy.zeros( (n) ) 
  sqtp = numpy.sqrt(2 * numpy.pi)
  c = r
  d = numpy.sqrt(numpy.max(numpy.vstack( (numpy.diag(c),numpy.zeros( (numpy.diag(c).shape) )) ), 0 ))
  for i in range(n):
    di = float(d[i])
    if di > 0:
      c[:,i] = c[:,i]   / di
      c[i,:] = c[i,:]   / di
      ch[:,i] = ch[:,i] * di
      
  #
  # determine (with pivoting) Cholesky factor for r 
  #  and form revised constraint matrix ch
  #
  clg = numpy.zeros( n, dtype = int )  

  for i in range(n):
    clg[i] = 0
    epi = ep* i ** 2
    j = i 
    for l in range(i + 1, n):
      if c[l, l] > c[j, j]:
        j = l
    if j > i:
      t = c[i, i].copy()
      c[i, i] = c[j,j]
      c[j, j] = t
      t = c[i, :i].copy()
      c[i, :i] = c[j,:i]
      c[j, :i] = t
      t = c[i+1:-1,i].copy()
      c[i+1:j,i]= c[j,i+1:-1]
      c[j,i+1:j] = t
      t = c[j+1:n,i].copy()
      c[j+1:n+1,i] = c[j+1:n,j]
      c[j+1:n+1,j] = t
      t = ch[:,i].copy()
      ch[:,i] = ch[:,j]
      ch[:,j] = t
    if c[i,i] < epi:
      break
    cvd = numpy.sqrt( c[i, i] )
    c[i,i] = cvd
    for l in range(i+1, n):
      c[l,i] = c[l,i] / cvd
      c[l,i+1:l+1] = c[l,i+1:l+1] - numpy.dot(c[l,i], c[i+1:l+1,i])
    ch[:,i] = numpy.dot(ch[:,i:n+1], c[i:n+1,i])
    np += 1
    
    #
    # use right reflectors to reduce ch to lower triangular
    #
  for i in range(min( np-1, m )):
    epi = ep*i*i
    vm = 1 
    lm = i
    #
    # permute rows so that smallest variance variables are first.
    #
    for l in range(i, m):
      v = ch[l,:np] 
      s = numpy.dot(v[:i], y[:i])
      ss = numpy.max( numpy.sqrt( numpy.sum( v[i:np] ** 2 ) ), epi ) 
      al = ( ap[l] - s ) / ss
      bl = ( bp[l] - s ) / ss 
      dna = 0
      dsa = 0
      dnb = 0
      dsb = 1
      if al > -9:
        dna = numpy.exp(-al*al/2)/sqtp
        dsa = phi(al)
      if bl <  9:
        dnb = numpy.exp(-bl*bl/2)/sqtp
        dsb = phi(bl)
      if dsb - dsa > epi:
        if al <= -9:
          mn = -dnb
          vr = -bl*dnb
        elif  bl >=  9:
          mn = dna
          vr = al*dna 
        else:
          mn = dna - dnb
          vr = al*dna - bl*dnb
        mn = mn/( dsb - dsa )
        vr = 1 + vr/( dsb - dsa ) - mn**2
      else:
        if     al <= -9:
          mn = bl
        elif bl >=  9:
          mn = al
        else:
          mn = ( al + bl )/2
        vr = 0;
      if vr <= vm:
        lm = l
        vm = vr
        y[i] = mn 
    v = ch[lm, :np+1].copy()
    if lm > i:
      ch[lm,:np+1] = ch[i,:np+1]
      ch[i,:np+1] = v.copy()
      tl = ap[i].copy()
      ap[i] = ap[lm]
      ap[lm] = tl
      tl = bp[i].copy()
      bp[i] = bp[lm]
      bp[lm] = tl
    ch[i,i+1:np+1] = 0
    ss = numpy.sum( v[i+1:np+1] ** 2 )
    if ( ss > epi ):
      ss = numpy.sqrt( ss + v[i] ** 2 )
      if v[i] < 0:
        ss = -ss
      ch[i,i] = -ss
      v[i] = v[i] + ss
      vt = v[i:np + 1] / ( ss*v[i] )
      try:
        chVtDotProd = numpy.dot(ch[i+1:m+1,i:np+1], vt)
        chVtDotProd = chVtDotProd.reshape(chVtDotProd.shape[0], -1)
        ch[i+1:m + 1,i:np+1] = ch[i+1:m+1,i:np+1] - numpy.kron(chVtDotProd, v[i:np+1])
      except:
        pass
  #
  # scale and sort constraints
  #
  clm = numpy.zeros( (m), dtype = int )
  for i in range(m):
    v = ch[i,:np+1].copy()
    clm[i] = min(i,np)
    jm = 0
    for j in range(clm[i] + 1):
      if numpy.abs(v[j]) > ep*(j+1)*(j+1):
        jm = j
    if jm < np:
      v[jm+1:np+1] = 0
    clg[jm] = clg[jm] + 1
    at = ap[i].copy()
    bt = bp[i].copy()
    j = i
    for l in range(i-1, -1, -1):
      if jm >= clm[l]:
        break
      ch[l+1,:np+1] = ch[l,:np+1]
      j = l
      ap[l+1] = ap[l]
      bp[l+1] = bp[l]
      clm[l+1] = clm[l]
    clm[j] = jm
    vjm = v[jm].copy()
    ch[j,:np+1] = v/vjm 
    ap[j] = at/vjm
    bp[j] = bt/vjm
    if vjm < 0:
      tl = ap[j].copy()
      ap[j] = bp[j]
      bp[j] = tl
  j = 0
  for i in range(np):
    if clg[i] > 0:
      j= i
  np = j
  #
  # combine constraints for first variable
  #
  if clg[1] > 1:
    ap[1] = numpy.max( ap[:clg[1] + 1] )
    bp[1] = numpy.max( ap[1], numpy.min( bp[:clg[1]+1] ) ) 
    ap[1:m-clg[1]+1] = ap[clg[0]+1:m+1]
    bp[1:m-clg[1]+1] = bp[clg[0]+1:m+1]
    ch[1:m-clg[1]+1,:] = ch[clg[0]+1:m+1,:]
    clg[1] = 1
  return [ ap, ch, bp, clg, np ]
  #
  # end chlsrt
  #

def phi(z):
  '''
    Standard statistical normal distribution cdf
  '''
  return scipy.special.erfc( -z/numpy.sqrt(2) )/2

def phinv(w):
  '''
    Standard statistical inverse normal distribution
  '''
  return -numpy.sqrt(2)*scipy.special.erfcinv( 2*w )



if __name__ == '__main__': 
  numpy.set_printoptions(precision = 4, suppress = True)
  r = numpy.array([ [4, 3, 2, 1], [3, 5, -1, 1], [2, -1, 4, 2], [1, 1, 2, 5] ], dtype = float)
  a = numpy.array([ -numpy.inf, 1, -5 ], dtype = float)
  b = numpy.array([ 3, numpy.inf, 4 ], dtype = float)
  cn = numpy.array([ [1, 2, 3, -2], [2, 4, 1, 2],  [-2, 3, 4, 1] ], dtype = float)
  print r
  print a
  print b
  print cn
  print '---'
  [ p, e ] = qscmvnv( 5000, r, a, cn, b )
  print p
  print e