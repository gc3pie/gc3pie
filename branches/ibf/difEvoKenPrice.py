import numpy as np
import sys
import logbook


class deKenPrice:

  def __init__(self, evaluator, paraStruct):
    #self.evaluator = evaluator
    #self.paraStruct = paraStruct
  #  logger.debug('hello')
    self.deopt(evaluator, paraStruct)


  def deopt(self, evaluator, S_struct):
    mySH = logbook.StreamHandler(stream = sys.stdout, format_string = '{record.message}', bubble = True)
    mySH.format_string = '{record.message}'
#    mySH.push_application()
    myFH = logbook.FileHandler(filename = __name__ + '.log', bubble = True)
    myFH.format_string = '{record.message}'
#    myFH.push_application()  
    
    logger = logbook.Logger(__name__)
    logger.handlers.append(mySH)
    logger.handlers.append(myFH)


    # This is just for notational convenience and to keep the code uncluttered.--------
    I_NP         = S_struct['I_NP']
    F_weight     = S_struct['F_weight']
    F_CR         = S_struct['F_CR']
    I_D          = S_struct['I_D']
    FVr_minbound = S_struct['FVr_minbound']
    FVr_maxbound = S_struct['FVr_maxbound']
    I_bnd_constr = S_struct['I_bnd_constr']
    I_itermax    = S_struct['I_itermax']
    F_VTR        = S_struct['F_VTR']
    I_strategy   = S_struct['I_strategy']
    I_refresh    = S_struct['I_refresh']
    I_plotting   = S_struct['I_plotting']


    # -----Check input variables---------------------------------------------
    if ( I_NP < 5 ):
      I_NP = 5
      logger.debug(' I_NP increased to minimal value 5')
    if ( ( F_CR < 0 ) or ( F_CR > 1 ) ):
      F_CR = 0.5
      logger.debug('F_CR should be from interval [0,1]; set to default value 0.5')
    if (I_itermax <= 0):
      I_itermax = 200
      logger.debug('I_itermax should be > 0; set to default value 200')
    I_refresh = int(np.floor(I_refresh))

    FM_pop = np.zeros( (I_NP, I_D ) ) #initialize FM_pop to gain speed

    #----FM_pop is a matrix of size I_NPx(I_D+1). It will be initialized------
    #----with random values between the min and max values of the-------------
    #----parameters-----------------------------------------------------------

    for k in range(I_NP):
      FM_pop[k,:] = FVr_minbound + np.random.random_sample( I_D ) * ( FVr_maxbound - FVr_minbound )

    #FM_pop = np.array([ 
      #[ 1.319957946440394e+00,    -1.165839645622266e+00 ],
      #[ -6.724549608245165e-01,     -4.639978119989765e-01 ],
      #[ -1.409217096906199e+00,     1.847624402221629e+00 ],
      #[ 5.427463056680506e-01 ,   -7.596186531735287e-01 ],
      #[ 1.679970533773739e+00 ,   -1.804249448721254e+00],
      #[-8.175685096139325e-01 ,    2.173201742531194e-02],
      #[-1.492203275397329e+00 ,    1.037499849971578e+00],
      #[ 1.876282657576228e+00 ,   -1.818419930729171e+00],
      #[ 1.335308227914194e+00 ,   -1.338661207044213e+00],
      #[-1.369443437970318e+00 ,    1.975408022527302e+00],
      #[ 2.475692913962151e-01 ,   -1.033780640566921e+00],
      #[ 1.175851331267991e+00 ,    2.306833852465773e-01],
      #[ 1.908197810754837e+00 ,    1.457438249288829e+00],
      #[ 1.135802936242739e+00,     1.247455109814998e+00],
      #[-1.612526445636986e-01 ,   -2.857917995562982e-01],
      #[ 4.251051381044095e-01 ,   -1.454564515409778e+00],
      #[ 1.147542433326465e+00 ,   -1.224992337000308e+00],
      #[-1.949474655450793e+00  ,   6.477607240023482e-01],
      #[ 8.410566181531665e-01 ,    2.122898920279215e-01],
      #[-1.976175947497272e+00,     1.709960562977214e+00 ]
      #])

    FM_popold     = np.zeros( np.size(FM_pop) )  # toggle population
    FVr_bestmem   = np.zeros( I_D )# best population member ever
    FVr_bestmemit = np.zeros( I_D )# best population member in iteration
    I_nfeval      = 0                    # number of function evaluations  

    # Evaluate target for the first time
    S_vals = evaluator.target(FM_pop)
    # # # #  # # # # # # ##  # # ## # # 

    if not isinstance(S_vals[0], dict):
      new_S_vals = []
      for ixS, S_val in enumerate(S_vals):
        new_S_vals.append({'FVr_oa': [ S_val ], 'I_nc': 0, 'FVr_ca': 0, 'I_no': 1})
      S_vals = new_S_vals


    for k in range(I_NP):                          # check the remaining members
      if k == 0:
        S_bestval = S_vals[0]                 # best objective function value so far
        I_nfeval  = I_nfeval + 1
        I_best_index  = 0
      I_nfeval  += 1
      if ( left_win( S_vals[k], S_bestval ) == 1 ):
        I_best_index   = k              # save its location
        S_bestval      = S_vals[k]
    FVr_bestmemit = FM_pop[I_best_index, :] # best member of current iteration
    S_bestvalit   = S_bestval              # best value of current iteration

    FVr_bestmem = FVr_bestmemit            # best member ever


  #------DE-Minimization---------------------------------------------
  #------FM_popold is the population which has to compete. It is--------
  #------static through one iteration. FM_pop is the newly--------------
  #------emerging population.----------------------------------------

    FM_pm1   = np.zeros( (I_NP, I_D) )   # initialize population matrix 1
    FM_pm2   = np.zeros( (I_NP, I_D) )   # initialize population matrix 2
    FM_pm3   = np.zeros( (I_NP, I_D) )   # initialize population matrix 3
    FM_pm4   = np.zeros( (I_NP, I_D) )   # initialize population matrix 4
    FM_pm5   = np.zeros( (I_NP, I_D) )   # initialize population matrix 5
    FM_bm    = np.zeros( (I_NP, I_D) )   # initialize FVr_bestmember  matrix
    FM_ui    = np.zeros( (I_NP, I_D) )   # intermediate population of perturbed vectors
    FM_mui   = np.zeros( (I_NP, I_D) )   # mask for intermediate population
    FM_mpo   = np.zeros( (I_NP, I_D) )   # mask for old population
    FVr_rot  = np.arange(0, I_NP, 1)    # rotating index array (size I_NP)
    FVr_rotd = np.arange(0, I_D, 1)     # rotating index array (size I_D)
    FVr_rt   = np.zeros(I_NP)            # another rotating index array
    FVr_rtd  = np.zeros(I_D)                 # rotating index array for exponential crossover
    FVr_a1   = np.zeros(I_NP)                # index array
    FVr_a2   = np.zeros(I_NP)                # index array
    FVr_a3   = np.zeros(I_NP)                # index array
    FVr_a4   = np.zeros(I_NP)                # index array
    FVr_a5   = np.zeros(I_NP)                # index array
    FVr_ind  = np.zeros(4)

    FM_meanv = np.ones( (I_NP, I_D ) )


    ### Iter  
    I_iter = 0
    while ( ( I_iter < I_itermax ) and ( S_bestval['FVr_oa'][0] > F_VTR)):
      FM_popold               = FM_pop                  # save the old population
      S_struct['FM_pop']      = FM_pop
      S_struct['FVr_bestmem'] = FVr_bestmem

      FVr_ind = np.random.permutation(4)             # index pointer array


      #FVr_ind = [ 2,3, 1, 0]

      FVr_a1  = np.random.permutation(I_NP)                   # shuffle locations of vectors
      #FVr_a1  = np.array([ 15,   7,     8,     5,    10,    18,     6,    14,    16,    12,    13,     2,     9,    11,     4,     1,    19,     3,
                  #20, 17 ]) - 1
      FVr_rt  = ( FVr_rot + FVr_ind[0] ) % I_NP     # rotate indices by ind(1) positions
      FVr_a2  = FVr_a1[FVr_rt]                 # rotate vector locations
      FVr_rt  = ( FVr_rot + FVr_ind[1] ) % I_NP
      FVr_a3  = FVr_a2[FVr_rt]                
      FVr_rt  = ( FVr_rot + FVr_ind[2] ) % I_NP
      FVr_a4  = FVr_a3[FVr_rt]                
      FVr_rt  = ( FVr_rot + FVr_ind[3] ) % I_NP
      FVr_a5  = FVr_a4[FVr_rt]                


      FM_pm1 = FM_popold[FVr_a1, :]             # shuffled population 1
      FM_pm2 = FM_popold[FVr_a2, :]             # shuffled population 2
      FM_pm3 = FM_popold[FVr_a3, :]             # shuffled population 3
      FM_pm4 = FM_popold[FVr_a4, :]             # shuffled population 4
      FM_pm5 = FM_popold[FVr_a5, :]             # shuffled population 5


      for k in range(I_NP):                              # population filled with the best member
        FM_bm[k,:] = FVr_bestmemit                       # of the last iteration

      FM_mui = np.random.random_sample( (I_NP, I_D ) ) < F_CR  # all random numbers < F_CR are 1, 0 otherwise

      #----Insert this if you want exponential crossover.----------------
      #FM_mui = sort(FM_mui')	  # transpose, collect 1's in each column
      #for k  = 1:I_NP
      #  n = floor(rand*I_D)
      #  if (n > 0)
      #     FVr_rtd     = rem(FVr_rotd+n,I_D)
      #     FM_mui(:,k) = FM_mui(FVr_rtd+1,k) #rotate column k by n
      #  end
      #end
      #FM_mui = FM_mui'			  # transpose back
      #----End: exponential crossover------------------------------------

      FM_mpo = FM_mui < 0.5    # inverse mask to FM_mui

      if ( I_strategy == 1 ):                             # DE/rand/1
        FM_ui = FM_pm3 + F_weight * ( FM_pm1 - FM_pm2 )   # differential variation
        FM_ui = FM_popold * FM_mpo + FM_ui * FM_mui       # crossover
        FM_origin = FM_pm3
      elif (I_strategy == 2):                         # DE/local-to-best/1
        FM_ui = FM_popold + F_weight * ( FM_bm - FM_popold ) + F_weight * ( FM_pm1 - FM_pm2 )
        FM_ui = FM_popold * FM_mpo + FM_ui * FM_mui
        FM_origin = FM_popold
      elif (I_strategy == 3):                         # DE/best/1 with jitter
        FM_ui = FM_bm + ( FM_pm1 - FM_pm2 ) * ( (1 - 0.9999 ) * np.random.random_sample( (I_NP, I_D ) ) +F_weight )               
        FM_ui = FM_popold * FM_mpo + FM_ui * FM_mui
        FM_origin = FM_bm
      elif (I_strategy == 4):                         # DE/rand/1 with per-vector-dither
        f1 = ( ( 1 - F_weight ) * np.random.random_sample( (I_NP, 1 ) ) + F_weight)
        for k in range(I_D):
          FM_pm5[:,k] = f1
        FM_ui = FM_pm3 + (FM_pm1 - FM_pm2) * FM_pm5    # differential variation
        FM_origin = FM_pm3
        FM_ui = FM_popold * FM_mpo + FM_ui * FM_mui     # crossover
      elif (I_strategy == 5):                          # DE/rand/1 with per-vector-dither
        f1 = ( ( 1 - F_weight ) * np.random.random_sample() + F_weight )
        FM_ui = FM_pm3 + ( FM_pm1 - FM_pm2 ) * f1         # differential variation
        FM_origin = FM_pm3
        FM_ui = FM_popold * FM_mpo + FM_ui * FM_mui   # crossover
      else:                                              # either-or-algorithm
        if (np.random.random_sample() < 0.5):                               # Pmu = 0.5
          FM_ui = FM_pm3 + F_weight * ( FM_pm1 - FM_pm2 )# differential variation
          FM_origin = FM_pm3
        else:                                           # use F-K-Rule: K = 0.5(F+1)
          FM_ui = FM_pm3 + 0.5 * ( F_weight + 1.0 ) * ( FM_pm1 + FM_pm2 - 2 * FM_pm3 )
          FM_ui = FM_popold * FM_mpo + FM_ui * FM_mui     # crossover  


    #-----Optional parent+child selection-----------------------------------------

    #-----Select which vectors are allowed to enter the new population------------
      for k in range(I_NP):

        #=====Only use this if boundary constraints are needed==================
        if ( I_bnd_constr == 1 ):
          for j in range(I_D): #----boundary constraints via bounce back-------
            if ( FM_ui[k, j] > FVr_maxbound[j] ):
              FM_ui[k,j] = FVr_maxbound[j] + np.random.random_sample() * ( FM_origin[k, j] - FVr_maxbound[j] )
            if ( FM_ui[k, j] < FVr_minbound[j] ):
              FM_ui[k, j] = FVr_minbound[j] + np.random.random_sample() * ( FM_origin[k,j] - FVr_minbound[j] )
        #=====End boundary constraints==========================================

      # EVALUATE TARGET #
      S_tempvals = evaluator.target(FM_ui)
      # # # # # # # # # #

      if not isinstance(S_tempvals[0], dict):
        new_S_vals = []
        for ixS, S_val in enumerate(S_tempvals):
          new_S_vals.append({'FVr_oa': [ S_val ], 'I_nc': 0, 'FVr_ca': 0, 'I_no': 1})
        S_tempvals = new_S_vals
      
      logger.debug(FM_ui)
      logger.debug([ S_tempvals[ix]['FVr_oa'] for ix in range(len(S_vals)) ])

      for k in range(I_NP):
        I_nfeval  = I_nfeval + 1
        if ( left_win( S_tempvals[k], S_vals[k] ) == 1 ):   
          FM_pop[k,:] = FM_ui[k, :]                    # replace old vector with new one (for new iteration)
          S_vals[k]   = S_tempvals[k]                      # save value in "cost array"

          #----we update S_bestval only in case of success to save time-----------
          if ( left_win( S_tempvals[k], S_bestval ) == 1 ):
            S_bestval = S_tempvals[k]                    # new best value
            FVr_bestmem = FM_ui[k,:]                 # new best parameter vector ever

      FVr_bestmemit = FVr_bestmem       # freeze the best member of this iteration for the coming 
                                          # iteration. This is needed for some of the strategies.

    #----Output section----------------------------------------------------------

      if ( I_refresh > 0 ):
        if ( ( I_iter % I_refresh == 0 ) or I_iter == 1 ):
#          logger.debug('Iteration: %d,  Best: %f,  F_weight: %f,  F_CR: %f,  I_NP: %d', I_iter, S_bestval['FVr_oa'][0], F_weight, F_CR, I_NP)
#          print >> logFile, 'Iteration: %d,  Best: %f,  F_weight: %f,  F_CR: %f,  I_NP: %d' % (I_iter, S_bestval['FVr_oa'][0], F_weight, F_CR, I_NP)
#          logFile.flush()
          #var(FM_pop)
          logger.debug('Iteration: %d,  Best: %f,  F_weight: %f,  F_CR: %f,  I_NP: %d' % (I_iter, S_bestval['FVr_oa'][0], F_weight, F_CR, I_NP))
          for n in range(I_D):
#            logger.debug('best(%d) = %g',n,FVr_bestmem[n])
#            print >> logFile, 'best(%d) = %g' % (n,FVr_bestmem[n])
#            logFile.flush()
            logger.debug('best(%d) = %g' % (n,FVr_bestmem[n]))
          if ( I_plotting == 1 ):
            pass
            #PlotIt(FVr_bestmem,I_iter,S_struct) 
      I_iter += 1
    
    logger.debug('exiting ' + __name__)
##    myFH.pop_application()
##    mySH.pop_application()
    logger.handlers = []



def left_win(S_x, S_y):
  I_z = 1  #start with I_z=1

  #----deal with the constraints first. If constraints are not met------
  #----S_x can't win.---------------------------------------------------
  if ( S_x['I_nc'] > 0 ):
    for k in range(1,S_x.I_nc):
      if (S_x['FVr_ca'][k] > 0): #if constraint is not yet met
        if (S_x['FVr_ca'][k] > S_y['FVr_ca'][k]): #if just one constraint of S_x is not improved
          I_z = 0

  if ( S_x['I_no'] > 0 ):
    for k in range(S_x['I_no']):
      if (S_x['FVr_oa'][k] > S_y['FVr_oa'][k]):#if just one objective of S_x is less
        I_z = 0

  return I_z

class Rosenbrock:
  def __init__(self):
    #********************************************************************
    # Script file for the initialization and run of the differential 
    # evolution optimizer.
    #********************************************************************

    # F_VTR		"Value To Reach" (stop when ofunc < F_VTR)
    F_VTR = 1.e-8 

    # I_D		number of parameters of the objective function 
    I_D = 2 

    # FVr_minbound,FVr_maxbound   vector of lower and bounds of initial population
    #    		the algorithm seems to work especially well if [FVr_minbound,FVr_maxbound] 
    #    		covers the region where the global minimum is expected
    #               *** note: these are no bound constraints!! ***
    FVr_minbound = -2 * np.ones(I_D) 
    FVr_maxbound = +2 * np.ones(I_D) 
    I_bnd_constr = 0  #1: use bounds as bound constraints, 0: no bound constraints      

    # I_NP            number of population members
    I_NP = 20  #pretty high number - needed for demo purposes only

    # I_itermax       maximum number of iterations (generations)
    I_itermax = 200 

    # F_weight        DE-stepsize F_weight ex [0, 2]
    F_weight = 0.85 

    # F_CR            crossover probabililty constant ex [0, 1]
    F_CR = 1

    # I_strategy     1 --> DE/rand/1:
    #                      the classical version of DE.
    #                2 --> DE/local-to-best/1:
    #                      a version which has been used by quite a number
    #                      of scientists. Attempts a balance between robustness
    #                      and fast convergence.
    #                3 --> DE/best/1 with jitter:
    #                      taylored for small population sizes and fast convergence.
    #                      Dimensionality should not be too high.
    #                4 --> DE/rand/1 with per-vector-dither:
    #                      Classical DE with dither to become even more robust.
    #                5 --> DE/rand/1 with per-generation-dither:
    #                      Classical DE with dither to become even more robust.
    #                      Choosing F_weight = 0.3 is a good start here.
    #                6 --> DE/rand/1 either-or-algorithm:
    #                      Alternates between differential mutation and three-point-
    #                      recombination.           

    I_strategy = 1

    # I_refresh     intermediate output will be produced after "I_refresh"
    #               iterations. No intermediate output will be produced
    #               if I_refresh is < 1
    I_refresh = 3

    # I_plotting    Will use plotting if set to 1. Will skip plotting otherwise.
    I_plotting = 1

    #-----Problem dependent constant values for plotting----------------

    #if (I_plotting == 1):
            #FVc_xx = [-2:0.125:2]
            #FVc_yy = [-1:0.125:3]
            #[FVr_x,FM_y]=meshgrid(FVc_xx',FVc_yy') 
            #FM_meshd = 100.*(FM_y-FVr_x.*FVr_x).^2 + (1-FVr_x).^2 

            #S_struct.FVc_xx    = FVc_xx
            #S_struct.FVc_yy    = FVc_yy
            #S_struct.FM_meshd  = FM_meshd 
    #end

    S_struct = {}
    S_struct['I_NP']         = I_NP
    S_struct['F_weight']     = F_weight
    S_struct['F_CR']         = F_CR 
    S_struct['I_D']          = I_D 
    S_struct['FVr_minbound'] = FVr_minbound
    S_struct['FVr_maxbound'] = FVr_maxbound
    S_struct['I_bnd_constr'] = I_bnd_constr
    S_struct['I_itermax']    = I_itermax
    S_struct['F_VTR']        = F_VTR
    S_struct['I_strategy']   = I_strategy
    S_struct['I_refresh']    = I_refresh
    S_struct['I_plotting']   = I_plotting

    deKenPrice(self, S_struct)

  def target(self, vectors):

    S_MSEvec = []
    for vector in vectors:
      #---Rosenbrock saddle-------------------------------------------
      F_cost = 100 * ( vector[1] - vector[0]**2 )**2 + ( 1 - vector[0] )**2

      #----strategy to put everything into a cost function------------
      S_MSE = {}
      S_MSE['I_nc']      = 0 #no constraints
      S_MSE['FVr_ca']    = 0 #no constraint array
      S_MSE['I_no']      = 1 #number of objectives (costs)
      S_MSE['FVr_oa'] = []
      S_MSE['FVr_oa'].append(F_cost)
      S_MSEvec.append(S_MSE)

    return S_MSEvec




if __name__ == '__main__':

  Rosenbrock()