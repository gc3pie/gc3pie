#!/usr/bin/env python

from statemachine import *

from optparse import OptionParser
import logging
import os
import copy
import sys
from utils import * 

from ase.io.gamess import ReadGamessInp
from ase.calculators.gamess import GamessGridCalc

sys.path.append('/home/mmonroe/apps/gorg')

from gorg_site.gorg_site.lib.mydb import Mydb




def main():
    
        program_name = sys.argv[0]
        # todo : do we need this?  does the parser automatically know to look at the arguments?
        sysargs = sys.argv
        
        options, args = ParseOptions(program_name, sysargs)

        # Configure logging service
        logger = CreateBasicLogger(options.verbosity)
        
        if options.input == None:
            logging.info('No input file specified.  Exiting.')
            sys.exit(1)
           
        if options.application == None:
            logging.info('No application specified.  Exiting.')
            sys.exit(1)
        
        if options.tasktype == None:
            logging.info('No tasktype specified.  Exiting.')
            sys.exit(1)

        if options.resource == None:
            logging.info('No resource specified.  Exiting.')
            sys.exit(1)
        
        # todo : get these from config file or elsewhere later
        if options.ncores == None:
            options.ncores = 2 
        if options.memory_per_core == None:
            options.memory_per_core = 1 
        if options.walltime == None:
            options.walltime = -1 
        
        
        # Check input file.  Can be made fancy later.
        try:
            CheckInputFile(options.input)
        except:
            raise
        
        db = Mydb('mark',options.db_name,options.db_loc).cdb()
        
        print options.tasktype

        # Call a different method depending on the type of task we want to create.
  
        if options.tasktype == 'hessian':
            try:
                # Parse the gamess inp file
                myfile = open(options.file, 'rb')
                reader = ReadGamessInp(myfile)
                myfile.close()
                params = reader.params
                atoms = reader.atoms
                
                fsm = GHessian(options.logging_level)
                gamess_calc = GamessGridCalc(db)
                fsm.start(db, gamess_calc, atoms, params)
                fsm.run()
                a_task = fsm.save_state()
                print a_task.id
            except:
                raise     
        elif options.tasktype == 'single':
            try:
                print 'wow'
                from gsingle import GSingle
                # Parse the gamess inp file
                inputfile = open(options.input, 'rb')
                single_fsm = GSingle(options.verbosity)
                single_fsm.start(db, inputfile, options.application, options.resource,  options.ncores, options.memory_per_core, options.walltime)
                inputfile.close()
                single_fsm.run()
                a_task = single_fsm.save_state()
                print a_task.id
            except:
                raise           
        elif options.tasktype == 'restart':
            try:
                myfile = open(options.file, 'rb')
                reader = ReadGamessInp(myfile)
                myfile.close()
                params = reader.params
                atoms = reader.atoms
                
                fsm = GRestart(options.logging_level)
                gamess_calc = GamessGridCalc(db)
                fsm.start(db, gamess_calc, atoms, params)
                fsm.run()
                a_task = fsm.save_state()
                print a_task.id
            except:
                raise
        else:
            print 'unknown task type'
            logging.error('unknown task type')
            sys.exit(1)     

    

if __name__ == '__main__':
    #Setup logger
    main()

    sys.exit()
