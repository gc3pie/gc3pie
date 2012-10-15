#! /usr/bin/env python
"""
A general script for performing an global optimization over the parameter space. 
This code is an adaptation of the following MATLAB code: http://www.icsi.berkeley.edu/~storn/DeMat.zip
Please refer to this web site for more information: http://www.icsi.berkeley.edu/~storn/code.html#deb1
"""

import logging

class EAOptimizer:

    self.population = [] # current population
    self.iteration = 0
    self.size = len(self.population)
    self.fitness_vectors = []
    def __init__(self):
        self.logger = logging.getLogger('gc3.branches.EAoptimizer')

    def initialize_population(self):
        #Modify self.population
        
	pass
    def has_converged(self):
        return True

    def evaluate(self, pop):
	# For each indivdual in self.population evaluate individual
	return fitness_vector

    def select(self, fitness_vec, pop):
	pass # return a matrix of size self.size

    # a list of modified population, for example mutated, recombined, etc. 
    def modify(self, offspring):
        return modified_population # a mixture of different variations 

    def main_loop(self):
	#Run GAMESS with input file from a command line
        initial_population = initialize_population(self)
	population = initial_population # one matrix with all possible modifications of initial population

        while not self.has_converged():
            fitness_vector = self.evaluate(population)
            offspring =  self.select(fitness_vector, population)
            population = self.modify(offspring)
            self.get_statistics(fitness_vector)
	    self.iteration +=1

    def get_statistics(self, fitness_vector):
       # get the best individual from a population
       # get the mean individual from a population
	pass

class Optimizer_GC3(SequentialTaskCollection):
    def next():
	 
