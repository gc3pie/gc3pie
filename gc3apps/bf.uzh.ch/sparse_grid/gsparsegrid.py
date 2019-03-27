#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011-2012  University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
__author__ = 'Riccardo Murri <riccardo.murri@gmail.com>'
# summary of user-visible changes
__changelog__ = """
  2012-11-06:
    * Initial draft.
"""
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == '__main__':
from __future__ import absolute_import, print_function
    import gsparsegrid
    gsparsegrid.SparseGridScript().run()


# stdlib imports
import itertools
import os
import os.path
import string
import sys


## interface to Gc3libs
import gc3libs
from gc3libs import Application, Run, Task
import gc3libs.exceptions
from gc3libs.cmdline import SessionBasedScript, executable_file, nonnegative_int
import gc3libs.utils



class SparseGridApplication(Application):
    application_name = 'sgrid'
    def __init__(self, np, total_memory, fnum, dimension, lmax, epsilon,
                 executable=None, **extra_args):
        """
        Execute one instance of the 'sparse grid' application.

        Parameters are as follows:

        :param int   np:        Number of processors to use
        :param int   fnum:      Test function number [1-6]
        :param int   dimension: 1 -120
        :param int   Lmax:
        :param float epsilon: e.g., 1e-3, 1e-4, 1e-5
        """
        extra_args['requested_cores'] = np
        extra_args['requested_memory'] = total_memory

        # build Application object
        gc3libs.Application.__init__(
            self,
            executable = '',
            arguments = [
                # std MPI startup
                'mpiexec', '-n', np,
                # actual `sgrid` invocation
                "$SGRID_EXEC", fnum, dimension, lmax, epsilon
                ],
            inputs = [], # no input files
            outputs = [], # no output files (except stdout/stderr)
            join = True,
            stdout = 'sgrid.log',
            tags = [ 'TEST/SGRID-0.1' ],
            **extra_args
            )


class SparseGridScript(SessionBasedScript):
    """
    Sample parameter-study script.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            )


    def setup_options(self):
        self.add_param("--fnum", action="store", dest="fnum",default='1',
                       help="Select function no. to test [1-6].")
        self.add_param("--dimension", action="store", dest="dimension",default='2',
                       help="Grid dimension.")
        self.add_param("--lmax", action="store", dest="lmax",default='3',
                       help="Select Lmax")
        self.add_param("--epsilon", action="store", dest="epsilon",default='1.e-3',
                       help="Select epsilon.")
        self.add_param("--mpi_cores", action="store", dest="mpi_cores",default='10',
                       help="Select number of cores to use. ")


    def new_tasks(self, extra):

        fnums = [ int(fnum) for fnum in self.params.fnum.split(',') ]
        dims = [ int(dim) for dim in self.params.dimension.split(',') ]
        lmaxs = [ int(lmax) for lmax in self.params.lmax.split(',') ]
        epsilons = [ float(epsilon) for epsilon in self.params.epsilon.split(',') ]
        ncores = [ int(ncore) for ncore in self.params.mpi_cores.split(',') ]

        # create one task per combination of the input parameters
        for fnum, dim, lmax, epsilon,ncore in itertools.product(fnums, dims, lmaxs, epsilons, ncores):
            jobname = ("sgrid_fnum=%d_dim=%d_Lmax=%d_epsilon=%g_mpi_cores=%d" % (fnum, dim, lmax, epsilon,ncore))

            extra_args = extra.copy()
            extra_args['output_dir'] = os.path.join(
                self.make_directory_path(self.params.output, jobname), 'output')
            extra_args['jobname'] = jobname

            # passed to Application as `requested_cores` in SparseGridApplication
            np = ncore
            # Compute total memory needed for a job with np cores. 
            total_memory = self.params.memory_per_core * np
            yield SparseGridApplication(np, total_memory, fnum, dim, lmax, epsilon, **extra_args)
