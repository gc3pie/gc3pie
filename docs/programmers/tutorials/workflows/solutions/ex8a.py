#! /usr/bin/env python

"""
Write a `pricetplot.py` script that performs the following two steps:

  1. Run the `simAsset.R` script (from Exercise 6.D) with the parameters
    given on the command line, and

  2. Feed the `results.csv` file it outputs into the `saplot.py` script and
    retrieve the produced `saplot.pdf` file.
"""

import os
from os.path import join

from gc3libs import Application
from gc3libs.cmdline import SessionBasedScript
from gc3libs.workflow import SequentialTaskCollection


if __name__ == '__main__':
    from ex8a import PricePlotScript
    PricePlotScript().run()


class PricePlotScript(SessionBasedScript):
    """
    Simulate asset pricing via Monte-Carlo methods.
    """

    def __init__(self):
      super(PricePlotScript, self).__init__(version='1.0')

    def setup_args(self):
        self.add_param('S0',    type=float, help="stock price today (e.g., 50)")
        self.add_param('mu',    type=float, help="expected return (e.g., 0.04)")
        self.add_param('sigma', type=float, help="volatility (e.g., 0.1)")
        self.add_param('dt',    type=float, help="size of time steps (e.g., 0.273)")
        self.add_param('etime', type=int,   help="days to expiry (e.g., 1000)")
        self.add_param('nsims', type=int,   help="number of simulation paths")

    def new_tasks(self, extra):
        # *Note:* We must wire together the two apps by having `app2` reference
        # *as input file a file that is in the output files of `app1`
        app1 = SimAssetApp(self.params.S0, self.params.mu, self.params.sigma,
                           self.params.dt, self.params.etime, self.params.nsims)
        app2 = SAPlotApp(join(app1.output_dir, 'results.csv'))
        apps_to_run = [ SequentialTaskCollection([app1, app2], output_dir='/tmp') ]
        return apps_to_run


class SimAssetApp(Application):
    def __init__(self, S0, mu, sigma, delta, etime, nsims):
        Application.__init__(
            self,
            ['Rscript', 'simAsset.R', S0, mu, sigma, delta, etime, nsims],
            inputs=['downloads/simAsset.R'],
            outputs=['results.csv'],
            output_dir=('simAsset.d'),
            stdout="simAsset.log",
            stderr="simAsset.log"
        )


class SAPlotApp(Application):
    def __init__(self, result_csv_path):
        Application.__init__(
            self,
            ['python', 'saplot.py'],
            inputs=['downloads/saplot.py', result_csv_path],
            outputs=['saplot.pdf'],
            output_dir=('saplot.d'),
            stdout="saplot.log",
            stderr="saplot.log"
        )
