#! /usr/bin/env python

"""
Write a ``sim_asset.py`` program that:

* takes the same command-line positional arguments as ``simAsset.R``, plus an
  additional integer trailing parameter P;

* runs ``simAsset.R`` (in parallel) P times with the given arguments (so,
  effectively simulates N x P price paths);

* reads all the generated ``results.csv`` files, and I computes and prints the
  average value of the option at the end of the simulated time, across all N x
  P price paths.
"""

import csv
import os
import sys

from gc3libs import Application
from gc3libs.cmdline import SessionBasedScript


if __name__ == '__main__':
    from ex6d import SimAssetScript
    SimAssetScript().run()


class SimAssetScript(SessionBasedScript):
    """
    Simulate asset pricing via Monte-Carlo methods.
    """

    def __init__(self):
      super(SimAssetScript, self).__init__(version='1.0')

    def setup_args(self):
        self.add_param('S0',    type=float, help="stock price today (e.g., 50)")
        self.add_param('mu',    type=float, help="expected return (e.g., 0.04)")
        self.add_param('sigma', type=float, help="volatility (e.g., 0.1)")
        self.add_param('dt',    type=float, help="size of time steps (e.g., 0.273)")
        self.add_param('etime', type=int,   help="days to expiry (e.g., 1000)")
        self.add_param('nsims', type=int,   help="number of simulation paths per task")
        self.add_param('P',     type=int,   help="number of task to run")

    def new_tasks(self, extra):
        apps_to_run = []
        for seqnr in range(self.params.P):
            app = SimAssetApp(self.params.S0, self.params.mu, self.params.sigma,
                              self.params.dt, self.params.etime, self.params.nsims, seqnr)
            apps_to_run.append(app)
        return apps_to_run

    def after_main_loop(self):
        # check that all tasks are terminated
        can_postprocess = True
        for task in self.session.tasks.values():
            if task.execution.state != 'TERMINATED':
                can_postprocess = False
                break
        if can_postprocess:
            final_prices = []
            for task in self.session.tasks.values():
                result_path = os.path.join(task.output_dir, 'results.csv')
                with open(result_path, 'r') as result_file:
                    result_csv = csv.reader(result_file)
                    for row in result_csv:
                        final_prices.append(float(row[-1]))
            # now compute average
            if final_prices:
                average = sum(final_prices) / len(final_prices)
                print("==> Average final price is: {average}".format(average=average))
            else:
                print("==> No data to compute average!")


class SimAssetApp(Application):
    def __init__(self, S0, mu, sigma, delta, etime, nsims, seqnr):
        Application.__init__(
            self,
            ['/usr/bin/Rscript', 'simAsset.R', S0, mu, sigma, delta, etime, nsims],
            inputs=['downloads/simAsset.R'],
            outputs=['results.csv'],
            output_dir=('simAsset-%d.d' % seqnr),
            stdout="simAsset.log",
            stderr="simAsset.log"
        )
