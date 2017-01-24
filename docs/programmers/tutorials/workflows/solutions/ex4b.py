#! /usr/bin/env python

"""
Exercise 4.B: Write a topblast.py script to perform 1-1
BLAST comparisons.

The topblast.py script shall be invoked like this::

    $ python topblast.py [-e T ] [-m F ] \
        new.faa k1.faa [k2.faa ...]

where:

* Option -e (alias: --e-value) takes a floating point threshold
  argument T ;

* Option -m (alias: -output-format) takes a single-digit integer
  argument F ;

* Arguments new.faa, k1.faa, etc. are files.

The script should generate and run comparisons between
new.faa and each of the kN .faa. Each 1-1 comparison
should run as a separate task. All of them share the same
settings for the -e and -m options for blastpgp.
"""

import os
from os.path import abspath, basename
import sys

from gc3libs import Application
from gc3libs.cmdline import SessionBasedScript


if __name__ == '__main__':
    from ex4b import TopBlastScript
    TopBlastScript().run()


class TopBlastScript(SessionBasedScript):
    """
    Run BLAST on pairs of FAA files.
    """
    def __init__(self):
        super(TopBlastScript, self).__init__(version='1.0')
    def setup_args(self):
        self.add_param('new_faa', help="Query FAA")
        self.add_param('known_faa', nargs='+',
                       help="Samples to compare to the query")
    def setup_options(self):
        self.add_param('--e-value', '-e',
                       type=float, help="Expectation value")
        self.add_param('--output-format', '-o', dest='output_fmt',
                       type=int, help="Output format, int from 0 to 9")
    def new_tasks(self, extra):
        apps_to_run = []
        for in_file2 in self.params.known_faa:
            in_file2 = abspath(in_file2)
            apps_to_run.append(BlastApp(self.params.new_faa, in_file2,
                                        self.params.e_value, self.params.output_fmt))
        return apps_to_run


from gc3libs.quantity import GB, minutes

class BlastApp(Application):
    """Run BLAST on two files."""
    def __init__(self, input1, input2, e_value, output_fmt):
        inp1 = basename(input1)
        inp2 = basename(input2)
        Application.__init__(
            self,
            arguments=["blastp", "-query", inp1, "-subject", inp2,
                       "-evalue", e_value, "-outfmt", output_fmt, "-out", "output.txt"],
            inputs=[input1, input2],
            outputs=["output.txt"],
            output_dir=("blast-" + inp1 + "-" + inp2 + ".d"),
            stdout=None,  # BLAST's option `-o` already does this
            stderr="errors.txt",
            requested_memory=1*GB)
