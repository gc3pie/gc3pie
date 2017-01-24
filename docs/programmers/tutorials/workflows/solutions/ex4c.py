#! /usr/bin/env python

"""
Exercise 4.C: Modify the topblast.py script that you've written in
Exercise 4.B to be invoked like this::

    $ python topblast.py [-e T ] [-m F ] new.faa dir

Input files describing the "known" subjects should be
found by recursively scanning the given directory
path.

Bonus points if the modified script exists with a
correct error message in case new.faa is not an
existing file, or dir is not a valid directory path.
"""

import os
from os.path import abspath, basename, join
import sys

from gc3libs import Application
from gc3libs.cmdline import SessionBasedScript


if __name__ == '__main__':
    from ex4c import TopBlastScript
    TopBlastScript().run()


# to ensure that path-name arguments given on the command-line exist
from gc3libs.cmdline import existing_file, existing_directory

class TopBlastScript(SessionBasedScript):
    """
    Run BLAST on pairs of FAA files.
    """
    def __init__(self):
        super(TopBlastScript, self).__init__(version='1.0')
    def setup_args(self):
        self.add_param('query', help="Query file (FASTA format)",
                       type=existing_file)
        self.add_param('dir', nargs='+', type=existing_directory,
                       help=("Directories to scan for `.faa` files"
                             " to compare to the query."))
    def setup_options(self):
        self.add_param('--e-value', '-e',
                       type=float, help="Expectation value")
        self.add_param('--output-format', '-o', dest='output_fmt',
                       type=int, help="Output format, int from 0 to 9")
    def new_tasks(self, extra):
        apps_to_run = []
        # recursively scan the directories given on the command line,
        # and collect `.faa` files; create one task to run for each of them
        for dir in self.params.dir:
            for rootpath, dirs, files in os.walk(dir):
                for filename in files:
                    if filename.endswith('.faa'):
                        vs_file = abspath(join(rootdir, file))
                        apps_to_run.append(BlastApp(
                            self.params.query, vs_file,
                            self.params.e_value, self.params.output_fmt))
        return apps_to_run


from gc3libs.quantity import GB

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
