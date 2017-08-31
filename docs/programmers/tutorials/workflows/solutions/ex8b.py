#! /usr/bin/env python

"""
Write a `blastdb.py` script to build a BLAST DB and query it.

The `blastdb.py` script shall be invoked like this:

  $ python blastdb.py query.faa p1.faa [p2.faa ...]

where arguments `new.faa`, `p1.faa`, etc. are FASTA-format files.

The script should build a BLAST DB out of the files `pN.faa`.
Then, it should query this database for occurrences of the
proteins in `query.faa` using `blastpgp`.
"""

import os
from os.path import abspath, basename
import sys

from gc3libs import Application
from gc3libs.cmdline import SessionBasedScript
from gc3libs.quantity import GB, minutes


if __name__ == '__main__':
    from ex8b import BlastDbScript
    BlastDbScript().run()


class BlastDbScript(SessionBasedScript):
    """
    Run BLAST on pairs of FAA files.
    """
    def __init__(self):
        super(BlastDbScript, self).__init__(version='1.0')
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
        return [ BlastDbTasks(self.params.new_faa, self.params.known_faa,
                              self.params.e_value, self.params.output_fmt) ]


class BlastDbTasks(StagedTaskCollection):

    def __init__(self, query, subjects, e_value, output_fmt):
        self.query = query
        self.subjects = subjects
        self.e_value = e_value
        self.output_fmt = output_fmt
        StagedTaskCollection.__init__(self)

    def stage0(self):
        return ConcatFastaApp('db.faa', self.subjects)

    def stage1(self):
        return FormatDbApp(join(self.tasks[0].output_dir, 'db.faa'))

    def stage2(self):
        return BlastpgpApp(self.query, join(self.tasks[1].output_dir, 'db.faa'), self.e_value, self.output_fmt)


class ConcatFastaApp(Application):
    """Merge FASTA files."""
    def __init__(self, output_name, input_files):
        input_names = [basename(infile) for infile in input_files]
        Application.__init__(
            self,
            arguments=(["cat"] + input_names + [">", output_name])
            inputs=input_files,
            outputs=[output_name],
            output_dir=("cat-" + output_name + ".d"),
            stdout=None,  # redirection operator `>` already does it
            stderr="errors.txt",
            requested_memory=1*GB)


class FormatDbApp(Application):
    """Index a (large) FASTA file."""
    def __init__(self, input_file_path):
        input_name = basename(input_file_path)
        output_names = [input_name+suffix for suffix in ('', '.phr', '.pin', '.psq')]
        Application.__init__(
            self,
            arguments=["formatdb", "-i", input_name],
            inputs=[input_file_path],
            outputs=output_names,
            output_dir=("formatdb-" + input_name + ".d"),
            stdout="formatdb.log",
            stderr="formatdb.log",
            requested_memory=1*GB)


class BlastpgpApp(Application):
    """Run BLAST on two files."""
    def __init__(self, query_file_path, db_files_path, e_value, output_fmt):
        q = basename(query_file_path)
        db = basename(db_files_path)
        db_files = [db_files_path+suffix for suffix in ('', '.phr', '.pin', '.psq')]
        Application.__init__(
            self,
            arguments=["blastpgp", "-i", q, "-d", db,
                       "-e", e_value, "-m", output_fmt, "-o", "output.txt"],
            inputs=([q] + db_files),
            outputs=["output.txt"],
            output_dir=("blast-" + inp1 + "-" + inp2 + ".d"),
            stdout=None,  # BLAST's option `-o` already does this
            stderr="errors.txt",
            requested_memory=1*GB)
